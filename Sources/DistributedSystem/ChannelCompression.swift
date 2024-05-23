// Copyright 2024 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import class Helpers.Box
import Logging
import lz4
internal import NIOCore

private enum RequestedCompressionMode: UInt8 {
    case disabled = 0
    case streaming = 1
    case dictionary = 2
}

final class ChannelCompressionHandshakeServer: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer

    private let distributedSystem: DistributedSystem
    private let channelHandler: ChannelHandler
    private var timer: Scheduled<Void>?

    private var logger: Logger { distributedSystem.loggerBox.value }

    init(_ distributedSystem: DistributedSystem, _ channelHandler: ChannelHandler) {
        self.distributedSystem = distributedSystem
        self.channelHandler = channelHandler
    }

    func channelActive(context: ChannelHandlerContext) {
        timer = context.eventLoop.scheduleTask(in: DistributedSystem.pingInterval*2) {
            self.logger.info("\(context.channel.addressDescription): session timeout for compression client handshake, close connection")
            context.close(promise: nil)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        if let timer {
            timer.cancel()
            self.timer = nil
        }
        logger.info("\(context.channel.addressDescription): connection closed")
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        if let timer {
            timer.cancel()
            self.timer = nil
        }
        var buffer = unwrapInboundIn(data)
        if let compressionMode = buffer.readInteger(as: RequestedCompressionMode.RawValue.self) {
            if compressionMode == RequestedCompressionMode.disabled.rawValue {
                // not required
            } else if compressionMode == RequestedCompressionMode.streaming.rawValue {
                // streaming
                let emptyDictionary = UnsafeRawBufferPointer(start: nil, count: 0)
                var name = ChannelCompressionInboundHandler.name
                _ = context.pipeline.addHandler(ChannelCompressionInboundHandler(distributedSystem, emptyDictionary), name: name, position: .before(channelHandler))
                name = ChannelCompressionOutboundHandler.name
                _ = context.pipeline.addHandler(ChannelCompressionOutboundHandler(distributedSystem, emptyDictionary), name: name, position: .after(self))
            } else if compressionMode == RequestedCompressionMode.dictionary.rawValue {
                if case let .dictionary(dictionary) = distributedSystem.compressionMode {
                    if let checksum = buffer.readInteger(as: UInt32.self) {
                        if checksum != dictionary.checksum {
                            logger.info("\(context.channel.addressDescription): client/server compression dictionary mismatch, close connection")
                            context.close(promise: nil)
                            return
                        }
                        var name = ChannelCompressionInboundHandler.name
                        _ = context.pipeline.addHandler(ChannelCompressionInboundHandler(distributedSystem, dictionary.data), name: name, position: .before(channelHandler))
                        name = ChannelCompressionOutboundHandler.name
                        _ = context.pipeline.addHandler(ChannelCompressionOutboundHandler(distributedSystem, dictionary.data), name: name, position: .after(self))
                    } else {
                        logger.info("\(context.channel.addressDescription): invalid compression request received, close connection")
                        context.close(promise: nil)
                        return
                    }
                } else {
                    logger.info("\(context.channel.addressDescription): no compression dictionary configured, close connection")
                    context.close(promise: nil)
                    return
                }
            } else {
                logger.info("\(context.channel.addressDescription): unsupported compression mode '\(compressionMode)' received, close connection")
                context.close(promise: nil)
                return
            }
            context.fireChannelActive()
            if buffer.readableBytes > 0 {
                context.fireChannelRead(wrapInboundOut(buffer))
            }
            _ = context.pipeline.removeHandler(self)
        } else {
            logger.info("\(context.channel.addressDescription): invalid compression request received, close connection")
            context.close(promise: nil)
        }
    }
}

final class ChannelCompressionHandshakeClient: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer

    private let distributedSystem: DistributedSystem
    private let channelHandler: ChannelHandler

    init(_ distributedSystem: DistributedSystem, _ channelHandler: ChannelHandler) {
        self.distributedSystem = distributedSystem
        self.channelHandler = channelHandler
    }

    func channelActive(context: ChannelHandlerContext) {
        let compressionMode = distributedSystem.compressionMode
        switch compressionMode {
        case .disabled:
            var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt8>.size)
            buffer.writeInteger(RequestedCompressionMode.disabled.rawValue)
            context.writeAndFlush(NIOAny(buffer), promise: nil)
        case .streaming:
            var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt8>.size)
            buffer.writeInteger(RequestedCompressionMode.streaming.rawValue)
            context.writeAndFlush(NIOAny(buffer), promise: nil)
            let emptyDictionary = UnsafeRawBufferPointer(start: nil, count: 0)
            var name = ChannelCompressionInboundHandler.name
            _ = context.pipeline.addHandler(ChannelCompressionInboundHandler(distributedSystem, emptyDictionary), name: name, position: .before(channelHandler))
            name = ChannelCompressionOutboundHandler.name
            _ = context.pipeline.addHandler(ChannelCompressionOutboundHandler(distributedSystem, emptyDictionary), name: name, position: .after(self))
        case let .dictionary(dictionary):
            // send dictionary checksum to server for sanity check to be sure both client and server use same dictionary
            var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt8>.size + MemoryLayout<UInt64>.size)
            buffer.writeInteger(RequestedCompressionMode.dictionary.rawValue)
            buffer.writeInteger(dictionary.checksum)
            context.writeAndFlush(NIOAny(buffer), promise: nil)
            var name = ChannelCompressionInboundHandler.name
            _ = context.pipeline.addHandler(ChannelCompressionInboundHandler(distributedSystem, dictionary.data), name: name, position: .before(channelHandler))
            name = ChannelCompressionOutboundHandler.name
            _ = context.pipeline.addHandler(ChannelCompressionOutboundHandler(distributedSystem, dictionary.data), name: name, position: .after(self))
        }
        context.fireChannelActive()
        _ = context.pipeline.removeHandler(self)
    }
}

fileprivate struct BufferManager: ~Copyable {
    var buffers = Array(repeating: UnsafeMutableRawBufferPointer(start: nil, count: 0), count: 2)
    var nextIdx = 0

    deinit {
        for idx in 0..<buffers.count {
            buffers[idx].deallocate()
        }
    }

    mutating func getNextBuffer(capacity: Int) -> UnsafeMutableRawBufferPointer {
        let idx = nextIdx
        nextIdx = (1 - nextIdx)
        var buffer = buffers[idx]
        if buffer.count < capacity {
            buffer.deallocate()
            buffer = UnsafeMutableRawBufferPointer.allocate(byteCount: capacity, alignment: 0)
            buffers[idx] = buffer
        }
        return buffer
    }
}

final class ChannelCompressionInboundHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer

    private let distributedSystem: DistributedSystem
    private var bufferManager = BufferManager()
    private var lz4Stream = LZ4_streamDecode_t()
    private var bytesDecompressed = UInt64(0)

    private var logger: Logger { distributedSystem.loggerBox.value }

    static let statsKey = "bytes_decompressed"
    static let name = "inboundDecompression"

    init(_ distributedSystem: DistributedSystem, _ dictionary: UnsafeRawBufferPointer) {
        self.distributedSystem = distributedSystem
        LZ4_setStreamDecode(&lz4Stream, dictionary.baseAddress, Int32(dictionary.count))
    }

    deinit {
        distributedSystem.incrementStats([Self.statsKey: bytesDecompressed])
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var bufferIn = unwrapInboundIn(data)
        _ = bufferIn.readInteger(as: UInt32.self) // skip message size
        if let messageSize = bufferIn.readInteger(as: UInt32.self) {
            let dstBuffer = bufferManager.getNextBuffer(capacity: Int(messageSize))
            let decompressedBytes = bufferIn.withUnsafeReadableBytes { ptr in
                LZ4_decompress_safe_continue(&lz4Stream,
                                             ptr.baseAddress!.assumingMemoryBound(to: Int8.self),
                                             dstBuffer.baseAddress!.assumingMemoryBound(to: Int8.self),
                                             Int32(ptr.count),
                                             Int32(dstBuffer.count))
            }
            if decompressedBytes == messageSize {
                var bufferOut = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt32>.size + Int(messageSize))
                bufferOut.writeInteger(messageSize)
                _ = bufferOut.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in
                    ptr.copyMemory(from: UnsafeRawBufferPointer(start: dstBuffer.baseAddress, count: Int(messageSize)))
                    return Int(messageSize)
                }
                context.fireChannelRead(wrapInboundOut(bufferOut))
                bytesDecompressed += UInt64(messageSize)
                return
            }
        }
        logger.info("\(context.channel.addressDescription): invalid compressed message received, close connection")
        context.close(promise: nil)
    }
}

final class ChannelCompressionOutboundHandler: ChannelOutboundHandler {
    typealias OutboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private let distributedSystem: DistributedSystem
    private var bufferManager = BufferManager()
    private var lz4Stream = LZ4_stream_t()
    private var bytesCompressed = UInt64(0)

    static let statsKey = "bytes_compressed"
    static let name = "outboundCompression"

    init(_ distributedSystem: DistributedSystem, _ dictionary: UnsafeRawBufferPointer) {
        self.distributedSystem = distributedSystem
        LZ4_initStream(&lz4Stream, MemoryLayout<LZ4_stream_t>.size)
        if dictionary.count > 0 {
            LZ4_loadDict(&lz4Stream, dictionary.baseAddress!, Int32(dictionary.count))
        }
    }

    deinit {
        distributedSystem.incrementStats([Self.statsKey: bytesCompressed])
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        var bufferIn = unwrapOutboundIn(data)
        guard let messageSize = bufferIn.readInteger(as: UInt32.self), bufferIn.readableBytes == messageSize else { fatalError("invalid message") }
        let compressBound = LZ4_compressBound(Int32(bufferIn.readableBytes))
        let srcBuffer = bufferManager.getNextBuffer(capacity: bufferIn.readableBytes)
        bufferIn.withUnsafeReadableBytes { ptr in srcBuffer.copyMemory(from: ptr) }
        // LZ4_compressBound() returns value which even larger than size of the source data.
        // The compression ratio often is better than expected by LZ4_compressBound(),
        // so one possible further memory optimization is to allocate one big outbound buffer
        // and slice it with compressed data per each write operation.
        var bufferOut = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt32>.size * 2 + Int(compressBound))
        _ = bufferOut.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in
            let compressedBytes = LZ4_compress_fast_continue(&lz4Stream,
                                                             srcBuffer.baseAddress!.assumingMemoryBound(to: Int8.self),
                                                             (ptr.baseAddress! + MemoryLayout<UInt32>.size * 2).assumingMemoryBound(to: Int8.self),
                                                             Int32(messageSize),
                                                             compressBound, 1)
            ptr.storeBytes(of: (UInt32(MemoryLayout<UInt32>.size) + UInt32(compressedBytes)).bigEndian, as: UInt32.self)
            ptr.storeBytes(of: messageSize.bigEndian, toByteOffset: MemoryLayout<UInt32>.size, as: UInt32.self)
            return MemoryLayout<UInt32>.size * 2 + Int(compressedBytes)
        }
        context.write(wrapOutboundOut(bufferOut), promise: promise)
        bytesCompressed += UInt64(messageSize)
    }
}
