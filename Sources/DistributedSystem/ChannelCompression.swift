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

final class ChannelCompressionHandshakeServer: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer

    private let loggerBox: Box<Logger>
    private let streamHandler: NIOCore.ChannelHandler
    private var timer: Scheduled<Void>?

    private var logger: Logger { loggerBox.value }

    init(_ loggerBox: Box<Logger>, _ streamHandler: NIOCore.ChannelHandler) {
        self.loggerBox = loggerBox
        self.streamHandler = streamHandler
    }

    func channelActive(context: ChannelHandlerContext) {
        timer = context.eventLoop.scheduleTask(in: DistributedSystem.pingInterval*2) {
            let channel = context.channel
            self.logger.info("Session timeout for client @ \(channel.remoteAddressDescription), close connection.")
            context.close(promise: nil)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        if let timer {
            timer.cancel()
            self.timer = nil
        }
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        if let timer {
            timer.cancel()
            self.timer = nil
        }
        var buffer = unwrapInboundIn(data)
        if let compression = buffer.readInteger(as: UInt8.self) {
            if compression != 0 {
                _ = context.pipeline.addHandler(ChannelCompressionInboundHandler(loggerBox), position: .after(streamHandler))
                _ = context.pipeline.addHandler(ChannelCompressionOutboundHandler(), position: .last)
            }
            context.fireChannelActive()
            if buffer.readableBytes > 0 {
                context.fireChannelRead(wrapInboundOut(buffer))
            }
            _ = context.pipeline.removeHandler(self)
        } else {
            let channel = context.channel
            logger.info("Invalid compression request received from client @ \(channel.remoteAddressDescription), close connection.")
            channel.close(promise: nil)
        }
    }
}

final class ChannelCompressionHandshakeClient: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer

    private let loggerBox: Box<Logger>
    private let compression: UInt8
    private let streamHandler: NIOCore.ChannelHandler

    init(_ loggerBox: Box<Logger>, _ compression: UInt8, _ streamHandler: NIOCore.ChannelHandler) {
        self.loggerBox = loggerBox
        self.compression = compression
        self.streamHandler = streamHandler
    }

    func channelActive(context: ChannelHandlerContext) {
        var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt8>.size)
        buffer.writeInteger(compression, as: UInt8.self)
        context.writeAndFlush(NIOAny(buffer), promise: nil)
        if compression != 0 {
            _ = context.pipeline.addHandler(ChannelCompressionInboundHandler(loggerBox), position: .after(streamHandler))
            _ = context.pipeline.addHandler(ChannelCompressionOutboundHandler(), position: .last)
        }
        context.fireChannelActive()
        _ = context.pipeline.removeHandler(self)
    }
}

fileprivate struct BufferManager {
    var buffers = Array(repeating: UnsafeMutableRawBufferPointer(start: nil, count: 0), count: 2)
    var nextIdx = 0

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

    mutating func reset() {
        for idx in 0..<buffers.count {
            buffers[idx].deallocate()
        }
    }
}

final class ChannelCompressionInboundHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer

    private let loggerBox: Box<Logger>
    private var bufferManager = BufferManager()
    private var lz4Stream = LZ4_streamDecode_t()

    private var logger: Logger { loggerBox.value }

    init(_ loggerBox: Box<Logger>) {
        self.loggerBox = loggerBox
        LZ4_setStreamDecode(&lz4Stream, nil, 0);
    }

    deinit {
        bufferManager.reset()
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
                //print("received \(unwrapInboundIn(data).readableBytes) bytes, messageSize=\(messageSize)\n\(unwrapInboundIn(data).hexDump(format: .detailed))\n\(bufferOut.hexDump(format: .detailed))")
                context.fireChannelRead(wrapInboundOut(bufferOut))
                return
            }
        }
        logger.info("invalid compressed message received from client @ \(context.channel.remoteAddressDescription), close connection.")
        context.close(promise: nil)
    }
}

final class ChannelCompressionOutboundHandler: ChannelOutboundHandler {
    typealias OutboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private var bufferManager = BufferManager()
    private var lz4Stream = LZ4_stream_t()

    init() {
        LZ4_initStream(&lz4Stream, MemoryLayout<LZ4_stream_t>.size);
    }

    deinit {
        bufferManager.reset()
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
        //print("send \(bufferOut.readableBytes) bytes, messageSize=\(messageSize)\n\(bufferOut.hexDump(format: .detailed))\n\(unwrapOutboundIn(data).hexDump(format: .detailed))")
        context.write(wrapOutboundOut(bufferOut), promise: promise)
    }
}
