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

private enum HandshakeRequest: UInt8 {
    case noCompression = 0
    case streamingCompression = 1
    case dictionaryCompression = 2 // + UInt32 dictionary checksum
}

private enum HandshakeResponse: UInt8 {
    case noCompression = 0
    case streamingCompression = 1
    case sameDictionary = 2
    case dictionary = 3 // + dictionary
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
        logger.debug("\(context.channel.addressDescription): channel active (server compression handshake)")
        if DistributedSystem.pingInterval.nanoseconds > 0 {
            timer = context.eventLoop.scheduleTask(in: DistributedSystem.pingInterval*2) {
                self.logger.info("\(context.channel.addressDescription): session timeout for compression client handshake, close connection")
                context.close(promise: nil)
            }
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        if let timer {
            timer.cancel()
            self.timer = nil
        }
        logger.info("\(context.channel.addressDescription): connection closed")
    }

    private func sendResponse(_ response: HandshakeResponse, to context: ChannelHandlerContext) {
        assert(response != .dictionary)
        let size = MemoryLayout<HandshakeResponse.RawValue>.size
        var buffer = ByteBufferAllocator().buffer(capacity: size)
        buffer.writeInteger(response.rawValue)
        context.write(NIOAny(buffer), promise: nil)
    }

    private func sendDictionaryResponse(_ dictionary: UnsafeRawBufferPointer, to context: ChannelHandlerContext) {
        let size = MemoryLayout<HandshakeResponse.RawValue>.size + ULEB128.size(UInt32(dictionary.count)) + dictionary.count
        var buffer = ByteBufferAllocator().buffer(capacity: size)
        buffer.writeInteger(HandshakeResponse.dictionary.rawValue)
        buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ULEB128.encode(UInt32(dictionary.count), to: $0.baseAddress!) }
        buffer.writeBytes(dictionary)
        context.write(NIOAny(buffer), promise: nil)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        if let timer {
            timer.cancel()
            self.timer = nil
        }

        var buffer = unwrapInboundIn(data)
        guard let handshakeRequestRaw = buffer.readInteger(as: HandshakeRequest.RawValue.self) else {
            logger.info("\(context.channel.addressDescription): invalid compression request received, close connection")
            context.close(promise: nil)
            return
        }

        guard let handshakeRequest = HandshakeRequest(rawValue: handshakeRequestRaw) else {
            logger.info("\(context.channel.addressDescription): unsupported compression request '\(handshakeRequestRaw)' received, close connection")
            context.close(promise: nil)
            return
        }

        logger.debug("\(context.channel.addressDescription): client requested \(handshakeRequest) compression mode")

        let compressionMode = distributedSystem.compressionMode
        switch handshakeRequest {
        case .noCompression:
            switch compressionMode {
            case .disabled:
                sendResponse(.noCompression, to: context)
            case .streaming:
                sendResponse(.streamingCompression, to: context)
                _ = context.pipeline.addHandler(
                    ChannelStreamCompressionOutboundHandler(distributedSystem),
                    name: ChannelCompressionOutboundHandler.name,
                    position: .after(self))
            case let .dictionary(dictionaryData):
                sendDictionaryResponse(dictionaryData.data.value, to: context)
                _ = context.pipeline.addHandler(
                    ChannelDictCompressionOutboundHandler(distributedSystem, dictionaryData.data),
                    name: ChannelCompressionOutboundHandler.name,
                    position: .after(self))
            }
            context.fireChannelActive()
            _ = context.pipeline.removeHandler(self)
        case .streamingCompression:
            _ = context.pipeline.addHandler(ChannelStreamCompressionInboundHandler(distributedSystem),
                                            name: ChannelCompressionInboundHandler.name,
                                            position: .before(channelHandler))
            switch compressionMode {
            case .disabled:
                sendResponse(.noCompression, to: context)
            case .streaming:
                sendResponse(.streamingCompression, to: context)
                _ = context.pipeline.addHandler(
                    ChannelStreamCompressionOutboundHandler(distributedSystem),
                    name: ChannelCompressionOutboundHandler.name,
                    position: .after(self))
            case let .dictionary(dictionaryData):
                sendDictionaryResponse(dictionaryData.data.value, to: context)
                _ = context.pipeline.addHandler(
                    ChannelDictCompressionOutboundHandler(distributedSystem, dictionaryData.data),
                    name: ChannelCompressionOutboundHandler.name,
                    position: .after(self))
            }
            context.fireChannelActive()
            _ = context.pipeline.removeHandler(self)
        case .dictionaryCompression:
            guard let checksum = buffer.readInteger(as: UInt32.self) else {
                logger.info("\(context.channel.addressDescription): invalid compression request received, close connection")
                context.close(promise: nil)
                return
            }
            switch compressionMode {
            case .disabled:
                sendResponse(.noCompression, to: context)
                _ = context.pipeline.addHandler(
                    DictionaryReceiver(distributedSystem, channelHandler),
                    name: DictionaryReceiver.name,
                    position: .after(self))
                context.fireChannelActive()
                _ = context.pipeline.removeHandler(self)
            case .streaming:
                sendResponse(.streamingCompression, to: context)
                _ = context.pipeline.addHandler(
                    DictionaryReceiver(distributedSystem, channelHandler),
                    name: DictionaryReceiver.name,
                    position: .after(self))
                _ = context.pipeline.addHandler(
                    ChannelStreamCompressionOutboundHandler(distributedSystem),
                    name: ChannelCompressionOutboundHandler.name,
                    position: .after(self))
                context.fireChannelActive()
                _ = context.pipeline.removeHandler(self)
            case let .dictionary(dictionaryData):
                if checksum == dictionaryData.checksum {
                    // same dictionary on both sides
                    sendResponse(.sameDictionary, to: context)
                    _ = context.pipeline.addHandler(
                        ChannelDictCompressionInboundHandler(distributedSystem, dictionaryData.data),
                        name: ChannelDictCompressionInboundHandler.name,
                        position: .before(channelHandler))
                    _ = context.pipeline.addHandler(
                        ChannelDictCompressionOutboundHandler(distributedSystem, dictionaryData.data),
                        name: ChannelDictCompressionOutboundHandler.name,
                        position: .after(self))
                    context.fireChannelActive()
                    _ = context.pipeline.removeHandler(self)
                } else {
                    sendDictionaryResponse(dictionaryData.data.value, to: context)
                    _ = context.pipeline.addHandler(
                        DictionaryReceiver(distributedSystem, channelHandler),
                        name: DictionaryReceiver.name,
                        position: .after(self))
                    _ = context.pipeline.addHandler(
                        ChannelDictCompressionOutboundHandler(distributedSystem, dictionaryData.data),
                        name: ChannelDictCompressionOutboundHandler.name,
                        position: .after(self))
                    context.fireChannelActive()
                    _ = context.pipeline.removeHandler(self)
                }
            }
        }
    }
}

final class DictionaryReceiver: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer

    static let name = "dictionaryReceiver"

    private let distributedSystem: DistributedSystem
    private let channelHandler: ChannelHandler
    private var timer: Scheduled<Void>?
    private var dictionary = UnsafeMutableRawBufferPointer(start: nil, count: 0)
    private var dictionaryBytesReceived = 0

    private var logger: Logger { distributedSystem.loggerBox.value }

    init(_ distributedSystem: DistributedSystem, _ channelHandler: ChannelHandler) {
        self.distributedSystem = distributedSystem
        self.channelHandler = channelHandler
    }

    deinit {
        dictionary.deallocate()
    }

    private func startTimer(_ context: ChannelHandlerContext, _ timeout: TimeAmount, _ lastDictionaryBytesReceived: Int) {
        let eventLoop = context.eventLoop
        timer = eventLoop.scheduleTask(in: timeout) {
            let dictionaryBytesReceived = self.dictionaryBytesReceived
            if dictionaryBytesReceived > lastDictionaryBytesReceived {
                self.startTimer(context, timeout, dictionaryBytesReceived)
            } else {
                self.logger.info("\(context.channel.addressDescription): session timeout for dictionary receiver, close connection")
                context.close(promise: nil)
            }
        }
    }

    func channelActive(context: ChannelHandlerContext) {
        logger.debug("\(context.channel.addressDescription): channel active (dictionary receiver)")
        if DistributedSystem.pingInterval.nanoseconds > 0 {
            startTimer(context, DistributedSystem.pingInterval*2, 0)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        logger.info("\(context.channel.addressDescription): connection closed")
        if let timer {
            timer.cancel()
            self.timer = nil
        }
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var buffer = unwrapInboundIn(data)
        if dictionary.count == 0 {
            do {
                let (sizeSize, dictionarySize) = try buffer.withUnsafeReadableBytes { try ULEB128.decode($0, as: UInt32.self) }
                buffer.moveReaderIndex(forwardBy: sizeSize)
                dictionary = UnsafeMutableRawBufferPointer.allocate(byteCount: Int(dictionarySize), alignment: 0)
            } catch {
                // Is there any probability to receive less than 5 bytes?
                // Let's just close connection for now in such case.
                self.logger.info("\(context.channel.addressDescription): invalid dictionary block received, close connection")
                context.close(promise: nil)
                return
            }
        }

        let bytesRemaining = (dictionary.count - dictionaryBytesReceived)
        let copyBytes = min(bytesRemaining, buffer.readableBytes)
        buffer.withUnsafeReadableBytes {
            let ptr = self.dictionary.baseAddress! + dictionaryBytesReceived
            ptr.copyMemory(from: $0.baseAddress!, byteCount: copyBytes)
        }
        buffer.moveReaderIndex(forwardBy: copyBytes)
        dictionaryBytesReceived += copyBytes
        if dictionaryBytesReceived < dictionary.count {
            return
        }

        if let timer {
            timer.cancel()
            self.timer = nil
        }

        let ptr = UnsafeRawBufferPointer(self.dictionary)
        self.dictionary = UnsafeMutableRawBufferPointer(start: nil, count: 0)

        let dictionary = BoxEx(ptr) { ptr.deallocate() }

        _ = context.pipeline.addHandler(
            ChannelDictCompressionInboundHandler(distributedSystem, dictionary),
            name: ChannelDictCompressionInboundHandler.name,
            position: .before(channelHandler))

        context.fireChannelActive()
        if buffer.readableBytes > 0 {
            context.fireChannelRead(wrapInboundOut(buffer))
        }

        _ = context.pipeline.removeHandler(self)
    }
}

final class ChannelCompressionHandshakeClient: ChannelInboundHandler, RemovableChannelHandler {
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
        self.logger.debug("\(context.channel.addressDescription): channel active (client compression handshake)")
        let compressionMode = distributedSystem.compressionMode
        switch compressionMode {
        case .disabled:
            var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<HandshakeRequest.RawValue>.size)
            buffer.writeInteger(HandshakeRequest.noCompression.rawValue)
            context.writeAndFlush(NIOAny(buffer), promise: nil)
        case .streaming:
            var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<HandshakeRequest.RawValue>.size)
            buffer.writeInteger(HandshakeRequest.streamingCompression.rawValue)
            context.writeAndFlush(NIOAny(buffer), promise: nil)
        case let .dictionary(dictionary):
            var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<HandshakeRequest.RawValue>.size + MemoryLayout<UInt32>.size)
            buffer.writeInteger(HandshakeRequest.dictionaryCompression.rawValue)
            buffer.writeInteger(dictionary.checksum)
            context.writeAndFlush(NIOAny(buffer), promise: nil)
        }

        if DistributedSystem.pingInterval.nanoseconds > 0 {
            timer = context.eventLoop.scheduleTask(in: DistributedSystem.pingInterval*2) {
                self.logger.info("\(context.channel.addressDescription): session timeout for compression client handshake, close connection")
                context.close(promise: nil)
            }
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        logger.info("\(context.channel.addressDescription): connection closed")
        if let timer {
            timer.cancel()
            self.timer = nil
        }
    }

    private func sendDictionary(_ dictionary: UnsafeRawBufferPointer, to context: ChannelHandlerContext) {
        let size = ULEB128.size(UInt32(dictionary.count)) + dictionary.count
        var buffer = ByteBufferAllocator().buffer(capacity: size)
        buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in ULEB128.encode(UInt32(dictionary.count), to: ptr.baseAddress!) }
        buffer.writeBytes(dictionary)
        context.write(NIOAny(buffer), promise: nil)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var buffer = unwrapInboundIn(data)
        guard let handshakeResponseRaw = buffer.readInteger(as: HandshakeResponse.RawValue.self) else {
            logger.info("\(context.channel.addressDescription): invalid compression response received, close connection")
            context.close(promise: nil)
            return
        }

        guard let handshakeResponse = HandshakeResponse(rawValue: handshakeResponseRaw) else {
            logger.info("\(context.channel.addressDescription): invalid compression response \(handshakeResponseRaw) received, close connection")
            context.close(promise: nil)
            return
        }

        logger.debug("\(context.channel.addressDescription): server requested \(handshakeResponse) compression mode")

        let compressionMode = distributedSystem.compressionMode
        switch handshakeResponse {
        case .noCompression:
            switch compressionMode {
            case .disabled:
                break
            case .streaming:
                _ = context.pipeline.addHandler(
                    ChannelStreamCompressionOutboundHandler(distributedSystem),
                    name: ChannelStreamCompressionOutboundHandler.name,
                    position: .after(self))
                break
            case let .dictionary(dictionaryData):
                sendDictionary(dictionaryData.data.value, to: context)
                _ = context.pipeline.addHandler(
                    ChannelDictCompressionOutboundHandler(distributedSystem, dictionaryData.data),
                    name: ChannelDictCompressionOutboundHandler.name,
                    position: .after(self))
                break
            }
            context.fireChannelActive()
            if buffer.readableBytes > 0 {
                context.fireChannelRead(wrapInboundOut(buffer))
            }
            _ = context.pipeline.removeHandler(self)
        case .streamingCompression:
            switch compressionMode {
            case .disabled:
                break
            case .streaming:
                _ = context.pipeline.addHandler(
                    ChannelStreamCompressionOutboundHandler(distributedSystem),
                    name: ChannelStreamCompressionOutboundHandler.name,
                    position: .after(self))
            case let .dictionary(dictionaryData):
                sendDictionary(dictionaryData.data.value, to: context)
                _ = context.pipeline.addHandler(
                    ChannelDictCompressionOutboundHandler(distributedSystem, dictionaryData.data),
                    name: ChannelDictCompressionOutboundHandler.name,
                    position: .after(self))
                break
            }
            _ = context.pipeline.addHandler(
                ChannelStreamCompressionInboundHandler(distributedSystem),
                name: ChannelStreamCompressionInboundHandler.name,
                position: .before(channelHandler))
            context.fireChannelActive()
            if buffer.readableBytes > 0 {
                context.fireChannelRead(wrapInboundOut(buffer))
            }
            _ = context.pipeline.removeHandler(self)
        case .sameDictionary:
            if case let .dictionary(dictionaryData) = compressionMode {
                _ = context.pipeline.addHandler(
                    ChannelDictCompressionInboundHandler(distributedSystem, dictionaryData.data),
                    name: ChannelDictCompressionInboundHandler.name,
                    position: .before(channelHandler))
                _ = context.pipeline.addHandler(
                    ChannelDictCompressionOutboundHandler(distributedSystem, dictionaryData.data),
                    name: ChannelDictCompressionOutboundHandler.name,
                    position: .after(self))
                context.fireChannelActive()
                if buffer.readableBytes > 0 {
                    context.fireChannelRead(wrapInboundOut(buffer))
                }
                _ = context.pipeline.removeHandler(self)
            } else {
                logger.info("\(context.channel.addressDescription): unexpected response 'sameDictionary' received, close connection")
                context.close(promise: nil)
            }
        case .dictionary:
            switch compressionMode {
            case .disabled:
                break
            case .streaming:
                _ = context.pipeline.addHandler(
                    ChannelStreamCompressionOutboundHandler(distributedSystem),
                    name: ChannelStreamCompressionOutboundHandler.name,
                    position: .after(self))
            case let .dictionary(dictionaryData):
                sendDictionary(dictionaryData.data.value, to: context)
                _ = context.pipeline.addHandler(
                    ChannelDictCompressionOutboundHandler(distributedSystem, dictionaryData.data),
                    name: ChannelDictCompressionOutboundHandler.name,
                    position: .after(self))
                break
            }
            _ = context.pipeline.addHandler(
                DictionaryReceiver(distributedSystem, channelHandler),
                name: DictionaryReceiver.name,
                position: .after(self))
            context.fireChannelActive()
            if buffer.readableBytes > 0 {
                context.fireChannelRead(wrapInboundOut(buffer))
            }
            _ = context.pipeline.removeHandler(self)
        }
    }
}

struct BufferManager: ~Copyable {
    var buffers = Array(repeating: UnsafeMutableRawBufferPointer(start: nil, count: 0), count: 2)
    var nextIdx = 0

    deinit {
        for buffer in buffers {
            buffer.deallocate()
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

class ChannelCompressionInboundHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer

    static let statsKey = "bytes_decompressed"
    static let name = "inboundDecompression"

    private let distributedSystem: DistributedSystem
    var bytesDecompressed = UInt64(0)

    var logger: Logger { distributedSystem.loggerBox.value }

    init(_ distributedSystem: DistributedSystem) {
        self.distributedSystem = distributedSystem
    }

    deinit {
        distributedSystem.incrementStats([Self.statsKey: bytesDecompressed])
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        fatalError("should never be called")
    }
}

final class ChannelStreamCompressionInboundHandler: ChannelCompressionInboundHandler {
    private var lz4Stream = LZ4_streamDecode_t()
    private var bufferManager = BufferManager()

    override init(_ distributedSystem: DistributedSystem) {
        LZ4_setStreamDecode(&lz4Stream, nil, 0)
        super.init(distributedSystem)
    }

    override func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var bufferIn = unwrapInboundIn(data)
        _ = bufferIn.readInteger(as: UInt32.self) // skip message size
        do {
            let (messageSizeSize, messageSize) = try bufferIn.withUnsafeReadableBytes { try ULEB128.decode($0, as: UInt32.self) }
            bufferIn.moveReaderIndex(forwardBy: messageSizeSize)
            let buffer = bufferManager.getNextBuffer(capacity: Int(messageSize))
            let decompressedBytes = bufferIn.withUnsafeReadableBytes { bytes in
                LZ4_decompress_safe_continue(&lz4Stream,
                                             bytes.baseAddress!.assumingMemoryBound(to: Int8.self),
                                             buffer.baseAddress!.assumingMemoryBound(to: Int8.self),
                                             Int32(bytes.count),
                                             Int32(buffer.count))
            }
            if decompressedBytes == messageSize {
                var bufferOut = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt32>.size + Int(messageSize))
                bufferOut.writeInteger(messageSize)
                _ = bufferOut.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in
                    ptr.copyMemory(from: UnsafeRawBufferPointer(start: buffer.baseAddress, count: Int(messageSize)))
                    return Int(messageSize)
                }
                context.fireChannelRead(wrapInboundOut(bufferOut))
                bytesDecompressed += UInt64(messageSize)
                return
            }
        } catch {
        }
        logger.info("\(context.channel.addressDescription): invalid compressed message received, close connection")
        context.close(promise: nil)
    }
}

final class ChannelDictCompressionInboundHandler: ChannelCompressionInboundHandler {
    private let dictionary: BoxEx<UnsafeRawBufferPointer>
    private var lz4Stream = LZ4_streamDecode_t()

    init(_ distributedSystem: DistributedSystem, _ dictionary: BoxEx<UnsafeRawBufferPointer>) {
        self.dictionary = dictionary
        LZ4_setStreamDecode(&lz4Stream, dictionary.value.baseAddress, Int32(dictionary.value.count))
        super.init(distributedSystem)
    }

    override func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var bufferIn = unwrapInboundIn(data)
        _ = bufferIn.readInteger(as: UInt32.self) // skip message size
        do {
            let (messageSizeSize, messageSize) = try bufferIn.withUnsafeReadableBytes { try ULEB128.decode($0, as: UInt32.self) }
            bufferIn.moveReaderIndex(forwardBy: messageSizeSize)
            var bufferOut = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt32>.size + Int(messageSize))
            bufferOut.writeInteger(messageSize)
            _ = bufferOut.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { bytesOut in
                let decompressedBytes = bufferIn.withUnsafeReadableBytes { bytesIn in
                    var lz4Stream = self.lz4Stream
                    return LZ4_decompress_safe_continue(&lz4Stream,
                                                        bytesIn.baseAddress!.assumingMemoryBound(to: Int8.self),
                                                        bytesOut.baseAddress!.assumingMemoryBound(to: Int8.self),
                                                        Int32(bytesIn.count),
                                                        Int32(bytesOut.count))
                }
                return ((decompressedBytes < 0) ? 0 : Int(decompressedBytes))
            }
            if bufferOut.readableBytes == (MemoryLayout<UInt32>.size + Int(messageSize)) {
                context.fireChannelRead(wrapInboundOut(bufferOut))
                bytesDecompressed += UInt64(messageSize)
                return
            }
        } catch {
        }
        logger.info("\(context.channel.addressDescription): invalid compressed message received, close connection")
        context.close(promise: nil)
    }
}

class ChannelCompressionOutboundHandler: ChannelOutboundHandler {
    typealias OutboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    static let statsKey = "bytes_compressed"
    static let name = "outboundCompression"

    private let distributedSystem: DistributedSystem
    var bytesCompressed = UInt64(0)

    init(_ distributedSystem: DistributedSystem) {
        self.distributedSystem = distributedSystem
    }

    deinit {
        distributedSystem.incrementStats([Self.statsKey: bytesCompressed])
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        fatalError("should never be called")
    }
}

final class ChannelStreamCompressionOutboundHandler: ChannelCompressionOutboundHandler {
    private var lz4Stream = LZ4_stream_t()
    private var bufferManager = BufferManager()

    override init(_ distributedSystem: DistributedSystem) {
        LZ4_initStream(&lz4Stream, MemoryLayout<LZ4_stream_t>.size)
        super.init(distributedSystem)
    }

    override func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        var bufferIn = unwrapOutboundIn(data)
        guard let messageSize = bufferIn.readInteger(as: UInt32.self), bufferIn.readableBytes == messageSize else { fatalError("invalid message") }
        let compressBound = LZ4_compressBound(Int32(bufferIn.readableBytes))
        let srcBuffer = bufferManager.getNextBuffer(capacity: bufferIn.readableBytes)
        bufferIn.withUnsafeReadableBytes { srcBuffer.copyMemory(from: $0) }
        // LZ4_compressBound() returns value which even larger than size of the source data.
        // The compression ratio often is better than expected by LZ4_compressBound(),
        // so one possible further memory optimization is to allocate one big outbound buffer
        // and slice it with compressed data per each write operation.
        let messageSizeSize = ULEB128.size(messageSize)
        var bufferOut = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt32>.size + messageSizeSize + Int(compressBound))
        bufferOut.moveWriterIndex(forwardBy: MemoryLayout<UInt32>.size)
        bufferOut.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ULEB128.encode(UInt32(messageSize), to: $0.baseAddress!) }
        let compressedBytes = bufferOut.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in
            let compressedBytes = LZ4_compress_fast_continue(&lz4Stream,
                                                             srcBuffer.baseAddress!.assumingMemoryBound(to: Int8.self),
                                                             ptr.baseAddress!.assumingMemoryBound(to: Int8.self),
                                                             Int32(messageSize),
                                                             compressBound, 1)
            return Int(compressedBytes)
        }
        bufferOut.setInteger(UInt32(messageSizeSize + compressedBytes), at: 0)
        context.write(wrapOutboundOut(bufferOut), promise: promise)
        bytesCompressed += UInt64(messageSize)
    }
}

final class ChannelDictCompressionOutboundHandler: ChannelCompressionOutboundHandler {
    private let dictionary: BoxEx<UnsafeRawBufferPointer>
    private var lz4Stream = LZ4_stream_t()

    init(_ distributedSystem: DistributedSystem, _ dictionary: BoxEx<UnsafeRawBufferPointer>) {
        self.dictionary = dictionary
        LZ4_initStream(&lz4Stream, MemoryLayout<LZ4_stream_t>.size)
        LZ4_loadDict(&lz4Stream, dictionary.value.baseAddress!, Int32(dictionary.value.count))
        super.init(distributedSystem)
    }

    override func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        var bufferIn = unwrapOutboundIn(data)
        guard let messageSize = bufferIn.readInteger(as: UInt32.self), bufferIn.readableBytes == messageSize else { fatalError("invalid message") }
        let compressBound = LZ4_compressBound(Int32(bufferIn.readableBytes))
        let bufferOut = bufferIn.withUnsafeReadableBytes { bytesIn in
            // LZ4_compressBound() returns value which even larger than size of the source data.
            // The compression ratio often is better than expected by LZ4_compressBound(),
            // so one possible further memory optimization is to allocate one big outbound buffer
            // and slice it with compressed data per each write operation.
            let messageSizeSize = ULEB128.size(messageSize)
            var bufferOut = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt32>.size + messageSizeSize + Int(compressBound))
            bufferOut.moveWriterIndex(forwardBy: MemoryLayout<UInt32>.size)
            bufferOut.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ULEB128.encode(UInt32(messageSize), to: $0.baseAddress!) }
            let compressedBytes = bufferOut.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { bytesOut in
                var lz4Stream = self.lz4Stream
                let compressedBytes = LZ4_compress_fast_continue(&lz4Stream,
                                                                 bytesIn.baseAddress!.assumingMemoryBound(to: Int8.self),
                                                                 bytesOut.baseAddress!.assumingMemoryBound(to: Int8.self),
                                                                 Int32(messageSize),
                                                                 compressBound, 1)
                return Int(compressedBytes)
            }
            bufferOut.setInteger(UInt32(messageSizeSize + compressedBytes), at: 0)
            return bufferOut
        }
        context.write(wrapOutboundOut(bufferOut), promise: promise)
        bytesCompressed += UInt64(messageSize)
    }
}
