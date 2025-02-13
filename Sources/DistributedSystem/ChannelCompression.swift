// Copyright 2024 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import Logging
import lz4
import NIOCore
import struct Foundation.Data

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

extension ChannelHandlerContext: @unchecked @retroactive Sendable {}
extension ByteToMessageHandler: @unchecked @retroactive Sendable {}

final class ChannelCompressionHandshakeServer: ChannelInboundHandler, RemovableChannelHandler, @unchecked Sendable {
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
        logger.debug("\(context.channel.addressDescription)/\(Self.self): channel active")
        if DistributedSystem.pingInterval > TimeAmount.zero {
            timer = context.eventLoop.scheduleTask(in: DistributedSystem.pingInterval*2) {
                self.logger.info("\(context.channel.addressDescription)/\(Self.self): session timeout, closing connection")
                context.close(promise: nil)
            }
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        if let timer {
            timer.cancel()
            self.timer = nil
        }
        logger.info("\(context.channel.addressDescription)/\(Self.self): connection closed")
    }

    private func sendResponse(_ response: HandshakeResponse, to context: ChannelHandlerContext) -> EventLoopFuture<Void> {
        assert(response != .dictionary)
        let size = MemoryLayout<HandshakeResponse.RawValue>.size
        var buffer = ByteBufferAllocator().buffer(capacity: size)
        buffer.writeInteger(response.rawValue)
        let promise = context.eventLoop.makePromise(of: Void.self)
        context.writeAndFlush(NIOAny(buffer), promise: promise)
        return promise.futureResult
    }

    private func sendDictionaryResponse(_ dictionary: Data, to context: ChannelHandlerContext) -> EventLoopFuture<Void> {
        logger.debug("\(context.channel.addressDescription)/\(Self.self): send dictionary (\(dictionary.count) bytes)")
        let size = MemoryLayout<HandshakeResponse.RawValue>.size + ULEB128.size(UInt32(dictionary.count)) + dictionary.count
        var buffer = ByteBufferAllocator().buffer(capacity: size)
        buffer.writeInteger(HandshakeResponse.dictionary.rawValue)
        buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ULEB128.encode(UInt32(dictionary.count), to: $0.baseAddress!) }
        buffer.writeBytes(dictionary)
        let promise = context.eventLoop.makePromise(of: Void.self)
        context.writeAndFlush(NIOAny(buffer), promise: promise)
        return promise.futureResult
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        if let timer {
            timer.cancel()
            self.timer = nil
        }

        var buffer = unwrapInboundIn(data)
        guard let handshakeRequestRaw = buffer.readInteger(as: HandshakeRequest.RawValue.self) else {
            logger.info("\(context.channel.addressDescription)/\(Self.self): invalid compression request received, closing connection")
            context.close(promise: nil)
            return
        }

        guard let handshakeRequest = HandshakeRequest(rawValue: handshakeRequestRaw) else {
            logger.info("\(context.channel.addressDescription)/\(Self.self): unsupported compression request '\(handshakeRequestRaw)' received, closing connection")
            context.close(promise: nil)
            return
        }

        logger.debug("\(context.channel.addressDescription)/\(Self.self): client compression mode: \(handshakeRequest)")

        let pipeline = context.pipeline
        let future: EventLoopFuture<Void>

        switch handshakeRequest {
        case .noCompression:
            let sendFuture = switch distributedSystem.compressionMode {
            case .disabled:
                sendResponse(.noCompression, to: context)
            case .streaming:
                sendResponse(.streamingCompression, to: context).flatMap {
                    pipeline.addHandler(ChannelStreamCompressionOutboundHandler(self.distributedSystem))
                }
            case let .dictionary(dictionary, _):
                sendDictionaryResponse(dictionary, to: context).flatMap {
                    pipeline.addHandler(ChannelDictCompressionOutboundHandler(self.distributedSystem, dictionary))
                }
            }
            future = sendFuture.flatMap {
                _ = pipeline.addHandler(ByteToMessageHandler(StreamDecoder(self.distributedSystem.loggerBox)))
                return pipeline.addHandler(self.channelHandler, name: ChannelHandler.name)
            }
        case .streamingCompression:
            let sendFuture = switch distributedSystem.compressionMode {
            case .disabled:
                sendResponse(.noCompression, to: context)
            case .streaming:
                sendResponse(.streamingCompression, to: context).flatMap {
                    pipeline.addHandler(ChannelStreamCompressionOutboundHandler(self.distributedSystem))
                }
            case let .dictionary(dictionary, _):
                sendDictionaryResponse(dictionary, to: context).flatMap {
                    pipeline.addHandler(ChannelDictCompressionOutboundHandler(self.distributedSystem, dictionary))
                }
            }
            future = sendFuture.flatMap {
                _ = pipeline.addHandler(ByteToMessageHandler(StreamDecoder(self.distributedSystem.loggerBox)))
                _ = pipeline.addHandler(ChannelStreamCompressionInboundHandler(self.distributedSystem))
                return pipeline.addHandler(self.channelHandler, name: ChannelHandler.name)
            }
        case .dictionaryCompression:
            guard let checksum = buffer.readInteger(as: UInt32.self) else {
                logger.info("\(context.channel.addressDescription): invalid compression request received, close connection")
                context.close(promise: nil)
                return
            }
            switch distributedSystem.compressionMode {
            case .disabled:
                future = sendResponse(.noCompression, to: context).flatMap {
                    return pipeline.addHandler(DictionaryReceiver(self.distributedSystem, self.channelHandler))
                }
            case .streaming:
                future = sendResponse(.streamingCompression, to: context).flatMap {
                    _ = pipeline.addHandler(DictionaryReceiver(self.distributedSystem, self.channelHandler))
                    return pipeline.addHandler(ChannelStreamCompressionOutboundHandler(self.distributedSystem))
                }
            case let .dictionary(dictionary, dictionaryChecksum):
                if checksum == dictionaryChecksum {
                    // same dictionary on both sides
                    future = sendResponse(.sameDictionary, to: context).flatMap {
                        _ = pipeline.addHandler(ChannelDictCompressionOutboundHandler(self.distributedSystem, dictionary))
                        _ = pipeline.addHandler(ByteToMessageHandler(StreamDecoder(self.distributedSystem.loggerBox)))
                        _ = pipeline.addHandler(ChannelDictCompressionInboundHandler(self.distributedSystem, dictionary))
                        return pipeline.addHandler(self.channelHandler, name: ChannelHandler.name)
                    }
                } else {
                    future = sendDictionaryResponse(dictionary, to: context).flatMap {
                        _ = pipeline.addHandler(DictionaryReceiver(self.distributedSystem, self.channelHandler))
                        return pipeline.addHandler(ChannelDictCompressionOutboundHandler(self.distributedSystem, dictionary))
                    }
                }
            }
        }

        let prevContext: ChannelHandlerContext
        do {
            prevContext = try pipeline.syncOperations.context(handlerType: ChannelCounters.self)
        } catch {
            logger.error("\(context.channel.addressDescription)/\(Self.self): \(error), closing connection")
            context.close(promise: nil)
            return
        }

        future.flatMap { [buffer] in
            _ = pipeline.removeHandler(self)
            prevContext.fireChannelActive()
            if buffer.readableBytes > 0 {
                prevContext.fireChannelRead(self.wrapInboundOut(buffer))
            }
            return prevContext.eventLoop.makeSucceededVoidFuture()
        }.whenFailure {
            self.logger.error("\(context.channel.addressDescription)/\(Self.self): \($0), closing connection")
            context.close(promise: nil)
        }
    }
}

final class DictionaryReceiver: ChannelInboundHandler, RemovableChannelHandler, @unchecked Sendable {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer

    private let distributedSystem: DistributedSystem
    private let channelHandler: ChannelHandler
    private var timer: Scheduled<Void>?
    private var dictionary = Data()
    private var dictionarySize = 0
    private var dictionaryBytesReceived = 0

    private var logger: Logger { distributedSystem.loggerBox.value }

    init(_ distributedSystem: DistributedSystem, _ channelHandler: ChannelHandler) {
        self.distributedSystem = distributedSystem
        self.channelHandler = channelHandler
    }

    private func startTimer(_ context: ChannelHandlerContext, _ timeout: TimeAmount, _ lastDictionaryBytesReceived: Int) {
        let eventLoop = context.eventLoop
        timer = eventLoop.scheduleTask(in: timeout) {
            let dictionaryBytesReceived = self.dictionaryBytesReceived
            if dictionaryBytesReceived > lastDictionaryBytesReceived {
                self.startTimer(context, timeout, dictionaryBytesReceived)
            } else {
                self.logger.info("\(context.channel.addressDescription)/\(Self.self): session timeout, closing connection")
                context.close(promise: nil)
            }
        }
    }

    func channelActive(context: ChannelHandlerContext) {
        logger.debug("\(context.channel.addressDescription)/\(Self.self): channel active")
        if DistributedSystem.pingInterval > TimeAmount.zero {
            startTimer(context, DistributedSystem.pingInterval*2, 0)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        logger.info("\(context.channel.addressDescription)/\(Self.self): connection closed")
        if let timer {
            timer.cancel()
            self.timer = nil
        }
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var buffer = unwrapInboundIn(data)
        if dictionarySize == 0 {
            do {
                let (sizeSize, dictionarySize) = try buffer.withUnsafeReadableBytes { try ULEB128.decode($0, as: UInt32.self) }
                buffer.moveReaderIndex(forwardBy: sizeSize)
                self.dictionarySize = Int(dictionarySize)
            } catch {
                // Is there any probability to receive less than 5 bytes?
                // Let's just close connection for now in such case.
                self.logger.info("\(context.channel.addressDescription): invalid dictionary block received, close connection")
                context.close(promise: nil)
                return
            }
        }

        let bytesRemaining = (dictionarySize - dictionaryBytesReceived)
        let copyBytes = min(bytesRemaining, buffer.readableBytes)
        buffer.withUnsafeReadableBytes {
            $0.withMemoryRebound(to: UInt8.self) {
                dictionary.append($0.baseAddress!, count: copyBytes)
            }
        }
        buffer.moveReaderIndex(forwardBy: copyBytes)
        dictionaryBytesReceived += copyBytes
        if dictionaryBytesReceived < dictionarySize {
            return
        }

        if let timer {
            timer.cancel()
            self.timer = nil
        }

        logger.debug("\(context.channel.addressDescription)/\(Self.self): received dictionary (\(dictionary.count) bytes)")

        let pipeline = context.pipeline
        let prevContext: ChannelHandlerContext
        do {
            prevContext = try pipeline.syncOperations.context(name: ChannelCounters.name)
        } catch {
            logger.error("\(context.channel.addressDescription)/\(Self.self): \(error), closing connection")
            return
        }

        _ = pipeline.removeHandler(self)
        _ = pipeline.addHandler(ByteToMessageHandler(StreamDecoder(distributedSystem.loggerBox)))
        _ = pipeline.addHandler(ChannelDictCompressionInboundHandler(distributedSystem, dictionary))
        _ = pipeline.addHandler(channelHandler, name: ChannelHandler.name)

        prevContext.fireChannelActive()
        if buffer.readableBytes > 0 {
            prevContext.fireChannelRead(wrapInboundOut(buffer))
        }
    }
}

final class ChannelCompressionHandshakeClient: ChannelInboundHandler, RemovableChannelHandler, @unchecked Sendable {
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
        logger.debug("\(context.channel.addressDescription)/\(Self.self): channel active")
        let compressionMode = distributedSystem.compressionMode
        let promise = context.eventLoop.makePromise(of: Void.self)
        switch compressionMode {
        case .disabled:
            var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<HandshakeRequest.RawValue>.size)
            buffer.writeInteger(HandshakeRequest.noCompression.rawValue)
            context.writeAndFlush(NIOAny(buffer), promise: promise)
        case .streaming:
            var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<HandshakeRequest.RawValue>.size)
            buffer.writeInteger(HandshakeRequest.streamingCompression.rawValue)
            context.writeAndFlush(NIOAny(buffer), promise: promise)
        case let .dictionary(_, dictionaryChecksum):
            var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<HandshakeRequest.RawValue>.size + MemoryLayout<UInt32>.size)
            buffer.writeInteger(HandshakeRequest.dictionaryCompression.rawValue)
            buffer.writeInteger(dictionaryChecksum)
            context.writeAndFlush(NIOAny(buffer), promise: promise)
        }

        if DistributedSystem.pingInterval > TimeAmount.zero {
            promise.futureResult.whenSuccess {
                self.timer = context.eventLoop.scheduleTask(in: DistributedSystem.pingInterval*2) {
                    self.logger.info("\(context.channel.addressDescription)/\(Self.self): session timeout, closing connection")
                    context.close(promise: nil)
                }
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

    private func sendDictionary(_ dictionary: Data, to context: ChannelHandlerContext) -> EventLoopFuture<Void> {
        logger.debug("\(context.channel.addressDescription)/\(Self.self): send dictionary (\(dictionary.count) bytes)")
        let size = ULEB128.size(UInt32(dictionary.count)) + dictionary.count
        var buffer = ByteBufferAllocator().buffer(capacity: size)
        buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in ULEB128.encode(UInt32(dictionary.count), to: ptr.baseAddress!) }
        buffer.writeBytes(dictionary)
        let promise = context.eventLoop.makePromise(of: Void.self)
        context.writeAndFlush(NIOAny(buffer), promise: promise)
        return promise.futureResult
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        if let timer {
            timer.cancel()
            self.timer = nil
        }

        var buffer = unwrapInboundIn(data)
        guard let handshakeResponseRaw = buffer.readInteger(as: HandshakeResponse.RawValue.self) else {
            logger.info("\(context.channel.addressDescription): invalid compression response received, closing connection")
            context.close(promise: nil)
            return
        }

        guard let handshakeResponse = HandshakeResponse(rawValue: handshakeResponseRaw) else {
            logger.info("\(context.channel.addressDescription): invalid compression response \(handshakeResponseRaw) received, closing connection")
            context.close(promise: nil)
            return
        }

        logger.debug("\(context.channel.addressDescription): server compression mode: \(handshakeResponse)")

        let pipeline = context.pipeline
        let future: EventLoopFuture<Void>
        switch handshakeResponse {
        case .noCompression:
            let sendFuture = switch distributedSystem.compressionMode {
            case .disabled:
                context.eventLoop.makeSucceededVoidFuture()
            case .streaming:
                pipeline.addHandler(ChannelStreamCompressionOutboundHandler(distributedSystem))
            case let .dictionary(dictionary, _):
                sendDictionary(dictionary, to: context).flatMap {
                    pipeline.addHandler(ChannelDictCompressionOutboundHandler(self.distributedSystem, dictionary))
                }
            }
            future = sendFuture.flatMap {
                _ = pipeline.addHandler(ByteToMessageHandler(StreamDecoder(self.distributedSystem.loggerBox)))
                return pipeline.addHandler(self.channelHandler, name: ChannelHandler.name)
            }
        case .streamingCompression:
            let sendFuture = switch distributedSystem.compressionMode {
            case .disabled:
                context.eventLoop.makeSucceededVoidFuture()
            case .streaming:
                pipeline.addHandler(ChannelStreamCompressionOutboundHandler(distributedSystem))
            case let .dictionary(dictionary, _):
                sendDictionary(dictionary, to: context).flatMap {
                    pipeline.addHandler(ChannelDictCompressionOutboundHandler(self.distributedSystem, dictionary))
                }
            }
            future = sendFuture.flatMap {
                _ = pipeline.addHandler(ByteToMessageHandler(StreamDecoder(self.distributedSystem.loggerBox)))
                _ = pipeline.addHandler(ChannelStreamCompressionInboundHandler(self.distributedSystem))
                return pipeline.addHandler(self.channelHandler, name: ChannelHandler.name)
            }
        case .sameDictionary:
            if case let .dictionary(dictionary, _) = distributedSystem.compressionMode {
                _ = pipeline.addHandler(ChannelDictCompressionOutboundHandler(distributedSystem, dictionary))
                _ = pipeline.addHandler(ByteToMessageHandler(StreamDecoder(self.distributedSystem.loggerBox)))
                _ = pipeline.addHandler(ChannelDictCompressionInboundHandler(distributedSystem, dictionary))
                future = pipeline.addHandler(self.channelHandler, name: ChannelHandler.name)
            } else {
                logger.info("\(context.channel.addressDescription): unexpected response 'sameDictionary' received, closing connection")
                context.close(promise: nil)
                future = context.eventLoop.makeSucceededVoidFuture()
            }
        case .dictionary:
            let sendFuture = switch distributedSystem.compressionMode {
            case .disabled:
                context.eventLoop.makeSucceededVoidFuture()
            case .streaming:
                pipeline.addHandler(ChannelStreamCompressionOutboundHandler(distributedSystem))
            case let .dictionary(dictionary, _):
                sendDictionary(dictionary, to: context).flatMap {
                    pipeline.addHandler(ChannelDictCompressionOutboundHandler(self.distributedSystem, dictionary))
                }
            }
            future = sendFuture.flatMap {
                return pipeline.addHandler(DictionaryReceiver(self.distributedSystem, self.channelHandler))
            }
        }

        let prevContext: ChannelHandlerContext
        do {
            prevContext = try pipeline.syncOperations.context(handlerType: ChannelCounters.self)
        } catch {
            logger.error("\(context.channel.addressDescription)/\(Self.self): \(error), closing connection")
            return
        }

        future.flatMap { [buffer] in
            _ = pipeline.removeHandler(self)
            prevContext.fireChannelActive()
            if buffer.readableBytes > 0 {
                prevContext.fireChannelRead(self.wrapInboundOut(buffer))
            }
            return prevContext.eventLoop.makeSucceededVoidFuture()
        }.whenFailure {
            self.logger.error("\(context.channel.addressDescription)/\(Self.self): \($0), closing connection")
            context.close(promise: nil)
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

struct ManagedUnsafeRawPointer: ~Copyable {
    let ptr: UnsafeRawPointer

    init(_ data: Data) {
        ptr = data.withUnsafeBytes {
            let ptr = UnsafeMutableRawPointer.allocate(byteCount: data.count, alignment: 0)
            ptr.copyMemory(from: $0.baseAddress!, byteCount: $0.count)
            return UnsafeRawPointer(ptr)
        }
    }

    deinit {
        ptr.deallocate()
    }
}

class ChannelCompressionInboundHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer

    static let statsKey = "bytes_decompressed"

    let distributedSystem: DistributedSystem
    var bytesDecompressed = UInt64(0)

    var logger: Logger { distributedSystem.loggerBox.value }

    init(_ distributedSystem: DistributedSystem) {
        self.distributedSystem = distributedSystem
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        fatalError("should never be called")
    }

    func channelInactive(context: ChannelHandlerContext) {
        context.fireChannelInactive()
        distributedSystem.incrementStats([Self.statsKey: bytesDecompressed])
    }
}

final class ChannelStreamCompressionInboundHandler: ChannelCompressionInboundHandler, @unchecked Sendable {
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

final class ChannelDictCompressionInboundHandler: ChannelCompressionInboundHandler, @unchecked Sendable {
    private let dictionary: ManagedUnsafeRawPointer
    private let lz4Stream: LZ4_streamDecode_t

    init(_ distributedSystem: DistributedSystem, _ dictionary: Data) {
        self.dictionary = .init(dictionary)
        var lz4Stream = LZ4_streamDecode_t()
        LZ4_setStreamDecode(&lz4Stream, self.dictionary.ptr, Int32(dictionary.count))
        self.lz4Stream = lz4Stream
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
        logger.error("\(context.channel.addressDescription)/\(Self.self): invalid compressed message received, closing connection")
        context.close(promise: nil)
    }
}

class ChannelCompressionOutboundHandler: ChannelInboundHandler, ChannelOutboundHandler, @unchecked Sendable {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer
    typealias OutboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    static let statsKey = "bytes_compressed"

    let distributedSystem: DistributedSystem
    var bytesCompressed = UInt64(0)

    init(_ distributedSystem: DistributedSystem) {
        self.distributedSystem = distributedSystem
    }

    func channelInactive(context: ChannelHandlerContext) {
        context.fireChannelInactive()
        distributedSystem.incrementStats([Self.statsKey: bytesCompressed])
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        fatalError("should never be called")
    }
}

final class ChannelStreamCompressionOutboundHandler: ChannelCompressionOutboundHandler, @unchecked Sendable {
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

final class ChannelDictCompressionOutboundHandler: ChannelCompressionOutboundHandler, @unchecked Sendable {
    private let dictionary: ManagedUnsafeRawPointer
    private let lz4Stream: LZ4_stream_t

    init(_ distributedSystem: DistributedSystem, _ dictionary: Data) {
        self.dictionary = .init(dictionary)
        var lz4Stream = LZ4_stream_t()
        LZ4_initStream(&lz4Stream, MemoryLayout<LZ4_stream_t>.size)
        LZ4_loadDict(&lz4Stream, self.dictionary.ptr, Int32(dictionary.count))
        self.lz4Stream = lz4Stream
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
