// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import class Helpers.Box
import Logging
internal import NIOCore

final class ChannelHandshakeServer: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer

    private let distributedSystem: DistributedSystem
    private let channelHandler: ChannelHandler
    private var timer: Scheduled<Void>?

    private var logger: Logger { distributedSystem.loggerBox.value }

    static let name = "handshake"
    static let hexDumpMaxBytes = 32

    init(_ distributedSystem: DistributedSystem, _ channelHandler: ChannelHandler) {
        self.distributedSystem = distributedSystem
        self.channelHandler = channelHandler
    }

    func channelActive(context: ChannelHandlerContext) {
        logger.info("\(context.channel.addressDescription): connection accepted")
        if DistributedSystem.pingInterval.nanoseconds > 0 {
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

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        if let timer {
            timer.cancel()
            self.timer = nil
        }
        var buffer = unwrapInboundIn(data)
        if let clientProtocolVersionMajor = buffer.readInteger(as: UInt16.self),
           let clientProtocolVersionMinor = buffer.readInteger(as: UInt16.self),
           buffer.readableBytes == 0 {
            if clientProtocolVersionMajor == DistributedSystem.protocolVersionMajor,
               clientProtocolVersionMinor <= DistributedSystem.protocolVersionMinor {
                // handshake ok
                let prevContext: ChannelHandlerContext
                do {
                    prevContext = try context.pipeline.syncOperations.context(handlerType: ChannelCounters.self)
                } catch {
                    self.logger.error("\(context.channel.addressDescription)/\(Self.self): \(error), closing connection")
                    context.close(promise: nil)
                    return
                }

                var reply = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt16>.size)
                reply.writeInteger(UInt16(0))
                let promise = context.eventLoop.makePromise(of: Void.self)
                context.writeAndFlush(NIOAny(reply), promise: promise)

                promise.futureResult.flatMap {
                    let pipeline = context.pipeline
                    _ = pipeline.removeHandler(self)
                    _ = pipeline.addHandler(ChannelCompressionHandshakeServer(self.distributedSystem, self.channelHandler))
                    prevContext.fireChannelActive()
                    return prevContext.eventLoop.makeSucceededVoidFuture()
                }.whenFailure {
                    self.logger.error("\(context.channel.addressDescription)/\(Self.self): \($0), closing connection")
                    context.close(promise: nil)
                }
            } else {
                logger.info("\(context.channel.addressDescription): client protocol version \(clientProtocolVersionMajor).\(clientProtocolVersionMinor) not compatible with server version \(DistributedSystem.protocolVersionMajor).\(DistributedSystem.protocolVersionMinor), closing connection")
                var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt16>.size*3)
                buffer.writeInteger(UInt16(MemoryLayout<UInt16>.size * 2)) // indicate protocol version mismatch
                buffer.writeInteger(DistributedSystem.protocolVersionMajor)
                buffer.writeInteger(DistributedSystem.protocolVersionMinor)
                context.writeAndFlush(NIOAny(buffer), promise: nil)
                // We expect client will close TCP connection after receive this message.
                // If we would close TCP connection now then client not necessary will receive it.
            }
        } else {
            logger.info("\(context.channel.addressDescription): invalid handshake request received, closing connection\n\(unwrapInboundIn(data).hexDump(format: ByteBuffer.HexDumpFormat.detailed(maxBytes: Self.hexDumpMaxBytes)))")
            context.close(promise: nil)
        }
    }
}

final class ChannelHandshakeClient: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer

    private let loggerBox: Box<Logger>
    private var logger: Logger { loggerBox.value }

    private var timer: Scheduled<Void>?

    static let hexDumpMaxBytes = ChannelHandshakeServer.hexDumpMaxBytes

    init(_ loggerBox: Box<Logger>) {
        self.loggerBox = loggerBox
    }

    func channelActive(context: ChannelHandlerContext) {
        logger.info("\(context.channel.addressDescription): connected")

        var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt16>.size + MemoryLayout<UInt16>.size)
        buffer.writeInteger(DistributedSystem.protocolVersionMajor)
        buffer.writeInteger(DistributedSystem.protocolVersionMinor)
        context.writeAndFlush(NIOAny(buffer), promise: nil)

        if DistributedSystem.pingInterval.nanoseconds > 0 {
            timer = context.eventLoop.scheduleTask(in: DistributedSystem.pingInterval*2) {
                self.logger.info("\(context.channel.addressDescription)\(Self.self): session timeout, closing connection")
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

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        if let timer {
            timer.cancel()
            self.timer = nil
        }
        var buffer = unwrapInboundIn(data)
        if let messageSize = buffer.readInteger(as: UInt16.self) {
            if messageSize == 0 {
                // handshake ok
                context.fireChannelActive()
                let readableBytes = buffer.readableBytes
                if readableBytes > 0 {
                    // there also can be messages server sent to client after handshake reply
                    if let slice = buffer.readSlice(length: readableBytes) {
                        context.fireChannelRead(.init(slice))
                    }
                }
                _ = context.pipeline.removeHandler(self)
            } else {
                if messageSize == MemoryLayout<UInt16>.size * 2,
                   let serverProtocolVersionMajor = buffer.readInteger(as: UInt16.self),
                   let serverProtocolVersionMinor = buffer.readInteger(as: UInt16.self),
                   buffer.readableBytes == 0 {
                    logger.warning("\(context.channel.addressDescription): client protocol version \(DistributedSystem.protocolVersionMajor).\(DistributedSystem.protocolVersionMinor) is not compatible with server version \(serverProtocolVersionMajor).\(serverProtocolVersionMinor), closing connection")
                } else {
                    logger.warning("\(context.channel.addressDescription): invalid handshake response received, closing connection\(unwrapInboundIn(data).hexDump(format: ByteBuffer.HexDumpFormat.detailed(maxBytes: Self.hexDumpMaxBytes)))")
                }
                context.close(promise: nil)
            }
        } else {
            logger.warning("\(context.channel.addressDescription): invalid handshake responce received, closing connection\(unwrapInboundIn(data).hexDump(format: ByteBuffer.HexDumpFormat.detailed(maxBytes: Self.hexDumpMaxBytes)))")
            context.close(promise: nil)
        }
    }
}
