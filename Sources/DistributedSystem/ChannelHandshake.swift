// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import Logging
internal import NIOCore

class ChannelHandshakeServer: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer

    private let logger: Logger
    private var timer: Scheduled<Void>?

    init(_ logger: Logger) {
        self.logger = logger
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
        if let clientProtocolVersionMajor = buffer.readInteger(as: UInt16.self),
           let clientProtocolVersionMinor = buffer.readInteger(as: UInt16.self),
           buffer.readableBytes == 0 {
            if clientProtocolVersionMajor == DistributedSystem.protocolVersionMajor,
               clientProtocolVersionMinor <= DistributedSystem.protocolVersionMinor {
                // handshake ok
                var reply = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt16>.size)
                reply.writeInteger(UInt16(0))
                context.writeAndFlush(NIOAny(reply), promise: nil)
                context.fireChannelActive()
                _ = context.pipeline.removeHandler(self)
            } else {
                logger.info("Client protocol version \(clientProtocolVersionMajor).\(clientProtocolVersionMajor) not compatible with server version \(DistributedSystem.protocolVersionMajor).\(DistributedSystem.protocolVersionMinor), close connection.")
                var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt16>.size*3)
                buffer.writeInteger(UInt16(MemoryLayout<UInt16>.size * 2))
                buffer.writeInteger(DistributedSystem.protocolVersionMajor)
                buffer.writeInteger(DistributedSystem.protocolVersionMinor)
                context.writeAndFlush(NIOAny(buffer), promise: nil)
                // We expect client will close TCP connection after receive this message.
                // If we would close TCP connection now then client not necessary will receive it.
            }
        } else {
            let channel = context.channel
            logger.info("Invalid handshake request received from client @ \(channel.remoteAddressDescription), close connection.")
            context.close(promise: nil)
        }
    }
}

class ChannelHandshakeClient: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private let logger: Logger
    private var timer: Scheduled<Void>?

    init(_ logger: Logger) {
        self.logger = logger
    }

    func channelActive(context: ChannelHandlerContext) {
        var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt16>.size + MemoryLayout<UInt16>.size)
        buffer.writeInteger(DistributedSystem.protocolVersionMajor)
        buffer.writeInteger(DistributedSystem.protocolVersionMinor)
        context.writeAndFlush(NIOAny(buffer), promise: nil)

        timer = context.eventLoop.scheduleTask(in: DistributedSystem.pingInterval*2) {
            let channel = context.channel
            self.logger.info("Session timeout for server @ \(channel.remoteAddressDescription), close connection.")
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
                if messageSize == 4,
                   let serverProtocolVersionMajor = buffer.readInteger(as: UInt16.self),
                   let serverProtocolVersionMinor = buffer.readInteger(as: UInt16.self),
                   buffer.readableBytes == 0 {
                    logger.info("Client protocol version \(DistributedSystem.protocolVersionMajor).\(DistributedSystem.protocolVersionMinor) not compatible with server version \(serverProtocolVersionMajor).\(serverProtocolVersionMinor), close connection.")
                } else {
                    let channel = context.channel
                    logger.info("Invalid handshake response received from server @ \(channel.remoteAddressDescription), close connection.")
                }
                context.close(promise: nil)
            }
        } else {
            let channel = context.channel
            logger.info("Invalid handshake responce received from server @ \(channel.remoteAddressDescription), close connection.")
            context.close(promise: nil)
        }
    }
}
