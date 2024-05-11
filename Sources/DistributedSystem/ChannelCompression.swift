// Copyright 2024 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import Logging
internal import NIOCore

class ChannelCompressionHandshakeServer: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer

    private let logger: Logger
    private let streamHandler: NIOCore.ChannelHandler
    private var timer: Scheduled<Void>?

    init(_ logger: Logger, _ streamHandler: NIOCore.ChannelHandler) {
        self.logger = logger
        self.streamHandler = streamHandler
    }

    func channelActive(context: ChannelHandlerContext) {
        timer = context.eventLoop.scheduleTask(in: DistributedSystem.pingInterval*2) {
            let channel = context.channel
            self.logger.info("Session timeout for client @ \(channel.remoteAddressDescription), close connection.")
            channel.close(promise: nil)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        // override doing nothing,
        // do not need to propagate channelInacteve() further down the pipeline
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        if let timer {
            timer.cancel()
            self.timer = nil
        }

        var buffer = unwrapInboundIn(data)
        if let compression = buffer.readInteger(as: UInt8.self),
           buffer.readableBytes == 0 {
            if compression == 0 {
                context.fireChannelActive()
            } else {
                _ = context.pipeline.addHandler(ChannelCompressionInboundHandler(), position: .after(streamHandler))
                _ = context.pipeline.addHandler(ChannelCompressionOutboundHandler(), position: .last)
                context.fireChannelActive()
            }
            _ = context.pipeline.removeHandler(self)
        } else {
            let channel = context.channel
            logger.info("Invalid compression request received from client @ \(channel.remoteAddressDescription), close connection.")
            channel.close(promise: nil)
        }
    }
}

class ChannelCompressionHandshakeClient: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private let logger: Logger
    private let compression: UInt8
    private let streamHandler: NIOCore.ChannelHandler
    private var timer: Scheduled<Void>?

    init(_ logger: Logger, _ compression: UInt8, _ streamHandler: NIOCore.ChannelHandler) {
        self.logger = logger
        self.compression = compression
        self.streamHandler = streamHandler
    }

    func channelActive(context: ChannelHandlerContext) {
        var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt8>.size)
        buffer.writeInteger(compression)
        _ = context.writeAndFlush(.data(buffer), promise: nil)
    }

    func channelInactive(context: ChannelHandlerContext) {
        // override doing nothing,
        // do not need to propagate channelInacteve() further down the pipeline
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        if let timer {
            timer.cancel()
            self.timer = nil
        }
        var buffer = unwrapInboundIn(data)
        if let compression = buffer.readInteger(as: UInt8.self),
           buffer.readableBytes == 0 {
            if compression == 0 {
                context.fireChannelActive()
            } else {
                _ = context.pipeline.addHandler(ChannelCompressionInboundHandler(), position: .after(streamHandler))
                context.fireChannelActive()
            }
            _ = context.pipeline.removeHandler(self)
        } else {
            let channel = context.channel
            logger.info("Invalid compression request received from client @ \(channel.remoteAddressDescription), close connection.")
            channel.close(promise: nil)
        }
    }
}

class ChannelCompressionInboundHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
    }
}

class ChannelCompressionOutboundHandler: ChannelOutboundHandler {
    typealias OutboundIn = ByteBuffer
}
