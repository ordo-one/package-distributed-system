// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import Helpers
import Logging
internal import NIOCore

extension Channel {
    var remoteAddressDescription: String {
        remoteAddress?.description ?? "<nil>"
    }
}

extension ChannelHandlerContext {
    var remoteAddressDescription: String {
        remoteAddress?.description ?? "<nil>"
    }
}

class ChannelHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private var logger: Logger { actorSystem.logger }
    private let actorSystem: DistributedSystem
    private let address: SocketAddress?

    init(_ actorSystem: DistributedSystem, _ address: SocketAddress?) {
        self.actorSystem = actorSystem
        self.address = address
    }

    func channelActive(context: ChannelHandlerContext) {
        logger.debug("\(context.remoteAddressDescription): channel active")
        if let address {
            actorSystem.setChannel(context.channel, forProcessAt: address)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        logger.debug("\(context.remoteAddressDescription): channel inactive")
        actorSystem.channelInactive(context.channel)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var buffer = unwrapInboundIn(data)
        logger.trace("\(context.remoteAddressDescription): received \(buffer.readableBytes) bytes")
        actorSystem.channelRead(context.channel, &buffer)
    }

    // Flush it out. This can make use of gathering writes if multiple buffers are pending
    func channelReadComplete(context: ChannelHandlerContext) {
        context.flush()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        logger.error("\(String(describing: context.remoteAddress)): network error: \(error)")
        context.close(promise: nil)
    }
}

class StreamDecoder: ByteToMessageDecoder {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer

    private var loggerBox: Box<Logger>
    private var logger: Logger { loggerBox.value }

    init(_ loggerBox: Box<Logger>) {
        self.loggerBox = loggerBox
    }

    func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        logger.trace("\(context.channel.remoteAddressDescription): stream decoder: available \(buffer.readableBytes) bytes")
        if var messageSize = buffer.getInteger(at: buffer.readerIndex, as: UInt32.self) {
            messageSize += UInt32(MemoryLayout<UInt32>.size)
            if let slice = buffer.readSlice(length: Int(messageSize)) {
                context.fireChannelRead(wrapInboundOut(slice))
                return .continue
            }
        }
        return .needMoreData
    }
}
