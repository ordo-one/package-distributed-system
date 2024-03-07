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

    private let id: UInt32
    private let actorSystem: DistributedSystem
    private var address: SocketAddress?

    init(_ id: UInt32, _ actorSystem: DistributedSystem, _ address: SocketAddress?) {
        self.id = id
        self.actorSystem = actorSystem
        self.address = address
    }

    func channelActive(context: ChannelHandlerContext) {
        // 2024-03-07T12:51:20.589375+02:00 DEBUG ds : [DistributedSystem] [IPv4]192.168.0.9/192.168.0.9:58186/3: channel active ["port": 55056]
        // TODO: it would be nice to know "name/type" of remote process
        logger.info("Channel is active, remote: \(context.remoteAddressDescription)/\(id)")
        actorSystem.setChannel(id, context.channel, forProcessAt: address)
        if address == nil {
            // address is emoty for the server side,
            // and can be changed after actorSysten.setChannel() call
            address = context.remoteAddress
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        // 2024-03-07T12:48:24.806241+02:00 DEBUG ds : [DistributedSystem] [IPv4]192.168.0.9/192.168.0.9:53019/1: channel inactive ["port": 55056]
        // TODO: it would be nice to know "name/type" of remote process
        logger.info("Channel is inactive, remote: \(context.remoteAddressDescription)/\(id)")
        actorSystem.channelInactive(context.channel)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var buffer = unwrapInboundIn(data)
        logger.trace("\(context.remoteAddressDescription)/\(id): received \(buffer.readableBytes) bytes")
        actorSystem.channelRead(id, context.channel, &buffer)
    }

    // Flush it out. This can make use of gathering writes if multiple buffers are pending
    func channelReadComplete(context: ChannelHandlerContext) {
        context.flush()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        // 2024-02-20T20:14:13.570441+02:00 ERROR ds : [DistributedSystem] nil: network error: read(descriptor:pointer:size:): Operation timed out (errno: 60) ["port": 62871]
        // 2024-03-06T19:45:13.830792+02:00 ERROR ds : [DistributedSystem] nil: network error: read(descriptor:pointer:size:): Connection reset by peer (errno: 54) ["port": 55166]
        logger.info("Network error: \(error), remote: \((self.address == nil) ? "<unknown>" : address!.description)/\(id), will try to reconnect")
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
