// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import Logging
import NIOCore

extension SocketAddress {
    func makeCopyWithoutHost() -> SocketAddress {
        switch self {
        case let .v4(addr): SocketAddress(addr.address, host: "")
        case let .v6(addr): SocketAddress(addr.address, host: "")
        default: self
        }
    }
}

extension Channel {
    var addressDescription: String {
        let localAddress = self.localAddress
        let remoteAddress = self.remoteAddress
        return "\(localAddress == nil ? "<nil-addr>" : localAddress!.makeCopyWithoutHost().description) -> \(remoteAddress == nil ? "<nil-addr>" : remoteAddress!.makeCopyWithoutHost().description)"
    }
}

class ChannelHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private var logger: Logger { actorSystem.logger }

    private let id: UInt32
    private let actorSystem: DistributedSystem
    private let address: SocketAddress?
    private var writeBufferHighWatermark: Int
    private var targetFuncs = [String]()

    static let name = "channelHandler"

    init(_ id: UInt32, _ actorSystem: DistributedSystem, _ address: SocketAddress?, _ writeBufferHighWatermark: UInt64) {
        self.id = id
        self.actorSystem = actorSystem
        self.address = address
        self.writeBufferHighWatermark = Int(writeBufferHighWatermark)
    }

    func channelActive(context: ChannelHandlerContext) {
        // 2024-03-07T12:51:20.589375+02:00 DEBUG ds : [DistributedSystem] [IPv4]192.168.0.9/192.168.0.9:58186/3: channel active ["port": 55056]
        // TODO: it would be nice to know "name/type" of remote process
        let channel = context.channel
        logger.debug("\(channel.addressDescription)/\(Self.self): channel active")

        actorSystem.setChannel(id, channel, forProcessAt: address)
    }

    func channelInactive(context: ChannelHandlerContext) {
        // 2024-03-07T12:48:24.806241+02:00 DEBUG ds : [DistributedSystem] [IPv4]192.168.0.9/192.168.0.9:53019/1: channel inactive ["port": 55056]
        // TODO: it would be nice to know "name/type" of remote process
        logger.info("\(context.channel.addressDescription)/\(Self.self): connection closed")
        actorSystem.channelInactive(id, address)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var buffer = unwrapInboundIn(data)
        logger.trace("\(context.channel.addressDescription)/\(id): received \(buffer.readableBytes) bytes")
        actorSystem.channelRead(id, context.channel, &buffer, &targetFuncs)
    }

    // Flush it out. This can make use of gathering writes if multiple buffers are pending
    func channelReadComplete(context: ChannelHandlerContext) {
        context.flush()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        // 2024-02-20T20:14:13.570441+02:00 ERROR ds : [DistributedSystem] nil: network error: read(descriptor:pointer:size:): Operation timed out (errno: 60) ["port": 62871]
        // 2024-03-06T19:45:13.830792+02:00 ERROR ds : [DistributedSystem] nil: network error: read(descriptor:pointer:size:): Connection reset by peer (errno: 54) ["port": 55166]
        logger.info("\(context.channel.addressDescription): Network error: \(error)")
        context.close(promise: nil)
    }

    func channelWritabilityChanged(context: ChannelHandlerContext) {
        let channel = context.channel
        if !channel.isWritable {
            logger.warning("\(channel.addressDescription): outbound queue size reached \(writeBufferHighWatermark) bytes")
            writeBufferHighWatermark *= 2

            let writeBufferWaterMark = ChannelOptions.Types.WriteBufferWaterMark(low: writeBufferHighWatermark/2, high: writeBufferHighWatermark)
            _ = channel.setOption(ChannelOptions.writeBufferWaterMark, value: writeBufferWaterMark)
        }
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
        logger.trace("\(context.channel.addressDescription): stream decoder: available \(buffer.readableBytes) bytes")
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
