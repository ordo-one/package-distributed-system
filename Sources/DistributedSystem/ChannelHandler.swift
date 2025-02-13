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

extension Optional where Wrapped == SocketAddress {
    var addressDescription: String {
        self.map { $0.makeCopyWithoutHost().description } ?? "<nil-addr>"
    }
}

extension Channel {
    var addressDescription: String {
        "\(localAddress.addressDescription) -> \(remoteAddress.addressDescription)"
    }
}

final class ChannelHandler: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private var logger: Logger { actorSystem.logger }

    let id: UInt32
    private let actorSystem: DistributedSystem
    private let address: SocketAddress?
    private var writeBufferHighWatermark: Int
    private var targetFuncs = [String]()

    static let name = "channelHandler"

    private func logPrefix(_ channel: Channel) -> String {
        "\(channel.addressDescription)/\(Self.self)/\(id)"
    }

    init(_ id: UInt32, _ actorSystem: DistributedSystem, _ address: SocketAddress?, _ writeBufferHighWatermark: UInt64) {
        self.id = id
        self.actorSystem = actorSystem
        self.address = address
        self.writeBufferHighWatermark = Int(writeBufferHighWatermark)
    }

    func channelActive(context: ChannelHandlerContext) {
        let channel = context.channel
        logger.debug("\(logPrefix(channel)): channel active")
        actorSystem.setChannel(id, channel, forProcessAt: address)
    }

    func channelInactive(context: ChannelHandlerContext) {
        logger.info("\(logPrefix(context.channel)): connection closed")
        actorSystem.channelInactive(id, address)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var buffer = unwrapInboundIn(data)
        let channel = context.channel
        logger.trace("\(logPrefix(channel)): received \(buffer.readableBytes) bytes")
        actorSystem.channelRead(id, channel, &buffer, &targetFuncs)
    }

    // Flush it out. This can make use of gathering writes if multiple buffers are pending
    func channelReadComplete(context: ChannelHandlerContext) {
        context.flush()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        // 2024-02-20T20:14:13.570441+02:00 ERROR ds : [DistributedSystem] nil: network error: read(descriptor:pointer:size:): Operation timed out (errno: 60) ["port": 62871]
        // 2024-03-06T19:45:13.830792+02:00 ERROR ds : [DistributedSystem] nil: network error: read(descriptor:pointer:size:): Connection reset by peer (errno: 54) ["port": 55166]
        logger.info("\(logPrefix(context.channel)): Network error: \(error)")
        context.close(promise: nil)
    }

    func channelWritabilityChanged(context: ChannelHandlerContext) {
        let channel = context.channel
        if !channel.isWritable {
            logger.warning("\(logPrefix(channel)): outbound queue size reached \(writeBufferHighWatermark) bytes")
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
