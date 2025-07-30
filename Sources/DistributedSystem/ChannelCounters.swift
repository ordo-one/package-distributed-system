// Copyright 2024 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import NIOCore
import Synchronization

final class ChannelCounters: ChannelInboundHandler, ChannelOutboundHandler, @unchecked Sendable {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer
    typealias OutboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private let distributedSystem: DistributedSystem
    let bytesReceived = Atomic<UInt64>(0)
    let bytesSent = Atomic<UInt64>(0)

    static let name = "channelCounters"
    static let keyBytesReceived = "bytes_received"
    static let keyBytesSent = "bytes_sent"

    var stats: [String: UInt64] { [
            Self.keyBytesReceived: bytesReceived.load(ordering: .relaxed),
            Self.keyBytesSent: bytesSent.load(ordering: .relaxed)
        ]
    }

    init(_ distributedSystem: DistributedSystem) {
        self.distributedSystem = distributedSystem
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = unwrapInboundIn(data)
        bytesReceived.wrappingAdd(UInt64(buffer.readableBytes), ordering: .relaxed)
        context.fireChannelRead(data)
    }

    func channelInactive(context: ChannelHandlerContext) {
        context.fireChannelInactive()
        distributedSystem.incrementStats(stats)
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let buffer = unwrapOutboundIn(data)
        bytesSent.wrappingAdd(UInt64(buffer.readableBytes), ordering: .relaxed)
        context.write(wrapOutboundOut(buffer), promise: promise)
    }
}
