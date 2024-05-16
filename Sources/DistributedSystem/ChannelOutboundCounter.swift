// Copyright 2024 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import Atomics
internal import NIOCore

final class ChannelOutboundCounter: ChannelOutboundHandler {
    typealias OutboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private var distributedSystem: DistributedSystem
    var _bytesSent = ManagedAtomic<UInt64>(0)
    var bytesSent: UInt64 { _bytesSent.load(ordering: .relaxed) }

    init(_ distributedSystem: DistributedSystem) {
        self.distributedSystem = distributedSystem
    }

    deinit {
        let bytesSent = _bytesSent.load(ordering: .relaxed)
        distributedSystem._bytesSent.wrappingIncrement(by: bytesSent, ordering: .relaxed)
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let buffer = unwrapOutboundIn(data)
        _bytesSent.wrappingIncrement(by: UInt64(buffer.readableBytes), ordering: .relaxed)
        context.write(wrapOutboundOut(buffer), promise: promise)
    }
}
