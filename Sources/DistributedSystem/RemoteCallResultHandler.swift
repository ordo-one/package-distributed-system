// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import Distributed
internal import NIOCore

public final class RemoteCallResultHandler: DistributedTargetInvocationResultHandler {
    public typealias SerializationRequirement = Transferable

    private var buffer: ByteBuffer?

    var hasResult: Bool { buffer != nil }

    func sendTo(_ channel: Channel, for callID: UInt64) throws {
        if var buffer {
            if callID != 0 {
                let offs = MemoryLayout<UInt32>.size + MemoryLayout<DistributedSystem.SessionMessage.RawValue>.size
                buffer.setInteger(callID, at: offs)
                _ = channel.writeAndFlush(buffer)
            } else {
                throw DistributedSystemErrors.error("Internal error: unexpected result for void call (callID=0, buffer not empty)")
            }
            self.buffer = nil
        } else {
            throw DistributedSystemErrors.error("Internal error: unexpected result for void call (callID=\(callID), buffer empty)")
        }
    }

    public func onReturn(value: some SerializationRequirement) async throws {
        value.withUnsafeBytesSerialization { bytes in
            let valueType = type(of: value as Any)
            let valueTypeHint: String = _mangledTypeName(valueType) ?? _typeName(valueType)
            let payloadSize =
                MemoryLayout<DistributedSystem.SessionMessage.RawValue>.size +
                MemoryLayout<UInt64>.size +
                MemoryLayout<UInt16>.size + valueTypeHint.count +
                bytes.count
            var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt32>.size + payloadSize)
            buffer.writeInteger(UInt32(payloadSize))
            buffer.writeInteger(DistributedSystem.SessionMessage.invocationResult.rawValue)
            buffer.writeInteger(UInt64(0))
            buffer.writeInteger(UInt16(valueTypeHint.count))
            buffer.writeString(valueTypeHint)
            buffer.writeBytes(bytes)
            self.buffer = buffer
        }
    }

    public func onReturnVoid() async throws {
        // logger.debug("onReturnVoid")
    }

    public func onThrow(error: some Error) async throws {
        // TODO: if func throws we need to send some result, right?
        // logger.debug("onThrow: \(error)")
    }
}
