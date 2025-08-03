// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import Distributed
import Logging
import NIOCore

public final class RemoteCallResultHandler: DistributedTargetInvocationResultHandler {
    public typealias SerializationRequirement = Transferable

    private let loggerBox: Box<Logger>
    private let targetFunc: String
    private var buffer: ByteBuffer?

    var hasResult: Bool { buffer != nil }

    init(_ loggerBox: Box<Logger>, _ targetFunc: String) {
        self.loggerBox = loggerBox
        self.targetFunc = targetFunc
    }

    func sendTo(_ channel: Channel, for callID: UInt64) throws {
        if var buffer {
            if callID != 0 {
                let offs = MemoryLayout<UInt32>.size + MemoryLayout<DistributedSystem.SessionMessage.RawValue>.size
                buffer.setInteger(callID, at: offs)
                _ = channel.writeAndFlush(buffer)
            } else {
                throw DistributedSystemErrors.error("""
                    Internal error: unexpected result for void call (callID=0, buffer not empty)
                    """
                )
            }
            self.buffer = nil
        } else {
            throw DistributedSystemErrors.error("""
                Internal error: unexpected result for void call (callID=\(callID), buffer empty)
                """
            )
        }
    }

    private static func encodeResult(_ result: some SerializationRequirement, isError: Bool) -> ByteBuffer {
        result.withUnsafeBytesSerialization { bytes in
            let resultType = type(of: result as Any)
            let valueTypeHint: String = _mangledTypeName(resultType) ?? _typeName(resultType)
            let payloadSize =
                MemoryLayout<DistributedSystem.SessionMessage.RawValue>.size
                + MemoryLayout<UInt64>.size // callID
                + MemoryLayout<UInt8>.size  // isError
                + ULEB128.size(UInt(valueTypeHint.count)) + valueTypeHint.count
                + bytes.count
            var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt32>.size + payloadSize)
            buffer.writeInteger(UInt32(payloadSize))
            buffer.writeInteger(DistributedSystem.SessionMessage.invocationResult.rawValue)
            buffer.writeInteger(UInt64(0))
            buffer.writeInteger(UInt8(isError ? 1 : 0))
            buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) {
                ptr in ULEB128.encode(UInt(valueTypeHint.count), to: ptr.baseAddress!)
            }
            buffer.writeString(valueTypeHint)
            buffer.writeBytes(bytes)
            return buffer
        }
    }

    public func onReturn(value: some SerializationRequirement) async throws {
        self.buffer = Self.encodeResult(value, isError: false)
    }

    public func onReturnVoid() async throws {
        // logger.debug("onReturnVoid")
    }

    struct TransferableError: Error & Transferable {
        private let description: String

        init(_ errorDescription: String) {
            self.description = errorDescription
        }

        func withUnsafeBytesSerialization<Result>(
            _ body: (UnsafeRawBufferPointer) throws -> Result
        ) rethrows -> Result {
            let utf8String = description.utf8CString
            return try utf8String.withUnsafeBufferPointer {
                try body(UnsafeRawBufferPointer(start: $0.baseAddress, count: $0.count))
            }
        }

        init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws {
            self.description = buffer.withMemoryRebound(to: CChar.self) {
                String(cString: $0.baseAddress!)
            }
        }

        func _releaseBuffer() {
        }
    }

    public func onThrow(error: some Error) async throws {
        if let error = error as? SerializationRequirement {
            self.buffer = Self.encodeResult(error, isError: true)
        } else {
            let errorDescription = """
                Error from distributed actor function '\(targetFunc)' cannot be transmitted \
                back to the caller: error type '\(type(of: (error as Any)))' is not Transferable
                """
            loggerBox.value.error("\(errorDescription)")
            self.buffer = Self.encodeResult(TransferableError(errorDescription), isError: true)
        }
    }
}
