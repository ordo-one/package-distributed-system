// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

public extension Optional where Wrapped: Serializable {
    func withUnsafeBytesSerialization<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        switch self {
        case .none:
            return try withUnsafeTemporaryAllocation(byteCount: 1, alignment: 1) { buffer in
                buffer[0] = 0
                return try body(.init(buffer))
            }
        case .some(let wrapped):
            return try wrapped.withUnsafeBytesSerialization { bytes in
                try withUnsafeTemporaryAllocation(byteCount: 1 + bytes.count, alignment: 1) { buffer in
                    buffer[0] = 1
                    if bytes.count > 0 {
                        buffer.baseAddress!.advanced(by: 1).copyMemory(from: bytes.baseAddress!, byteCount: bytes.count)
                    }
                    return try body(.init(buffer))
                }
            }
        }
    }
}

public extension Optional where Wrapped: Deserializable {
    init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws {
        if buffer.count == 0 {
            throw DistributedSystemErrors.decodeError(
                description: "Invalid serialized data for Optional (expected greater than 0)")
        }
        if buffer[0] == 0 {
            if buffer.count != 1 {
                throw DistributedSystemErrors.decodeError(
                    description: "Invalid serialized data for Optional (expected 1 byte for .none instead of \(buffer.count))")
            }
            self = .none
        } else {
            let firstByte = buffer.baseAddress!.advanced(by: 1)
            self = .some(try Wrapped(fromSerializedBuffer: UnsafeRawBufferPointer(start: firstByte, count: buffer.count - 1)))
        }
    }

    func _releaseBuffer() {
        if case let .some(value) = self {
            value._releaseBuffer()
        }
    }
}

extension Optional: Serializable where Wrapped: Serializable {}
extension Optional: Deserializable where Wrapped: Deserializable {}
extension Optional: Transferable where Wrapped: Transferable {}
