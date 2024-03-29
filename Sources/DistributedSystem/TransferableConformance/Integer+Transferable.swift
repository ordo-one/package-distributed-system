// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

public extension BinaryInteger {
    func withUnsafeBytesSerialization<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        try Swift.withUnsafeBytes(of: self) {
            try body(UnsafeRawBufferPointer($0))
        }
    }

    init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws {
        guard buffer.count == MemoryLayout<Self>.size else {
            throw DecodeError.invalidBufferSize(buffer.count, MemoryLayout<Self>.size)
        }
        self = buffer.loadUnaligned(as: Self.self)
    }

    func _releaseBuffer() {}
}

extension Int: Transferable & TriviallyCopyable {}
extension Int8: Transferable & TriviallyCopyable {}
extension Int16: Transferable & TriviallyCopyable {}
extension Int32: Transferable & TriviallyCopyable {}
extension Int64: Transferable & TriviallyCopyable {}
extension UInt: Transferable & TriviallyCopyable {}
extension UInt8: Transferable & TriviallyCopyable {}
extension UInt16: Transferable & TriviallyCopyable {}
extension UInt32: Transferable & TriviallyCopyable {}
extension UInt64: Transferable & TriviallyCopyable {}
