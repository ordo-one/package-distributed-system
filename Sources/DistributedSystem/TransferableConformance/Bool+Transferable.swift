// Copyright 2024 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

public extension Bool {
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

extension Bool: Transferable {}
