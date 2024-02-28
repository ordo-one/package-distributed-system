// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

public extension String {
    func withUnsafeBytesSerialization<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        try utf8CString.withUnsafeBufferPointer {
            try body(.init(start: .init($0.baseAddress), count: $0.count - 1))
        }
    }

    init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws {
        self = String(unsafeUninitializedCapacity: buffer.count) {
            _ = $0.initialize(from: buffer)
            return buffer.count
        }
    }

    func _releaseBuffer() {}
}

extension String: Transferable {}
