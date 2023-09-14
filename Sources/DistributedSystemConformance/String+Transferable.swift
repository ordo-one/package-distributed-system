// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import Helpers

public extension String {
    func withUnsafeBytesSerialization<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        try utf8CString.withUnsafeBufferPointer {
            try body(.init(start: .init($0.baseAddress), count: $0.count - 1))
        }
    }

    func withRetainedBytesSerialization<Result>(_: (UnsafeRetainedRawBuffer) throws -> Result) rethrows -> Result {
        fatalError("\(#file):\(#line): This is not supported, please review the source code")
    }

    init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws {
        self = String(unsafeUninitializedCapacity: buffer.count) {
            _ = $0.initialize(from: buffer)
            return buffer.count
        }
    }
}

extension String: Transferable {}
