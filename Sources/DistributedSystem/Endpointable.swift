// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import Distributed

public enum ConnectionState: UInt16, Sendable {
    case active
    case stale
    case closed
}

extension ConnectionState: Transferable {
    public func withUnsafeBytesSerialization<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        var state = self.rawValue
        return try withUnsafeBytes(of: &state, body)
    }

    public init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws {
        if buffer.count != MemoryLayout<Self.RawValue>.size {
            throw DecodeError.invalidBufferSize(buffer.count, MemoryLayout<Self.RawValue>.size)
        }
        let rawValue = buffer.loadUnaligned(fromByteOffset: 0, as: Self.RawValue.self)
        if let value = Self(rawValue: rawValue) {
            self = value
        } else {
            throw DecodeError.error("Invalid value '\(rawValue)' for '\(Self.self)'")
        }
    }

    public func _releaseBuffer() {
    }
}

public protocol ConnectionStateHandler: DistributedActor {
    distributed func handleConnectionState(_ state: ConnectionState) async throws
}

/// Defines service side endpoint distributed actor
public protocol ServiceEndpoint: ConnectionStateHandler {
    static var serviceName: String { get }
}

/// Defines client side endpoint distributed actor
public protocol ClientEndpoint: ConnectionStateHandler {
}
