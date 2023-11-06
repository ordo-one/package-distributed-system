// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import PackageConcurrencyHelpers
import DistributedSystemConformance
internal import NIOCore

class SyncCallManager {
    private var lock = Lock()
    private var nextID: UInt64 = 1
    private var continuations: [UInt64: CheckedContinuation<any DistributedSystem.SerializationRequirement, Error>] = [:]

    func addCall<T: DistributedSystem.SerializationRequirement>(_ body: (UInt64) throws -> Void) async throws -> T {
        let result = try await withCheckedThrowingContinuation { continuation in
            let callID = self.lock.withLock {
                let callID = self.nextID
                self.continuations[callID] = continuation
                self.nextID += 1
                return callID
            }

            do {
                try body(callID)
            } catch {
                lock.withLockVoid {
                    continuations.removeValue(forKey: callID)
                }
                continuation.resume(throwing: error)
            }
        }

        if let result = result as? T {
            return result
        } else {
            throw DistributedSystemErrors.error("Invalid result type")
        }
    }

    func handleResult(_ buffer: inout ByteBuffer) throws {
        let (callID, typeHint, result) = try RemoteCallResultHandler.decode(from: &buffer)
        let continuation = lock.withLock {
            continuations.removeValue(forKey: callID)
        }

        guard let continuation else {
            throw DistributedSystemErrors.error("\(callID) not found")
        }

        if let type = _typeByName(typeHint) {
            if let type = type as? DistributedSystem.SerializationRequirement.Type {
                try result.withUnsafeReadableBytes { bytes in
                    let value: Any = try type.init(fromSerializedBuffer: bytes)
                    if let value = value as? (any DistributedSystem.SerializationRequirement) {
                        continuation.resume(returning: value)
                    } else {
                        continuation.resume(throwing: DistributedSystemErrors.error("result with invalid type '\(typeHint)'"))
                    }
                }
                return
            }
        }
        continuation.resume(throwing: DistributedSystemErrors.error("result with invalid type '\(typeHint)'"))
    }
}
