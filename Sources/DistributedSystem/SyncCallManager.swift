// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import Atomics
import Logging
import NIOCore
internal import struct NIOConcurrencyHelpers.NIOLock

class SyncCallManager {
    private let loggerBox: Box<Logger>
    private var nextID = ManagedAtomic<UInt64>(0)
    private var lock = NIOLock()
    private var continuations = [UInt64: CheckedContinuation<any DistributedSystem.SerializationRequirement, Error>]()
    private var results = [UInt64: ByteBuffer?]()

    var nextCallID: UInt64 {
        while true {
            let nextID = nextID.wrappingIncrementThenLoad(ordering: .releasing)
            if nextID != 0 {
                return nextID
            }
        }
    }

    var logger: Logger {
        loggerBox.value
    }

    init(_ loggerBox: Box<Logger>) {
        self.loggerBox = loggerBox
    }

    private func resumeContinuation(
        _ continuation: CheckedContinuation<any DistributedSystem.SerializationRequirement, Error>,
        with result: inout ByteBuffer
    ) {
        guard let isError = result.readInteger(as: UInt8.self).map({ $0 != 0 }) else {
            continuation.resume(throwing: DistributedSystemErrors.error("Invalid result received"))
            return
        }

        let typeHintSize: UInt
        do {
            typeHintSize = try result.readWithUnsafeReadableBytes { try ULEB128.decode($0, as: UInt.self) }
        } catch {
            continuation.resume(throwing: DistributedSystemErrors.error("\(error)"))
            return
        }

        guard let typeHint = result.readString(length: Int(typeHintSize)) else {
            continuation.resume(throwing: DistributedSystemErrors.error("Invalid result received"))
            return
        }

        guard let type = _typeByName(typeHint) else {
            continuation.resume(throwing: DistributedSystemErrors.error("Unknown type '\(typeHint)'"))
            return
        }

        guard let type = type as? DistributedSystem.SerializationRequirement.Type else {
            continuation.resume(
                throwing: DistributedSystemErrors.error("""
                    \(isError ? "Error" : "Result") type '\(typeHint)' \
                    does not conform to DistributedSystem.SerializationRequirement
                    """
                )
            )
            return
        }

        result.withUnsafeReadableBytes { bytes in
            do {
                let value = try type.init(fromSerializedBuffer: bytes)
                if isError {
                    if let error = value as? Error {
                        continuation.resume(throwing: error)
                    } else {
                        continuation.resume(
                            throwing: DistributedSystemErrors.error("""
                                Decoded value of type '\(type)' does not conform to 'Error' to be thrown
                                """
                            )
                        )
                    }
                } else {
                    continuation.resume(returning: value)
                }
            } catch {
                continuation.resume(throwing: error)
            }
        }
    }

    func waitResult<T: DistributedSystem.SerializationRequirement>(_ callID: UInt64) async throws -> T {
        logger.trace("waiting result for \(callID)")
        let result = try await withCheckedThrowingContinuation { continuation in
            let result: Optional<ByteBuffer>? = self.lock.withLock {
                if let result = self.results.removeValue(forKey: callID) {
                    return result
                } else {
                    self.continuations[callID] = continuation
                    return nil
                }
            }

            if let result {
                if var result {
                    resumeContinuation(continuation, with: &result)
                } else {
                    continuation.resume(throwing: DistributedSystemErrors.connectionLost)
                }
            }
        }
        logger.trace("got result for \(callID)")

        if let result = result as? T {
            return result
        } else {
            throw DistributedSystemErrors.unexpectedResultType("\(type(of: result)) instead of \(T.self)")
        }
    }

    func handleResult(_ buffer: inout ByteBuffer) throws {
        let callID = buffer.readInteger(as: UInt64.self)
        guard let callID else {
            logger.warning("Invalid result received")
            return
        }

        let continuation: CheckedContinuation<any DistributedSystem.SerializationRequirement, Error>? = lock.withLock {
            let continuation = self.continuations.removeValue(forKey: callID)
            if continuation == nil {
                self.results[callID] = buffer
            }
            return continuation
        }

        if let continuation {
            resumeContinuation(continuation, with: &buffer)
        }
    }

    func resumeWithConnectionLoss(_ pendingSyncCalls: Set<UInt64>) {
        let continuations = lock.withLock {
            var continuations = [CheckedContinuation<any DistributedSystem.SerializationRequirement, Error>]()
            for callID in pendingSyncCalls {
                if let continuation = self.continuations[callID] {
                    continuations.append(continuation)
                } else {
                    self.results.updateValue(nil, forKey: callID)
                }
            }
            return continuations
        }

        for continuation in continuations {
            continuation.resume(throwing: DistributedSystemErrors.connectionLost)
        }
    }
}
