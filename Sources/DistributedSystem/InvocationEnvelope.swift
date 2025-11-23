// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import Distributed
import Instrumentation
import NIOCore
import ServiceContextModule

#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#endif

extension ULEB128 {
    static func encode<T: UnsignedInteger>(_ value: T, to buffer: inout ByteBuffer) {
        buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) {
            ULEB128.encode(value, to: $0.baseAddress!)
        }
    }

    static func decode<T: UnsignedInteger>(_ type: T.Type = T.self, from buffer: inout ByteBuffer) throws -> T {
        try buffer.readWithUnsafeReadableBytes {
            try ULEB128.decode($0, as: T.self)
        }
    }
}

public struct InvocationEnvelope: Sendable {
    public let size: UInt32
    public let callID: UInt64
    public let serviceContext: [String: String]
    public let targetFunc: String
    public let genericSubstitutions: [Any.Type]
    public let arguments: ByteBuffer

    public init(
        _ size: UInt32,
        _ callID: UInt64,
        _ serviceContext: [String: String],
        _ targetFunc: String,
        _ genericSubstitutions: [Any.Type],
        _ arguments: ByteBuffer
    ) {
        self.size = size
        self.callID = callID
        self.serviceContext = serviceContext
        self.targetFunc = targetFunc
        self.genericSubstitutions = genericSubstitutions
        self.arguments = arguments
    }

    public init(from buffer: inout ByteBuffer, _ size: UInt32, _ targetFuncs: inout [String]) throws {
        self.size = size
        self.callID = try ULEB128.decode(from: &buffer)

        var serviceContext = [String: String]()
        while true {
            let keySize = try ULEB128.decode(UInt.self, from: &buffer)
            if keySize == 0 {
                break
            }
            guard let key = buffer.readString(length: Int(keySize)) else {
                throw DistributedSystemErrors.error("Failed to decode InvocationEnvelope (service context key)")
            }
            let valueSize = try ULEB128.decode(UInt.self, from: &buffer)
            guard let value = buffer.readString(length: Int(valueSize)) else {
                throw DistributedSystemErrors.error("Failed to decode InvocationEnvelope (service context value)")
            }
            serviceContext[key] = value
        }
        self.serviceContext = serviceContext

        var genericSubstitutions = [Any.Type]()
        while true {
            let typeNameSize = try buffer.readWithUnsafeReadableBytes { ptr in try ULEB128.decode(ptr, as: UInt.self) }
            if typeNameSize == 0 {
                break
            }
            guard let typeName = buffer.readString(length: Int(typeNameSize)) else {
                throw DistributedSystemErrors.error("Failed to decode InvocationEnvelope (type name)")
            }
            guard let type = _typeByName(typeName) else {
                throw DistributedSystemErrors.error("Failed to decode InvocationEnvelope: unknown type \(typeName)")
            }
            genericSubstitutions.append(type)
        }
        self.genericSubstitutions = genericSubstitutions

        let argumentsSize = try buffer.readWithUnsafeReadableBytes { ptr in try ULEB128.decode(ptr, as: UInt.self) }
        guard let arguments = buffer.readSlice(length: Int(argumentsSize)) else {
            throw DistributedSystemErrors.error("Failed to decode InvocationEnvelope: arguments")
        }
        self.arguments = arguments

        guard let targetType = buffer.readInteger(as: UInt8.self) else {
            throw DistributedSystemErrors.error("Failed to decode InvocationEnvelope: target type")
        }
        if targetType == 0 {
            let targetFuncSize = try buffer.readWithUnsafeReadableBytes { ptr in try ULEB128.decode(ptr, as: UInt.self) }
            guard let targetFunc = buffer.readString(length: Int(targetFuncSize)) else {
                throw DistributedSystemErrors.error("Failed to decode InvocationEnvelope (target func)")
            }
            self.targetFunc = targetFunc
            targetFuncs.append(targetFunc)
        } else {
            let funcId = try buffer.readWithUnsafeReadableBytes { ptr in try ULEB128.decode(ptr, as: UInt32.self) }
            self.targetFunc = targetFuncs[Int(funcId)]
        }
    }

    public static func wireSize(
        _ callID: UInt64,
        _ serviceContext: ServiceContext?,
        _ genericSubstitutions: [String],
        _ arguments: ByteBuffer,
        _ targetFunc: RemoteCallTarget
    ) -> Int {
        var wireSize = 0
        wireSize += ULEB128.size(callID)

        if let serviceContext {
            struct ServiceContextSizeCalculatorInjector: Injector {
                typealias Carrier = Int

                func inject(_ value: String, forKey key: String, into carrier: inout Carrier) {
                    key.withCString {
                        let count = strlen($0)
                        carrier += ULEB128.size(UInt(count))
                        carrier += count
                    }
                    value.withCString {
                        let count = strlen($0)
                        carrier += ULEB128.size(UInt(count))
                        carrier += count
                    }
                }
            }

            InstrumentationSystem.instrument.inject(
                serviceContext,
                into: &wireSize,
                using: ServiceContextSizeCalculatorInjector()
            )
        }
        wireSize += ULEB128.size(UInt(0))

        for typeName in genericSubstitutions {
            typeName.utf8.withContiguousStorageIfAvailable {
                wireSize += ULEB128.size(UInt($0.count))
                wireSize += $0.count
            }
        }
        wireSize += ULEB128.size(UInt(0))
        wireSize += ULEB128.size(UInt(arguments.readableBytes)) + arguments.readableBytes

        // The target funcion name first time will be sent as a string,
        // and then each next time as integer which is an index in the function names table.
        // When we encode envelope we do not know is it first time or not,
        // so just reserve space large enough for any of them.
        wireSize += MemoryLayout<UInt8>.size // target type
        let stringTargetSize = targetFunc.identifier.withCString {
            let count = strlen($0)
            return ULEB128.size(UInt(count)) + count
        }
        let idxTargetSize = ULEB128.size(UInt32.max)
        wireSize += max(stringTargetSize, idxTargetSize)
        return wireSize
    }

    public static func encode(
        _ callID: UInt64,
        _ serviceContext: ServiceContext?,
        _ genericSubstitutions: [String],
        _ arguments: inout ByteBuffer,
        to buffer: inout ByteBuffer
    ) -> Int {
        ULEB128.encode(callID, to: &buffer)

        if let serviceContext {
            struct ServiceContextInjector: Injector {
                typealias Carrier = ByteBuffer

                func inject(_ value: String, forKey key: String, into carrier: inout Carrier) {
                    key.withCString {
                        let count = strlen($0)
                        ULEB128.encode(UInt(count), to: &carrier)
                        carrier.writeBytes(UnsafeRawBufferPointer(start: $0, count: count))
                    }

                    value.withCString {
                        let count = strlen($0)
                        ULEB128.encode(UInt(count), to: &carrier)
                        carrier.writeBytes(UnsafeRawBufferPointer(start: $0, count: count))
                    }
                }
            }

            InstrumentationSystem.instrument.inject(
                serviceContext,
                into: &buffer,
                using: ServiceContextInjector()
            )
        }
        ULEB128.encode(UInt(0), to: &buffer)

        for typeName in genericSubstitutions {
            ULEB128.encode(UInt(typeName.count), to: &buffer)
            buffer.writeString(typeName)
        }
        ULEB128.encode(UInt(0), to: &buffer)

        ULEB128.encode(UInt(arguments.readableBytes), to: &buffer)
        buffer.writeBuffer(&arguments)

        return buffer.writerIndex // offset where target should be encoded
    }

    public static func setTargetId(_ name: String, in buffer: inout ByteBuffer, at offs: Int) {
        buffer.moveWriterIndex(to: offs)
        buffer.writeInteger(UInt8(0)) // target type = string
        name.withCString {
            let count = strlen($0)
            ULEB128.encode(UInt(count), to: &buffer)
            buffer.writeBytes(UnsafeRawBufferPointer(start: $0, count: count))
        }
    }

    public static func setTargetId(_ id: UInt32, in buffer: inout ByteBuffer, at offs: Int) {
        buffer.moveWriterIndex(to: offs)
        buffer.writeInteger(UInt8(1)) // target type = index
        ULEB128.encode(id, to: &buffer)
    }
}
