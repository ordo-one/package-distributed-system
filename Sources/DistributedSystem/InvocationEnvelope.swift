// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import Distributed
import NIOCore

public struct InvocationEnvelope {
    public enum TargetFunc {
        case name(String)
        case id(UInt32)
    }

    public let callID: UInt64
    public let targetFunc: String
    public let genericSubstitutions: [Any.Type]
    public let arguments: ByteBuffer

    public var size: UInt64 {
        UInt64(MemoryLayout<Self>.size + targetFunc.count + arguments.readableBytes)
    }

    public init(_ callID: UInt64, _ targetFunc: String, _ genericSubstitutions: [Any.Type], _ arguments: ByteBuffer) {
        self.callID = callID
        self.targetFunc = targetFunc
        self.genericSubstitutions = genericSubstitutions
        self.arguments = arguments
    }

    public init(from buffer: inout ByteBuffer, _ targetFuncs: inout [String]) throws {
        callID = try buffer.readWithUnsafeReadableBytes { ptr in try ULEB128.decode(ptr, as: UInt64.self) }

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
        _ genericSubstitutions: [String],
        _ arguments: ByteBuffer,
        _ targetFunc: TargetFunc
    ) -> Int {
        var wireSize = 0
        wireSize += ULEB128.size(callID)
        for typeName in genericSubstitutions {
            wireSize += ULEB128.size(UInt(typeName.count))
            wireSize += typeName.count
        }
        wireSize += MemoryLayout<UInt8>.size
        wireSize += ULEB128.size(UInt(arguments.readableBytes)) + arguments.readableBytes
        wireSize += MemoryLayout<UInt8>.size // target type
        let targetFuncWireSize = switch targetFunc {
        case let .name(name): ULEB128.size(UInt(name.count)) + name.count
        case let .id(id): ULEB128.size(id)
        }
        wireSize += targetFuncWireSize
        return wireSize
    }

    public static func encode(
        _ callID: UInt64,
        _ genericSubstitutions: [String],
        _ arguments: inout ByteBuffer,
        _ targetFunc: TargetFunc,
        to buffer: inout ByteBuffer
    ) {
        buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in ULEB128.encode(callID, to: ptr.baseAddress!) }

        for typeName in genericSubstitutions {
            buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in ULEB128.encode(UInt(typeName.count), to: ptr.baseAddress!) }
            buffer.writeString(typeName)
        }
        buffer.writeInteger(UInt8(0)) // generic substitution end indicator

        buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in ULEB128.encode(UInt(arguments.readableBytes), to: ptr.baseAddress!) }
        buffer.writeBuffer(&arguments)

        switch targetFunc {
        case let .name(name):
            buffer.writeInteger(UInt8(0)) // target type = string
            buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in ULEB128.encode(UInt(name.count), to: ptr.baseAddress!) }
            buffer.writeString(name)
        case let .id(id):
            buffer.writeInteger(UInt8(1)) // target type = index
            buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in ULEB128.encode(id, to: ptr.baseAddress!) }
        }
    }
}
