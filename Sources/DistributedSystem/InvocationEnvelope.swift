// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import Distributed
internal import NIOCore

struct InvocationEnvelope {
    private typealias TypeNameSizeType = UInt16

    let targetID: EndpointIdentifier
    let callID: UInt64
    let targetFunc: String
    let genericSubstitutions: [Any.Type]
    let arguments: ByteBuffer

    var size: UInt64 {
        UInt64(MemoryLayout<Self>.size + targetFunc.count + arguments.readableBytes)
    }

    private init(_ targetID: EndpointIdentifier, _ callID: UInt64, _ targetFunc: String, _ genericSubstitutions: [Any.Type], _ arguments: ByteBuffer) {
        self.targetID = targetID
        self.callID = callID
        self.targetFunc = targetFunc
        self.genericSubstitutions = genericSubstitutions
        self.arguments = arguments
    }

    init(from buffer: inout ByteBuffer) throws {
        targetID = try EndpointIdentifier(from: &buffer)
        callID = try buffer.readWithUnsafeReadableBytes { ptr in try ULEB128.decode(ptr, as: UInt64.self) }

        let targetFuncSize = try buffer.readWithUnsafeReadableBytes { ptr in try ULEB128.decode(ptr, as: TypeNameSizeType.self) }
        guard let targetFunc = buffer.readString(length: Int(targetFuncSize)) else {
            throw DistributedSystemErrors.error("Failed to decode InvocationEnvelope (target func)")
        }
        self.targetFunc = targetFunc

        var genericSubstitutions = [Any.Type]()
        while true {
            let typeNameSize = try buffer.readWithUnsafeReadableBytes { ptr in try ULEB128.decode(ptr, as: TypeNameSizeType.self) }
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
    }

    static func wireSize(_ targetID: EndpointIdentifier, _ callID: UInt64, _ targetFunc: RemoteCallTarget, _ genericSubstitutions: [String], _ arguments: ByteBuffer) -> Int {
        var wireSize = targetID.wireSize
        wireSize += ULEB128.size(callID)
        wireSize += ULEB128.size(TypeNameSizeType(targetFunc.identifier.count)) + targetFunc.identifier.count
        for typeName in genericSubstitutions {
            wireSize += ULEB128.size(TypeNameSizeType(typeName.count))
            wireSize += typeName.count
        }
        wireSize += MemoryLayout<UInt8>.size
        wireSize += ULEB128.size(UInt(arguments.readableBytes)) + arguments.readableBytes
        return wireSize
    }

    static func encode(_ targetID: EndpointIdentifier, _ callID: UInt64, _ targetFunc: RemoteCallTarget, _ genericSubstitutions: [String], _ arguments: inout ByteBuffer, to buffer: inout ByteBuffer) {
        targetID.encode(to: &buffer)

        buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in ULEB128.encode(callID, to: ptr.baseAddress!) }

        let targetFuncMangled = targetFunc.identifier
        buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in ULEB128.encode(TypeNameSizeType(targetFuncMangled.count), to: ptr.baseAddress!) }
        buffer.writeString(targetFuncMangled)

        for typeName in genericSubstitutions {
            buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in ULEB128.encode(TypeNameSizeType(typeName.count), to: ptr.baseAddress!) }
            buffer.writeString(typeName)
        }
        buffer.writeInteger(UInt8(0))

        buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in ULEB128.encode(UInt(arguments.readableBytes), to: ptr.baseAddress!) }
        buffer.writeBuffer(&arguments)
    }
}
