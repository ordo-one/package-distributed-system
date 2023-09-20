// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import Helpers

fileprivate typealias SizeType = UInt32

public extension Dictionary where Key: Serializable, Value: Serializable {
    func withUnsafeBytesSerialization<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        var buffer = UnsafeMutableRawBufferPointer(start: nil, count: 0)
        defer {
            buffer.deallocate()
        }
        var pos = 0
        for entry in self {
            entry.key.withUnsafeBytesSerialization { key in
                entry.value.withUnsafeBytesSerialization { value in
                    let space = (buffer.count - pos)
                    let keySizeSize = ULEB128.size(SizeType(key.count))
                    let valueSizeSize = ULEB128.size(SizeType(value.count))
                    let entrySize = keySizeSize + key.count + valueSizeSize + value.count
                    if space < entrySize {
                        if buffer.count == 0 {
                            let capacity = nearestPowerOf2(entrySize * count)
                            buffer = UnsafeMutableRawBufferPointer.allocate(byteCount: capacity, alignment: 0)
                        } else {
                            let capacity = nearestPowerOf2(buffer.count + entrySize)
                            let newBuffer = UnsafeMutableRawBufferPointer.allocate(byteCount: capacity, alignment: 0)
                            newBuffer.copyMemory(from: UnsafeRawBufferPointer(start: buffer.baseAddress, count: pos))
                            buffer.deallocate()
                            buffer = newBuffer
                        }
                    }
                    var dst = (buffer.baseAddress! + pos)
                    dst += ULEB128.encode(SizeType(key.count), to: dst)
                    dst.copyMemory(from: key.baseAddress!, byteCount: key.count)
                    dst += key.count
                    dst += ULEB128.encode(SizeType(value.count), to: dst)
                    dst.copyMemory(from: value.baseAddress!, byteCount: value.count)
                    pos += entrySize
                }
            }
        }
        return try body(UnsafeRawBufferPointer(start: buffer.baseAddress, count: pos))
    }

    func withRetainedBytesSerialization<Result>(_ body: (Helpers.UnsafeRetainedRawBuffer) throws -> Result) rethrows -> Result {
        fatalError("Not implemented")
    }
}

public extension Dictionary where Key: Deserializable, Value: Deserializable {
    private func decode<T: Deserializable>(from buffer: UnsafeRawBufferPointer, as: T.Type = T.self) throws -> (Int, T) {
        let (sizeSize, size) = try ULEB128.decode(buffer, as: SizeType.self)
        let ptr = UnsafeRawBufferPointer(start: buffer.baseAddress! + sizeSize, count: Int(size))
        return (sizeSize + Int(size), try T(fromSerializedBuffer: ptr))
    }

    init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws {
        self.init()
        var ptr = buffer
        while ptr.count > 0 {
            let (keySize, key) = try decode(from: ptr, as: Key.self)
            ptr = UnsafeRawBufferPointer(start: ptr.baseAddress! + keySize, count: ptr.count - keySize)
            let (valueSize, value) = try decode(from: ptr, as: Value.self)
            ptr = UnsafeRawBufferPointer(start: ptr.baseAddress! + valueSize, count: ptr.count - valueSize)
            self[key] = value
        }
    }
}

extension Dictionary: Serializable where Key: Serializable, Value: Serializable {}
extension Dictionary: Deserializable where Key: Deserializable, Value: Deserializable {}
extension Dictionary: Transferable where Key: Transferable, Value: Transferable {}
