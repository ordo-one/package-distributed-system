// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import Helpers

fileprivate typealias SizeType = UInt32

public extension Array where Element: Serializable {
    func withUnsafeBytesSerialization<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        if Element.self is any TriviallyCopyable.Type {
            return try withUnsafeBytes(body)
        } else {
            var buffer = UnsafeMutableRawBufferPointer(start: nil, count: 0)
            defer {
                if buffer.baseAddress != nil {
                    buffer.deallocate()
                }
            }
            var pos = 0
            for element in self {
                element.withUnsafeBytesSerialization { bytes in
                    let space = (buffer.count - pos)
                    let sizeSize = ULEB128.size(SizeType(bytes.count))
                    let elementSize = sizeSize + bytes.count
                    if space < elementSize {
                        if buffer.count == 0 {
                            let capacity = nearestPowerOf2(elementSize * count)
                            buffer = UnsafeMutableRawBufferPointer.allocate(byteCount: capacity, alignment: 0)
                        } else {
                            let capacity = nearestPowerOf2(buffer.count + elementSize)
                            let newBuffer = UnsafeMutableRawBufferPointer.allocate(byteCount: capacity, alignment: 0)
                            newBuffer.copyMemory(from: UnsafeRawBufferPointer(start: buffer.baseAddress, count: pos))
                            buffer.deallocate()
                            buffer = newBuffer
                        }
                    }
                    var dst = (buffer.baseAddress! + pos)
                    _ = ULEB128.encode(SizeType(bytes.count), to: dst)
                    dst += sizeSize
                    dst.copyMemory(from: bytes.baseAddress!, byteCount: bytes.count)
                    pos += elementSize
                }
            }
            return try body(UnsafeRawBufferPointer(start: buffer.baseAddress, count: pos))
        }
    }

    func withRetainedBytesSerialization<Result>(_ body: (Helpers.UnsafeRetainedRawBuffer) throws -> Result) rethrows -> Result {
        fatalError("Not implemented")
    }
}

public extension Array where Element: Deserializable {
    init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws {
        self.init()
        if Element.self is any TriviallyCopyable.Type {
            var count = (buffer.count / MemoryLayout<Element>.size)
            reserveCapacity(count)
            var ptr = buffer.baseAddress!
            while count > 0 {
                append(ptr.loadUnaligned(as: Element.self))
                ptr += MemoryLayout<Element>.size
                count -= 1
            }
        } else {
            var ptr = buffer
            var count = 0
            while ptr.count > 0 {
                let (sizeSize, elementSize) = try ULEB128.decode(ptr, as: SizeType.self)
                let size = (sizeSize + Int(elementSize))
                ptr = UnsafeRawBufferPointer(start: ptr.baseAddress! + size, count: ptr.count - size)
                count += 1
            }
            reserveCapacity(count)
            ptr = buffer
            while ptr.count > 0 {
                let (sizeSize, elementSize) = try ULEB128.decode(ptr, as: SizeType.self)
                let elementBuffer = UnsafeRawBufferPointer(start: ptr.baseAddress! + sizeSize, count: Int(elementSize))
                append(try Element(fromSerializedBuffer: elementBuffer))
                let size = (sizeSize + Int(elementSize))
                ptr = UnsafeRawBufferPointer(start: ptr.baseAddress! + size, count: ptr.count - size)
            }
        }
    }
}

extension Array: Serializable where Element: Serializable {}
extension Array: Deserializable where Element: Deserializable {}
extension Array: Transferable where Element: Transferable {}
