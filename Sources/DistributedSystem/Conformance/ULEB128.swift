// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

// LEB128 coder for unsigned integers

public struct ULEB128 {
    /// Return the number of bytes used to encode value.
    /// - Parameter value: the value to calculate the size for
    public static func size<T: UnsignedInteger>(_ value: T) -> Int {
        var value = value
        var ret = 0
        repeat {
            ret += 1
            value >>= 7
        }
        while value != 0
        return ret
    }

    /// Encode value to the destination buffer.
    /// The caller responsible to provide the buffer of a size enough to store the value
    /// - Parameters
    ///     - value: the value to encode
    ///     - dst: destination buffer to encode to
    /// - Returns: size in bytes of the encoded value
    public static func encode<T: UnsignedInteger>(_ value: T, to dst: UnsafeMutableRawPointer) -> Int {
        var value = value
        var ptr = dst.assumingMemoryBound(to: UInt8.self)
        var count = 1
        while true {
            let byte = UInt8(value & 0x7F)
            value >>= 7
            if value == 0 {
                ptr.pointee = byte
                break
            }
            ptr.pointee = (byte | 0x80)
            ptr += 1
            count += 1
        }
        return count
    }

    /// Decode value from the buffer
    /// - Parameter src: source buffer to decode value from
    /// - Returns: a tuple with a size and decoded value
    public static func decode<T: UnsignedInteger>(_ src: UnsafeRawBufferPointer, as: T.Type) throws -> (Int, T) {
        let ptr = src.assumingMemoryBound(to: UInt8.self)
        var value: T = 0
        var idx = 0
        var shift = 0
        while true {
            let byte = ptr[idx]
            value |= (T(byte & 0x7F) << shift)
            if (byte & 0x80) == 0 {
                break
            }
            idx += 1
            shift += 7
            if shift > value.bitWidth {
                throw DecodeError.invalidData
            }
        }
        return (idx + 1, value)
    }
}
