// Copyright 2024 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

/// Defines protocol used to serialize distributed function call arguments or result type
/// **Important:** Protocol requirements is subject to change, API compatibility is not guaranteed at this moment of time
/// **Note:** In future we plan to extend this protocol and provide an argument - a buffer for serialization of the object in place
public protocol Serializable {
    /// Calls a closure and pass the raw buffer pointer to serialized object, which is guaranteed to be valid during the closure call lifetime
    /// - Parameter body: A closure with an `UnsafeMutableBufferPointer` parameter that points to memory where the object is serialized
    func withUnsafeBytesSerialization<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result
}

/// Defines protocol used to deserialize distributed function call arguments or result type
/// **Important:** Protocol requirements is subject to change, API compatibility is not guaranteed at this moment of time
public protocol Deserializable {
    /// Creates a new instance by decoding from the given buffer.
    /// - Parameter from: the buffer to read data from
    init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws

    func _releaseBuffer()
}

/// Combine Serializable & Deserializable protocols
public protocol Transferable: Serializable & Deserializable {}

/// If a type implements TriviallyCopyable protocol then collection types (like an Array or Dictionary)
/// just serialize a binary data representation of the collection instead of serializing each item.
public protocol TriviallyCopyable {}
