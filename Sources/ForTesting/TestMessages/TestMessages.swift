
// swiftlint:disable all

import DistributedSystem
import Foundation

public typealias RequestIdentifier = UInt64
public typealias ClientIdentifier = UInt64
public typealias StreamIdentifier = UInt64
public typealias MonsterIdentifier = UInt64

public struct Timestamp: Codable, Transferable, CustomStringConvertible, @unchecked Sendable {
    public func withUnsafeBytesSerialization<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        let data: Data
        do {
            data = try JSONEncoder().encode(self)
        } catch {
            fatalError("Failed to encode Timestamp: \(error)")
        }

        return try data.withUnsafeBytes { ptr in
            try body(ptr)
        }
    }

    public init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws {
        let data = Data(buffer)
        self = try JSONDecoder().decode(Timestamp.self, from: data)
    }

    public func _releaseBuffer() {}

    public var seconds: UInt64
    public var attoseconds: UInt64

    public init(seconds: UInt64, attoseconds: UInt64) {
        self.seconds = seconds
        self.attoseconds = attoseconds
    }

    public var description: String {
        let time = TimeInterval(Double(seconds) + Double(attoseconds) / 1_000_000_000_000_000_000)
        let date = Date(timeIntervalSince1970: time)
        return ISO8601DateFormatter().string(from: date)
    }
}

public struct OpenRequest: Codable, Transferable, Identifiable, CustomStringConvertible, @unchecked Sendable {
    public func withUnsafeBytesSerialization<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        let data: Data
        do {
            data = try JSONEncoder().encode(self)
        } catch {
            fatalError("Failed to encode \(Self.self): \(error)")
        }

        return try data.withUnsafeBytes { ptr in
            try body(ptr)
        }
    }

    public init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws {
        let data = Data(buffer)
        self = try JSONDecoder().decode(OpenRequest.self, from: data)
    }

    public func _releaseBuffer() {}

    public var requestIdentifier: RequestIdentifier
    public var clientIdentifier: ClientIdentifier?

    public init(requestIdentifier: RequestIdentifier, clientIdentifier: ClientIdentifier? = nil) {
        self.requestIdentifier = requestIdentifier
        self.clientIdentifier = clientIdentifier
    }

    public typealias ID = RequestIdentifier
    public var id: RequestIdentifier { requestIdentifier }

    public var description: String {
        """
        OpenRequest {
            requestIdentifier = \(requestIdentifier)
        \(clientIdentifier.map { "    clientIdentifier = \($0)" } ?? "")
        }
        """
    }
}

public struct SnapshotDone: Codable, Transferable, Identifiable, CustomStringConvertible, @unchecked Sendable {
    public func withUnsafeBytesSerialization<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        let data: Data
        do {
            data = try JSONEncoder().encode(self)
        } catch {
            fatalError("Failed to encode SnapshotDone: \(error)")
        }

        return try data.withUnsafeBytes { ptr in
            try body(ptr)
        }
    }

    public init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws {
        let data = Data(buffer)
        self = try JSONDecoder().decode(SnapshotDone.self, from: data)
    }

    public func _releaseBuffer() {}

    public var streamIdentifier: StreamIdentifier

    public init(streamIdentifier: StreamIdentifier) {
        self.streamIdentifier = streamIdentifier
    }

    public typealias ID = StreamIdentifier
    public var id: StreamIdentifier { streamIdentifier }

    public var description: String {
        "SnapshotDone { streamIdentifier = \(streamIdentifier) }"
    }
}

public struct Stream: Codable, Transferable, Identifiable, CustomStringConvertible, @unchecked Sendable {
    public func withUnsafeBytesSerialization<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        let data: Data
        do {
            data = try JSONEncoder().encode(self)
        } catch {
            fatalError("Failed to encode Stream: \(error)")
        }

        return try data.withUnsafeBytes { ptr in
            try body(ptr)
        }
    }

    public init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws {
        let data = Data(buffer)
        self = try JSONDecoder().decode(Stream.self, from: data)
    }

    public func _releaseBuffer() {}

    public var streamIdentifier: StreamIdentifier

    public init(streamIdentifier: StreamIdentifier) {
        self.streamIdentifier = streamIdentifier
    }

    public typealias ID = StreamIdentifier
    public var id: StreamIdentifier { streamIdentifier }

    public var description: String {
        """
        Stream {
            streamIdentifier = \(streamIdentifier)
        }
        """
    }
}

public struct StreamOpened: Codable, Transferable, Identifiable, CustomStringConvertible, @unchecked Sendable {
    public func withUnsafeBytesSerialization<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        let data: Data
        do {
            data = try JSONEncoder().encode(self)
        } catch {
            fatalError("Failed to encode StreamOpened: \(error)")
        }

        return try data.withUnsafeBytes { ptr in
            try body(ptr)
        }
    }

    public init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws {
        let data = Data(buffer)
        self = try JSONDecoder().decode(StreamOpened.self, from: data)
    }

    public func _releaseBuffer() {}

    public var requestIdentifier: RequestIdentifier
    public var streamIdentifier: StreamIdentifier?

    public init(requestIdentifier: RequestIdentifier, streamIdentifier: StreamIdentifier? = nil) {
        self.requestIdentifier = requestIdentifier
        self.streamIdentifier = streamIdentifier
    }

    public typealias ID = RequestIdentifier
    public var id: RequestIdentifier { requestIdentifier }

    public var description: String {
        """
        StreamOpened {
            requestIdentifier = \(requestIdentifier)
        \(streamIdentifier.map { "    streamIdentifier = \($0)" } ?? "")
        }
        """
    }
}

public struct Vec3: Codable, Transferable, CustomStringConvertible, @unchecked Sendable {
    public func withUnsafeBytesSerialization<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        let data: Data
        do {
            data = try JSONEncoder().encode(self)
        } catch {
            fatalError("Failed to encode SnapshotDone: \(error)")
        }

        return try data.withUnsafeBytes { ptr in
            try body(ptr)
        }
    }

    public init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws {
        let data = Data(buffer)
        self = try JSONDecoder().decode(Vec3.self, from: data)
    }

    public func _releaseBuffer() {}

    public var x: Float
    public var y: Float
    public var z: Float

    public init(x: Float, y: Float, z: Float) {
        self.x = x
        self.y = y
        self.z = z
    }

    public var description: String {
        "Vec3(x: \(x), y: \(y), z: \(z))"
    }
}

// MARK: - Color

public enum Color: String, Codable, Transferable, @unchecked Sendable {
    case red, green, blue

    public func withUnsafeBytesSerialization<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        let data: Data
        do {
            data = try JSONEncoder().encode(self)
        } catch {
            fatalError("Failed to encode SnapshotDone: \(error)")
        }

        return try data.withUnsafeBytes { ptr in
            try body(ptr)
        }
    }

    public init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws {
        let data = Data(buffer)
        self = try JSONDecoder().decode(Color.self, from: data)
    }

    public func _releaseBuffer() {}
}

public struct Monster: Codable, Transferable, Identifiable, CustomStringConvertible, @unchecked Sendable {
    public func withUnsafeBytesSerialization<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        let data: Data
        do {
            data = try JSONEncoder().encode(self)
        } catch {
            // force-crash to satisfy rethrows requirement
            fatalError("Failed to encode Monster to JSON: \(error)")
        }

        return try data.withUnsafeBytes { ptr in
            try body(ptr)
        }
    }

    public init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws {
        let data = Data(buffer)
        self = try JSONDecoder().decode(Monster.self, from: data)
    }

    public func _releaseBuffer() {}

    public var identifier: MonsterIdentifier
    public var name: String?
    public var pos: Vec3?
    public var mana: UInt16?
    public var hp: UInt16?
    public var color: Color?
    public var inventory: [UInt8]?

    public init(identifier: MonsterIdentifier, name: String? = nil, pos: Vec3? = nil, mana: UInt16? = nil, hp: UInt16? = nil, color: Color? = nil, inventory: [UInt8]? = nil) {
        self.identifier = identifier
        self.name = name
        self.pos = pos
        self.mana = mana
        self.hp = hp
        self.color = color
        self.inventory = inventory
    }

    public typealias ID = MonsterIdentifier
    public var id: MonsterIdentifier { identifier }

    public var description: String {
        """
        Monster {
            identifier = \(identifier)
        \(name.map { "    name = \($0)\n" } ?? "")
        \(pos.map { "    pos = \($0)\n" } ?? "")
        \(mana.map { "    mana = \($0)\n" } ?? "")
        \(hp.map { "    hp = \($0)\n" } ?? "")
        \(color.map { "    color = \($0)\n" } ?? "")
        \(inventory.map { "    inventory = \($0)" } ?? "")
        }
        """
    }
}
