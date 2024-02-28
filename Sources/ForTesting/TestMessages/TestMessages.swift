// swiftlint:disable all

import DateTime
import DistributedSystem
import FlatBuffers

public extension Timestamp {
    var description: String {
        var time = EpochDateTime.unixEpoch()
        time.convert(timestamp: Int(seconds))

        let microseconds = attoseconds / 1_000_000_000_000

        var str = "\(time.year)-"
        if time.month < 10 { str += "0" }
        str += "\(time.month)-"
        if time.day < 10 { str += "0" }
        str += "\(time.day) "
        if time.hour < 10 { str += "0" }
        str += "\(time.hour):"
        if time.minute < 10 { str += "0" }
        str += "\(time.minute):"
        if time.second < 10 { str += "0" }
        str += "\(time.second)."

        if microseconds < 10 {
            str += "00000"
        } else if microseconds < 100 {
            str += "0000"
        } else if microseconds < 1_000 {
            str += "000"
        } else if microseconds < 10_000 {
            str += "00"
        } else if microseconds < 100_000 {
            str += "0"
        }
        str += "\(microseconds)"

        return str
    }
}

//
// Typealiases
//

public typealias RequestIdentifier = UInt64
public typealias ClientIdentifier = UInt64
public typealias StreamIdentifier = UInt64
public typealias MonsterIdentifier = UInt64

public struct Timestamp: CustomStringConvertible {
    public var seconds: UInt64
    public var attoseconds: UInt64
    public init(seconds: UInt64, attoseconds: UInt64) {
        self.seconds = seconds
        self.attoseconds = attoseconds
    }
}

/*
 Public API to access the OpenRequest
 */
public struct OpenRequest: Transferable, CustomStringConvertible {
    private var value: _OpenRequestValue

    public var requestIdentifier: RequestIdentifier {
        value.requestIdentifier
    }

    public var clientIdentifier: ClientIdentifier? {
        value.clientIdentifier
    }

    public func withUnsafeBytesSerialization<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        try value.withUnsafeBytes(body)
    }

    /* Transferable.Deserializable */
    public init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws {
        value = .buffer(_OpenRequestBuffer(from: buffer))
    }

    public func _releaseBuffer() {
        value._releaseBuffer()
    }

    /* Identifiable */
    public typealias ID = RequestIdentifier
    public var id: RequestIdentifier { requestIdentifier }

    /* CustomStringConvertible */
    public var description: String {
        value.description
    }

    // Initialization
    private init(_ value: _OpenRequestValue) {
        self.value = value
    }

    public init(_ valueStruct: _OpenRequestStruct) {
        self.init(.struct(valueStruct))
    }

    public init(_ valueBuffer: _OpenRequestBuffer) {
        self.init(.buffer(valueBuffer))
    }

    public static func openRequest(_ valueStruct: _OpenRequestStruct) -> OpenRequest {
        OpenRequest(valueStruct)
    }

    public static func openRequest(_ valueBuffer: _OpenRequestBuffer) -> OpenRequest {
        OpenRequest(valueBuffer)
    }
}

private enum _OpenRequestValue {
    case `struct`(_OpenRequestStruct)
    case buffer(_OpenRequestBuffer)
    case released

    @inline(__always)
    public var requestIdentifier: RequestIdentifier {
        switch self {
        case let .struct(value):
            return value.requestIdentifier
        case let .buffer(value):
            return value.requestIdentifier
        default:
            fatalError("Value has inconsistent state: \(self)")
        }
    }

    @inline(__always)
    public var clientIdentifier: ClientIdentifier? {
        switch self {
        case let .struct(value):
            return value.clientIdentifier
        case let .buffer(value):
            return value.clientIdentifier
        default:
            fatalError("Value has inconsistent state: \(self)")
        }
    }

    @inline(__always)
    public func withUnsafeBytes<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        switch self {
        case let .struct(value):
            return try value.withUnsafeBytes(body)
        case let .buffer(value):
            return try value.withUnsafeBytes(body)
        default:
            fatalError("Value has inconsistent state: \(self)")
        }
    }

    public var description: String {
        switch self {
        case let .struct(value):
            return value.description
        case let .buffer(value):
            return value.description
        default:
            fatalError("Value has inconsistent state: \(self)")
        }
    }

    public func _releaseBuffer() {
        if case var .buffer(value) = self {
            value._releaseBuffer()
        }
    }
}

/*
 Raw struct used when not going over the network, includes
 support for flatbuffers serialization
 */
public struct _OpenRequestStruct: Identifiable, CustomStringConvertible {
    public var requestIdentifier: RequestIdentifier
    public var clientIdentifier: ClientIdentifier?

    public init(requestIdentifier: RequestIdentifier) {
        self.requestIdentifier = requestIdentifier
    }

    // Identifiable
    public typealias ID = RequestIdentifier
    public var id: RequestIdentifier { requestIdentifier }

    private func serialize(to fbb: inout FlatBufferBuilder) {
        let start = DataModel_OpenRequest.startOpenRequest(&fbb)
        DataModel_OpenRequest.add(requestIdentifier: requestIdentifier, &fbb)
        if let clientIdentifier {
            DataModel_OpenRequest.add(clientIdentifier: clientIdentifier, &fbb)
        }
        let finish = DataModel_OpenRequest.endOpenRequest(&fbb, start: start)
        fbb.finish(offset: finish)
    }

    @inline(__always)
    public func withUnsafeBytes<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        var fbb = FlatBufferBuilder(initialSize: 1_024)
        serialize(to: &fbb)
        let fbbDataSize = Int(fbb.size)
        let fbbDataPtr = fbb.buffer.memory.advanced(by: fbb.buffer.capacity - fbbDataSize)
        let buffer = UnsafeRawBufferPointer(start: fbbDataPtr, count: fbbDataSize)
        return try body(buffer)
    }

    public var description: String {
        """
        Message OpenRequest {
            requestIdentifier = \(requestIdentifier)
        \(clientIdentifier != nil ? "    clientIdentifier = \(String(describing: clientIdentifier!))\n" : "")\
        }
        """
    }
}

/*
 Flatbuffers wrapper implementing the protocol for access
 */
public struct _OpenRequestBuffer: Identifiable, CustomStringConvertible {
    var buffer: ByteBuffer
    var message: DataModel_OpenRequest

    public init(from buffer: UnsafeRawBufferPointer) {
        self.buffer = ByteBuffer(contiguousBytes: buffer, count: buffer.count)
        self.message = getRoot(byteBuffer: &self.buffer)
    }

    public mutating func _releaseBuffer() {
    }

    @inline(__always)
    public var requestIdentifier: RequestIdentifier {
        message.requestIdentifier
    }

    @inline(__always)
    public var clientIdentifier: ClientIdentifier? {
        message.clientIdentifier
    }

    // Identifiable
    public typealias ID = RequestIdentifier
    public var id: RequestIdentifier { requestIdentifier }

    @inline(__always)
    public func withUnsafeBytes<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        let ptr = UnsafeRawBufferPointer(start: buffer.memory, count: buffer.capacity)
        return try body(ptr)
    }

    public var description: String {
        """
        Message OpenRequest {
            requestIdentifier = \(requestIdentifier)
        \(clientIdentifier != nil ? "    clientIdentifier = \(String(describing: clientIdentifier!))\n" : "")\
        }
        """
    }
}

/*
 Public API to access the SnapshotDone
 */
public struct SnapshotDone: Transferable, CustomStringConvertible {
    private var value: _SnapshotDoneValue

    public var streamIdentifier: StreamIdentifier {
        value.streamIdentifier
    }

    /* Transferable.Serializable */
    public func withUnsafeBytesSerialization<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        try value.withUnsafeBytes(body)
    }

    /* Transferable.Deserializable */
    public init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws {
        value = .buffer(_SnapshotDoneBuffer(from: buffer))
    }

    public func _releaseBuffer() {
        value._releaseBuffer()
    }

    /* Identifiable */
    public typealias ID = StreamIdentifier
    public var id: StreamIdentifier { streamIdentifier }

    /* CustomStringConvertible */
    public var description: String {
        value.description
    }

    // Initialization
    private init(_ value: _SnapshotDoneValue) {
        self.value = value
    }

    public init(_ valueStruct: _SnapshotDoneStruct) {
        self.init(.struct(valueStruct))
    }

    public init(_ valueBuffer: _SnapshotDoneBuffer) {
        self.init(.buffer(valueBuffer))
    }

    public static func snapshotDone(_ valueStruct: _SnapshotDoneStruct) -> SnapshotDone {
        SnapshotDone(valueStruct)
    }

    public static func snapshotDone(_ valueBuffer: _SnapshotDoneBuffer) -> SnapshotDone {
        SnapshotDone(valueBuffer)
    }
}

private enum _SnapshotDoneValue {
    case `struct`(_SnapshotDoneStruct)
    case buffer(_SnapshotDoneBuffer)
    case released

    @inline(__always)
    public var streamIdentifier: StreamIdentifier {
        switch self {
        case let .struct(value):
            return value.streamIdentifier
        case let .buffer(value):
            return value.streamIdentifier
        default:
            fatalError("Value has inconsistent state: \(self)")
        }
    }

    @inline(__always)
    public func withUnsafeBytes<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        switch self {
        case let .struct(value):
            return try value.withUnsafeBytes(body)
        case let .buffer(value):
            return try value.withUnsafeBytes(body)
        default:
            fatalError("Value has inconsistent state: \(self)")
        }
    }

    public var description: String {
        switch self {
        case let .struct(value):
            return value.description
        case let .buffer(value):
            return value.description
        default:
            fatalError("Value has inconsistent state: \(self)")
        }
    }

    public func _releaseBuffer() {
        if case var .buffer(value) = self {
            value._releaseBuffer()
        }
    }
}

/*
 Raw struct used when not going over the network, includes
 support for flatbuffers serialization
 */
public struct _SnapshotDoneStruct: Identifiable, CustomStringConvertible {
    public var streamIdentifier: StreamIdentifier

    public init(streamIdentifier: StreamIdentifier) {
        self.streamIdentifier = streamIdentifier
    }

    // Identifiable
    public typealias ID = StreamIdentifier
    public var id: StreamIdentifier { streamIdentifier }

    private func serialize(to fbb: inout FlatBufferBuilder) {
        let start = DataModel_SnapshotDone.startSnapshotDone(&fbb)
        DataModel_SnapshotDone.add(streamIdentifier: streamIdentifier, &fbb)
        let finish = DataModel_SnapshotDone.endSnapshotDone(&fbb, start: start)
        fbb.finish(offset: finish)
    }

    @inline(__always)
    public func withUnsafeBytes<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        var fbb = FlatBufferBuilder(initialSize: 1_024)
        serialize(to: &fbb)
        let fbbDataSize = Int(fbb.size)
        let fbbDataPtr = fbb.buffer.memory.advanced(by: fbb.buffer.capacity - fbbDataSize)
        let buffer = UnsafeRawBufferPointer(start: fbbDataPtr, count: fbbDataSize)
        return try body(buffer)
    }

    public var description: String {
        """
        SnapshotDone {
            streamIdentifier = \(streamIdentifier)
        }
        """
    }
}

/*
 Flatbuffers wrapper implementing the protocol for access
 */
public struct _SnapshotDoneBuffer: Identifiable, CustomStringConvertible {
    var buffer: ByteBuffer
    var message: DataModel_SnapshotDone

    public init(from buffer: UnsafeRawBufferPointer) {
        self.buffer = ByteBuffer(contiguousBytes: buffer, count: buffer.count)
        self.message = getRoot(byteBuffer: &self.buffer)
    }

    public mutating func _releaseBuffer() {
    }

    @inline(__always)
    public var streamIdentifier: StreamIdentifier {
        message.streamIdentifier
    }

    // Identifiable
    public typealias ID = StreamIdentifier
    public var id: StreamIdentifier { streamIdentifier }

    @inline(__always)
    public func withUnsafeBytes<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        let ptr = UnsafeRawBufferPointer(start: buffer.memory, count: buffer.capacity)
        return try body(ptr)
    }

    public var description: String {
        """
        SnapshotDone {
            streamIdentifier = \(streamIdentifier)
        }
        """
    }
}

/*
 Public API to access the Stream
 */
public struct Stream: Transferable, CustomStringConvertible {
    private var value: _StreamValue

    public var streamIdentifier: StreamIdentifier {
        value.streamIdentifier
    }

    /* Transferable.Serializable */
    public func withUnsafeBytesSerialization<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        try value.withUnsafeBytes(body)
    }

    /* Transferable.Deserializable */
    public init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws {
        value = .buffer(_StreamBuffer(from: buffer))
    }

    public func _releaseBuffer() {
        value._releaseBuffer()
    }

    /* Identifiable */
    public typealias ID = StreamIdentifier
    public var id: StreamIdentifier { streamIdentifier }

    /* CustomStringConvertible */
    public var description: String {
        value.description
    }

    // Initialization
    private init(_ value: _StreamValue) {
        self.value = value
    }

    public init(_ valueStruct: _StreamStruct) {
        self.init(.struct(valueStruct))
    }

    public init(_ valueBuffer: _StreamBuffer) {
        self.init(.buffer(valueBuffer))
    }

    public static func stream(_ valueStruct: _StreamStruct) -> Stream {
        Stream(valueStruct)
    }

    public static func stream(_ valueBuffer: _StreamBuffer) -> Stream {
        Stream(valueBuffer)
    }
}

private enum _StreamValue {
    case `struct`(_StreamStruct)
    case buffer(_StreamBuffer)
    case released

    @inline(__always)
    public var streamIdentifier: StreamIdentifier {
        switch self {
        case let .struct(value):
            return value.streamIdentifier
        case let .buffer(value):
            return value.streamIdentifier
        default:
            fatalError("Value has inconsistent state: \(self)")
        }
    }

    @inline(__always)
    public func withUnsafeBytes<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        switch self {
        case let .struct(value):
            return try value.withUnsafeBytes(body)
        case let .buffer(value):
            return try value.withUnsafeBytes(body)
        default:
            fatalError("Value has inconsistent state: \(self)")
        }
    }

    public var description: String {
        switch self {
        case let .struct(value):
            return value.description
        case let .buffer(value):
            return value.description
        default:
            fatalError("Value has inconsistent state: \(self)")
        }
    }

    public func _releaseBuffer() {
        if case var .buffer(value) = self {
            value._releaseBuffer()
        }
    }
}

/*
 Raw struct used when not going over the network, includes
 support for flatbuffers serialization
 */
public struct _StreamStruct: Identifiable, CustomStringConvertible {
    public var streamIdentifier: StreamIdentifier

    public init(streamIdentifier: StreamIdentifier) {
        self.streamIdentifier = streamIdentifier
    }

    // Identifiable
    public typealias ID = StreamIdentifier
    public var id: StreamIdentifier { streamIdentifier }

    private func serialize(to fbb: inout FlatBufferBuilder) {
        let start = DataModel_Stream.startStream(&fbb)
        DataModel_Stream.add(streamIdentifier: streamIdentifier, &fbb)
        let finish = DataModel_Stream.endStream(&fbb, start: start)
        fbb.finish(offset: finish)
    }

    @inline(__always)
    public func withUnsafeBytes<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        var fbb = FlatBufferBuilder(initialSize: 1_024)
        serialize(to: &fbb)
        let fbbDataSize = Int(fbb.size)
        let fbbDataPtr = fbb.buffer.memory.advanced(by: fbb.buffer.capacity - fbbDataSize)
        let buffer = UnsafeRawBufferPointer(start: fbbDataPtr, count: fbbDataSize)
        return try body(buffer)
    }

    public var description: String {
        """
        Stream {
            streamIdentifier = \(streamIdentifier)
        }
        """
    }
}

/*
 Flatbuffers wrapper implementing the protocol for access
 */
public struct _StreamBuffer: Identifiable, CustomStringConvertible {
    var buffer: ByteBuffer
    var message: DataModel_Stream

    public init(from buffer: UnsafeRawBufferPointer) {
        self.buffer = ByteBuffer(contiguousBytes: buffer, count: buffer.count)
        self.message = getRoot(byteBuffer: &self.buffer)
    }

    public mutating func _releaseBuffer() {
    }

    @inline(__always)
    public var streamIdentifier: StreamIdentifier {
        message.streamIdentifier
    }

    // Identifiable
    public typealias ID = StreamIdentifier
    public var id: StreamIdentifier { streamIdentifier }

    @inline(__always)
    public func withUnsafeBytes<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        let ptr = UnsafeRawBufferPointer(start: buffer.memory, count: buffer.capacity)
        return try body(ptr)
    }

    public var description: String {
        """
        Stream {
            streamIdentifier = \(streamIdentifier)
        }
        """
    }
}

/*
 Public API to access the StreamOpened
 */
public struct StreamOpened: Transferable, CustomStringConvertible {
    private var value: _StreamOpenedValue

    public var requestIdentifier: RequestIdentifier {
        value.requestIdentifier
    }

    public var streamIdentifier: StreamIdentifier? {
        value.streamIdentifier
    }

    /* Transferable.Serializable */
    public func withUnsafeBytesSerialization<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        try value.withUnsafeBytes(body)
    }

    /* Transferable.Deserializable */
    public init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws {
        value = .buffer(_StreamOpenedBuffer(from: buffer))
    }

    public func _releaseBuffer() {
        value._releaseBuffer()
    }

    /* Identifiable */
    public typealias ID = RequestIdentifier
    public var id: RequestIdentifier { requestIdentifier }

    /* CustomStringConvertible */
    public var description: String {
        value.description
    }

    // Initialization
    private init(_ value: _StreamOpenedValue) {
        self.value = value
    }

    public init(_ valueStruct: _StreamOpenedStruct) {
        self.init(.struct(valueStruct))
    }

    public init(_ valueBuffer: _StreamOpenedBuffer) {
        self.init(.buffer(valueBuffer))
    }

    public static func streamOpened(_ valueStruct: _StreamOpenedStruct) -> StreamOpened {
        StreamOpened(valueStruct)
    }

    public static func streamOpened(_ valueBuffer: _StreamOpenedBuffer) -> StreamOpened {
        StreamOpened(valueBuffer)
    }
}

private enum _StreamOpenedValue {
    case `struct`(_StreamOpenedStruct)
    case buffer(_StreamOpenedBuffer)
    case released

    @inline(__always)
    public var requestIdentifier: RequestIdentifier {
        switch self {
        case let .struct(value):
            return value.requestIdentifier
        case let .buffer(value):
            return value.requestIdentifier
        default:
            fatalError("Value has inconsistent state: \(self)")
        }
    }

    @inline(__always)
    public var streamIdentifier: StreamIdentifier? {
        switch self {
        case let .struct(value):
            return value.streamIdentifier
        case let .buffer(value):
            return value.streamIdentifier
        default:
            fatalError("Value has inconsistent state: \(self)")
        }
    }

    @inline(__always)
    public func withUnsafeBytes<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        switch self {
        case let .struct(value):
            return try value.withUnsafeBytes(body)
        case let .buffer(value):
            return try value.withUnsafeBytes(body)
        default:
            fatalError("Value has inconsistent state: \(self)")
        }
    }

    public var description: String {
        switch self {
        case let .struct(value):
            return value.description
        case let .buffer(value):
            return value.description
        default:
            fatalError("Value has inconsistent state: \(self)")
        }
    }

    public func _releaseBuffer() {
        if case var .buffer(value) = self {
            value._releaseBuffer()
        }
    }
}

/*
 Raw struct used when not going over the network, includes
 support for flatbuffers serialization
 */
public struct _StreamOpenedStruct: Identifiable, CustomStringConvertible {
    public var requestIdentifier: RequestIdentifier
    public var streamIdentifier: StreamIdentifier?

    public init(requestIdentifier: RequestIdentifier) {
        self.requestIdentifier = requestIdentifier
    }

    // Identifiable
    public typealias ID = RequestIdentifier
    public var id: RequestIdentifier { requestIdentifier }

    private func serialize(to fbb: inout FlatBufferBuilder) {
        let start = DataModel_StreamOpened.startStreamOpened(&fbb)
        DataModel_StreamOpened.add(requestIdentifier: requestIdentifier, &fbb)
        if let streamIdentifier {
            DataModel_StreamOpened.add(streamIdentifier: streamIdentifier, &fbb)
        }
        let finish = DataModel_StreamOpened.endStreamOpened(&fbb, start: start)
        fbb.finish(offset: finish)
    }

    @inline(__always)
    public func withUnsafeBytes<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        var fbb = FlatBufferBuilder(initialSize: 1_024)
        serialize(to: &fbb)
        let fbbDataSize = Int(fbb.size)
        let fbbDataPtr = fbb.buffer.memory.advanced(by: fbb.buffer.capacity - fbbDataSize)
        let buffer = UnsafeRawBufferPointer(start: fbbDataPtr, count: fbbDataSize)
        return try body(buffer)
    }

    public var description: String {
        """
        StreamOpened {
            requestIdentifier = \(requestIdentifier)
        \(streamIdentifier != nil ? "    streamIdentifier = \(String(describing: streamIdentifier!))\n" : "")\
        }
        """
    }
}

/*
 Flatbuffers wrapper implementing the protocol for access
 */
public struct _StreamOpenedBuffer: Identifiable, CustomStringConvertible {
    var buffer: ByteBuffer
    var message: DataModel_StreamOpened

    public init(from buffer: UnsafeRawBufferPointer) {
        self.buffer = ByteBuffer(contiguousBytes: buffer, count: buffer.count)
        self.message = getRoot(byteBuffer: &self.buffer)
    }

    public mutating func _releaseBuffer() {
    }

    @inline(__always)
    public var requestIdentifier: RequestIdentifier {
        message.requestIdentifier
    }

    @inline(__always)
    public var streamIdentifier: StreamIdentifier? {
        message.streamIdentifier
    }

    // Identifiable
    public typealias ID = RequestIdentifier
    public var id: RequestIdentifier { requestIdentifier }

    @inline(__always)
    public func withUnsafeBytes<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        let ptr = UnsafeRawBufferPointer(start: buffer.memory, count: buffer.capacity)
        return try body(ptr)
    }

    public var description: String {
        """
        StreamOpened {
            requestIdentifier = \(requestIdentifier)
        \(streamIdentifier != nil ? "    streamIdentifier = \(String(describing: streamIdentifier!))\n" : "")\
        }
        """
    }
}

public struct Monster: Transferable, CustomStringConvertible {
    private var value: _MonsterValue

    public var identifier: MonsterIdentifier {
        value.identifier
    }

    public var name: String? {
        value.name
    }

    public var pos: DataModel_Vec3? {
        value.pos
    }

    public var mana: UInt16? {
        value.mana
    }

    public var hp: UInt16? {
        value.hp
    }

    public var color: DataModel_Color? {
        value.color
    }

    /* Transferable.Serializable */
    public func withUnsafeBytesSerialization<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        try value.withUnsafeBytes(body)
    }

    /* Transferable.Deserializable */
    public init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws {
        value = .buffer(_MonsterBuffer(from: buffer))
    }

    public func _releaseBuffer() {
        value._releaseBuffer()
    }

    /* Identifiable */
    public typealias ID = MonsterIdentifier
    public var id: MonsterIdentifier { identifier }

    /* CustomStringConvertible */
    public var description: String {
        value.description
    }

    // Initialization
    private init(_ value: _MonsterValue) {
        self.value = value
    }

    public init(_ valueStruct: _MonsterStruct) {
        self.init(.struct(valueStruct))
    }

    public init(_ valueBuffer: _MonsterBuffer) {
        self.init(.buffer(valueBuffer))
    }
}

private enum _MonsterValue {
    case `struct`(_MonsterStruct)
    case buffer(_MonsterBuffer)
    case released

    @inline(__always)
    public var identifier: MonsterIdentifier {
        switch self {
        case let .struct(value):
            return value.identifier
        case let .buffer(value):
            return value.identifier
        default:
            fatalError("Value has inconsistent state: \(self)")
        }
    }

    @inline(__always)
    public var name: String? {
        switch self {
        case let .struct(value):
            return value.name
        case let .buffer(value):
            return value.name
        default:
            fatalError("Value has inconsistent state: \(self)")
        }
    }

    @inline(__always)
    public var pos: DataModel_Vec3? {
        switch self {
        case let .struct(value):
            return value.pos
        case let .buffer(value):
            return value.pos
        default:
            fatalError("Value has inconsistent state: \(self)")
        }
    }

    @inline(__always)
    public var mana: UInt16? {
        switch self {
        case let .struct(value):
            return value.mana
        case let .buffer(value):
            return value.mana
        default:
            fatalError("Value has inconsistent state: \(self)")
        }
    }

    @inline(__always)
    public var hp: UInt16? {
        switch self {
        case let .struct(value):
            return value.hp
        case let .buffer(value):
            return value.hp
        default:
            fatalError("Value has inconsistent state: \(self)")
        }
    }

    @inline(__always)
    public var color: DataModel_Color? {
        switch self {
        case let .struct(value):
            return value.color
        case let .buffer(value):
            return value.color
        default:
            fatalError("Value has inconsistent state: \(self)")
        }
    }

    @inline(__always)
    public func withUnsafeBytes<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        switch self {
        case let .struct(value):
            return try value.withUnsafeBytes(body)
        case let .buffer(value):
            return try value.withUnsafeBytes(body)
        default:
            fatalError("Value has inconsistent state: \(self)")
        }
    }

    public var description: String {
        switch self {
        case let .struct(value):
            return value.description
        case let .buffer(value):
            return value.description
        default:
            fatalError("Value has inconsistent state: \(self)")
        }
    }

    public func _releaseBuffer() {
        if case var .buffer(value) = self {
            value._releaseBuffer()
        }
    }
}

/*
 Raw struct used when not going over the network, includes
 support for flatbuffers serialization
 */
public struct _MonsterStruct: Identifiable, CustomStringConvertible {
    public var identifier: MonsterIdentifier
    public var name: String?
    public var pos: DataModel_Vec3?
    public var mana: UInt16?
    public var hp: UInt16?
    public var color: DataModel_Color?
    public var inventory: [UInt8]?

    public init(identifier: MonsterIdentifier) {
        self.identifier = identifier
    }

    // Identifiable
    public typealias ID = MonsterIdentifier
    public var id: MonsterIdentifier { identifier }

    private func serialize(to fbb: inout FlatBufferBuilder) {
        let nameOffset = fbb.create(string: name)
        var inventoryOffset = Offset()
        if let inventory {
            inventoryOffset = fbb.createVector(inventory)
        }
        let start = DataModel_Monster.startMonster(&fbb)
        DataModel_Monster.add(identifier: identifier, &fbb)
        DataModel_Monster.add(name: nameOffset, &fbb)
        DataModel_Monster.add(pos: pos, &fbb)
        DataModel_Monster.add(mana: mana, &fbb)
        DataModel_Monster.add(hp: hp, &fbb)
        DataModel_Monster.add(color: color, &fbb)
        DataModel_Monster.addVectorOf(inventory: inventoryOffset, &fbb)
        let finish = DataModel_Monster.endMonster(&fbb, start: start)
        fbb.finish(offset: finish)
    }

    @inline(__always)
    public func withUnsafeBytes<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        var fbb = FlatBufferBuilder(initialSize: 1_024)
        serialize(to: &fbb)
        let fbbDataSize = Int(fbb.size)
        let fbbDataPtr = fbb.buffer.memory.advanced(by: fbb.buffer.capacity - fbbDataSize)
        let buffer = UnsafeRawBufferPointer(start: fbbDataPtr, count: fbbDataSize)
        return try body(buffer)
    }

    public var description: String {
        """
        Monster {
            identifier = \(identifier)
        \(name != nil ? "    name = \(name!)\n" : "")\
        \(pos != nil ? "    pos = \(String(describing: pos!))\n" : "")\
        \(mana != nil ? "    mana = \(String(describing: mana!))\n" : "")\
        \(hp != nil ? "    hp = \(String(describing: hp!))\n" : "")\
        \(color != nil ? "    color = \(String(describing: color!))\n" : "")\
        }
        """
    }
}

public struct _MonsterBuffer: Identifiable, CustomStringConvertible {
    var buffer: ByteBuffer
    var message: DataModel_Monster

    public init(from buffer: UnsafeRawBufferPointer) {
        self.buffer = ByteBuffer(contiguousBytes: buffer, count: buffer.count)
        self.message = getRoot(byteBuffer: &self.buffer)
    }

    public mutating func _releaseBuffer() {
    }

    @inline(__always)
    public var identifier: MonsterIdentifier {
        message.identifier
    }

    @inline(__always)
    public var name: String? {
        message.name
    }

    @inline(__always)
    public var pos: DataModel_Vec3? {
        message.pos
    }

    @inline(__always)
    public var mana: UInt16? {
        message.mana
    }

    @inline(__always)
    public var hp: UInt16? {
        message.hp
    }

    @inline(__always)
    public var color: DataModel_Color? {
        message.color
    }

    @inline(__always)
    public var inventory: [UInt8]? {
        message.inventory
    }

    // Identifiable
    public typealias ID = MonsterIdentifier
    public var id: MonsterIdentifier { identifier }

    @inline(__always)
    public func withUnsafeBytes<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        let ptr = UnsafeRawBufferPointer(start: buffer.memory, count: buffer.capacity)
        return try body(ptr)
    }

    public var description: String {
        """
        Monster {
            identifier = \(identifier)
        \(name != nil ? "    name = \(name!)\n" : "")\
        \(pos != nil ? "    pos = \(String(describing: pos!))\n" : "")\
        \(mana != nil ? "    mana = \(String(describing: mana!))\n" : "")\
        \(hp != nil ? "    hp = \(String(describing: hp!))\n" : "")\
        \(color != nil ? "    color = \(String(describing: color!))\n" : "")\
        }
        """
    }
}
