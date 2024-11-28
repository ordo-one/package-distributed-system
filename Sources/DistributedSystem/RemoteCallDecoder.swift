import Distributed
import NIOCore

public struct RemoteCallDecoder: DistributedTargetInvocationDecoder {
    public typealias SerializationRequirement = Transferable

    private var genericSubstitutions: [Any.Type]
    private var buffer: ByteBuffer
    private var arguments: [Deserializable]

    public init(envelope: InvocationEnvelope) {
        genericSubstitutions = envelope.genericSubstitutions
        buffer = envelope.arguments
        arguments = []
    }

    public mutating func decodeNextArgument<Argument: SerializationRequirement>() throws -> Argument {
        let size = try buffer.readWithUnsafeReadableBytes { ptr in try ULEB128.decode(ptr, as: UInt.self) }

        guard let slice = buffer.readSlice(length: Int(size)) else {
            throw DistributedSystemErrors.decodeError(description: "Failed to decode argument")
        }

        return try slice.withUnsafeReadableBytes { bytes in
            // The 'Argument' type can do nothing but just copy data from the buffer
            // TODO: probably would worth to invent a possibility to retain the buffer for the 'Argument' instance lifetime (sc-1307)
            let argument = try Argument(fromSerializedBuffer: bytes)
            arguments.append(argument)
            return argument
        }
    }

    public mutating func decodeGenericSubstitutions() throws -> [Any.Type] {
        genericSubstitutions
    }

    public mutating func decodeErrorType() throws -> Any.Type? {
        // Throwing error is not supported in this version
        nil
    }

    public mutating func decodeReturnType() throws -> Any.Type? {
        // Return type is not supported in this version
        nil
    }

    public func _releaseArguments() {
        for argument in arguments {
            argument._releaseBuffer()
        }
    }
}
