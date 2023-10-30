import Distributed
import DistributedSystemConformance
import NIOCore

public struct RemoteCallDecoder: DistributedTargetInvocationDecoder {
    public typealias SerializationRequirement = DistributedSystemConformance.Transferable

    private var genericSubstitutions: [Any.Type]
    private var arguments: ByteBuffer

    init(envelope: InvocationEnvelope) {
        genericSubstitutions = envelope.genericSubstitutions
        arguments = envelope.arguments
    }

    public mutating func decodeNextArgument<Argument: SerializationRequirement>() throws -> Argument {
        let size = try arguments.readWithUnsafeReadableBytes { ptr in try ULEB128.decode(ptr, as: UInt.self) }

        guard let slice = arguments.readSlice(length: Int(size)) else {
            throw DistributedSystemErrors.decodeError(description: "Failed to decode argument")
        }

        return try slice.withUnsafeReadableBytes { ptr in
            // The 'Argument' type can do nothing but just copy data from the buffer
            // TODO: probably would worth to invent a possibility to retain the buffer for the 'Argument' instance lifetime (sc-1307)
            try Argument(fromSerializedBuffer: ptr)
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
}
