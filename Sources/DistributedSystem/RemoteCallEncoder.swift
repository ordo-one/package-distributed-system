import Distributed
import DistributedSystemConformance
import Helpers
import NIOCore

public struct RemoteCallEncoder: DistributedTargetInvocationEncoder {
    public typealias SerializationRequirement = DistributedSystemConformance.Transferable

    var genericSubstitutions = [String]()
    var arguments = ByteBufferAllocator().buffer(capacity: 0)

    public mutating func recordArgument(_ argument: RemoteCallArgument<some SerializationRequirement>) throws {
        argument.value.withUnsafeBytesSerialization { bytes in
            let size = ULEB128.size(UInt(bytes.count)) + bytes.count
            if arguments.writableBytes < size {
                let capacity = nearestPowerOf2(arguments.readableBytes + size)
                var buffer = ByteBufferAllocator().buffer(capacity: capacity)
                buffer.writeBuffer(&arguments)
                arguments = buffer
            }
            arguments.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in ULEB128.encode(UInt(bytes.count), to: ptr.baseAddress!) }
            arguments.writeBytes(bytes)
        }
    }

    public mutating func recordReturnType(_: (some SerializationRequirement).Type) throws {
        // Return type is not supported in this version
    }

    public mutating func recordGenericSubstitution<T>(_ type: T.Type) throws {
        let typeName = _mangledTypeName(type) ?? _typeName(type)
        genericSubstitutions.append(typeName)
    }

    public mutating func recordErrorType(_: (some Error).Type) throws {
        // Throwing error is not supported in this version
    }

    public mutating func doneRecording() throws {
        // Nothing to do when it's done
    }
}
