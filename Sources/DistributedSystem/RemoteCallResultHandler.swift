import Distributed
import DistributedSystemConformance
import Logging
import NIOCore

public final class RemoteCallResultHandler: DistributedTargetInvocationResultHandler {
    public typealias SerializationRequirement = DistributedSystemConformance.Transferable

    private var buffer: ByteBuffer?
    private var logger: Logger { DistributedSystem.logger }

    var hasResult: Bool { buffer != nil }

    func sendTo(_ channel: Channel, for callID: UInt64) {
        if var buffer {
            if callID != 0 {
                let offs = MemoryLayout<UInt32>.size + MemoryLayout<DistributedSystem.SessionMessage.RawValue>.size
                buffer.setInteger(callID, at: offs)
                _ = channel.writeAndFlush(buffer)
            } else {
                logger.error("internal error: unexpected result for void call")
            }
            self.buffer = nil
        } else {
            logger.error("internal error: unexpected result for void call")
        }
    }

    static func decode(from buffer: inout ByteBuffer) throws -> (UInt64, String, ByteBuffer) {
        if let callID = buffer.readInteger(as: UInt64.self),
           let typeHintSize = buffer.readInteger(as: UInt16.self),
           let typeHint = buffer.readString(length: Int(typeHintSize)),
           let resultSize = buffer.readInteger(as: UInt32.self),
           let result = buffer.readSlice(length: Int(resultSize)) {
            return (callID, typeHint, result)
        } else {
            throw DistributedSystemErrors.error("failed to decode remote result (\(#file):\(#line))")
        }
    }

    public func onReturn(value: some SerializationRequirement) async throws {
        value.withUnsafeBytesSerialization { bytes in
            let valueType = type(of: value as Any)
            let valueTypeHint: String = _mangledTypeName(valueType) ?? _typeName(valueType)
            let payloadSize =
                MemoryLayout<DistributedSystem.SessionMessage.RawValue>.size +
                MemoryLayout<UInt64>.size +
                MemoryLayout<UInt16>.size + valueTypeHint.count +
                MemoryLayout<UInt32>.size + bytes.count
            var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt32>.size + payloadSize)
            buffer.writeInteger(UInt32(payloadSize))
            buffer.writeInteger(DistributedSystem.SessionMessage.invocationResult.rawValue)
            buffer.writeInteger(UInt64(0))
            buffer.writeInteger(UInt16(valueTypeHint.count))
            buffer.writeString(valueTypeHint)
            buffer.writeInteger(UInt32(bytes.count))
            buffer.writeBytes(bytes)
            self.buffer = buffer
        }
    }

    public func onReturnVoid() async throws {
        // logger.debug("onReturnVoid")
    }

    public func onThrow(error: some Error) async throws {
        // TODO: if func throws we need to send some result, right?
        logger.debug("onThrow: \(error)")
    }
}
