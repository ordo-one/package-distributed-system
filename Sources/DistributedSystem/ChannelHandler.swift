import Logging
import NIOCore

extension Channel {
    var remoteAddressDescription: String {
        remoteAddress?.description ?? "<nil>"
    }
}

extension ChannelHandlerContext {
    var remoteAddressDescription: String {
        remoteAddress?.description ?? "<nil>"
    }
}

class ChannelHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    enum Side: String {
        case server
        case client
    }

    private var logger: Logger { DistributedSystem.logger }
    private var logMetadata: Logger.Metadata? { actorSystem.logMetadata }
    private let side: Side
    private let actorSystem: DistributedSystem

    init(_ side: Side, _ actorSystem: DistributedSystem) {
        self.side = side
        self.actorSystem = actorSystem
    }

    func channelActive(context: ChannelHandlerContext) {
        logger.debug("\(context.remoteAddressDescription): \(side) channel active")
    }

    func channelInactive(context: ChannelHandlerContext) {
        logger.debug("\(context.remoteAddressDescription): channel inactive")
        actorSystem.channelInactive(context)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var buffer = unwrapInboundIn(data)
        logger.trace("\(context.remoteAddressDescription): received \(buffer.readableBytes) bytes", metadata: logMetadata)
        actorSystem.channelRead(context, &buffer)
    }

    // Flush it out. This can make use of gathering writes if multiple buffers are pending
    func channelReadComplete(context: ChannelHandlerContext) {
        context.flush()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        logger.error("\(String(describing: context.remoteAddress)): network error: \(error)")
        context.close(promise: nil)
    }
}

class StreamDecoder: ByteToMessageDecoder {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer

    private var logger: Logger { DistributedSystem.logger }

    func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        logger.trace("\(context.channel.remoteAddressDescription): stream decoder: available \(buffer.readableBytes) bytes")
        if var messageSize = buffer.getInteger(at: buffer.readerIndex, as: UInt32.self) {
            messageSize += UInt32(MemoryLayout<UInt32>.size)
            if let slice = buffer.readSlice(length: Int(messageSize)) {
                context.fireChannelRead(wrapInboundOut(slice))
                return .continue
            }
        }
        return .needMoreData
    }
}
