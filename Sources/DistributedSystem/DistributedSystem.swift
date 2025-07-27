// Copyright 2024 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

// swiftformat:disable opaqueGenericParameters

import Atomics
import ConsulServiceDiscovery
import Dispatch
import Distributed
import Logging
import struct Foundation.Data
import struct Foundation.UUID
import NIOCore
internal import NIOPosix
internal import struct NIOConcurrencyHelpers.NIOLock
internal import struct NIOConcurrencyHelpers.NIOLockedValueBox

#if os(Linux)
typealias uuid_t = (UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8)
#endif

extension Channel {
    var debugDescription: String {
        remoteAddress?.description ?? "?"
    }
}

final class Box<T> {
    var value: T

    init(_ value: T) {
        self.value = value
    }
}

public enum CompressionMode: CustomStringConvertible {
    case disabled
    case streaming
    case dictionary(Data, UInt32)

    // very simple checksum calculation
    // would use Hasher() but it use random seeds in the different processes
    private static func crc32(_ ptr: UnsafeRawBufferPointer) -> UInt32 {
        let polynomial: UInt32 = 0xEDB88320
        var crc: UInt32 = 0xFFFFFFFF

        for byte in ptr {
            var currentByte = UInt32(byte)
            for _ in 0..<8 {
                let mix = (crc ^ currentByte) & 1
                crc >>= 1
                if mix != 0 {
                    crc ^= polynomial
                }
                currentByte >>= 1
            }
        }

        return ~crc
    }

    public static func dictionary(_ dictionary: Data) -> Self {
        let crc = dictionary.withUnsafeBytes { Self.crc32($0) }
        return .dictionary(dictionary, crc)
    }

    public var description: String {
        switch self {
        case .disabled: "disabled"
        case .streaming: "streaming"
        case .dictionary: "dictionary"
        }
    }
}

public class DistributedSystem: DistributedActorSystem, @unchecked Sendable {
    public typealias ActorID = EndpointIdentifier
    public typealias InvocationEncoder = RemoteCallEncoder
    public typealias InvocationDecoder = RemoteCallDecoder
    public typealias ResultHandler = RemoteCallResultHandler
    public typealias SerializationRequirement = Transferable

    public struct ModuleIdentifier: Hashable, Codable, CustomStringConvertible, Sendable {
        public let rawValue: UInt64

        public var description: String {
            String(describing: rawValue)
        }

        public init(_ rawValue: UInt64) {
            self.rawValue = rawValue
        }

        public init?(_ rawValue: UInt64?) {
            if let rawValue {
                self.rawValue = rawValue
            } else {
                return nil
            }
        }

        public init?(_ str: String) {
            if let rawValue = UInt64(str) {
                self.init(rawValue)
            } else {
                return nil
            }
        }
    }

    public final class CancellationToken: Hashable, @unchecked Sendable {
        private let actorSystem: DistributedSystem
        var serviceName: String?
        var cancelled: Bool = false

        var ptr: UnsafeRawPointer {
            UnsafeRawPointer(Unmanaged.passUnretained(self).toOpaque())
        }

        init(_ actorSystem: DistributedSystem) {
            self.actorSystem = actorSystem
        }

        // return true if was not cancelled before
        public func cancel() -> Bool {
            actorSystem.cancel(self)
        }

        public func hash(into hasher: inout Hasher) {
            hasher.combine(ptr)
        }

        public static func == (lhs: DistributedSystem.CancellationToken, rhs: DistributedSystem.CancellationToken) -> Bool {
            (lhs === rhs)
        }
    }

    public enum ServiceMetadata: String {
        case moduleIdentifier
        case processIdentifier
        case systemName
        case datacenter
    }

    let loggerBox: Box<Logger>
    var logger: Logger { loggerBox.value }

    var endpointQueueWarningSize: UInt64 = (1024 * 1024)
    var endpointQueueHighWatermark: UInt64 = (10 * 1024 * 1024)
    var endpointQueueLowWatermark: UInt64 = (1024 * 1024)
    private static let endpointQueueSuspendIndicator: UInt64 = 0x8000_0000_0000_0000
    private static let endpointQueueSizeBits: Int = (UInt64.bitWidth - 8)

    // TODO: replace with configuration
    static let pingInterval = TimeAmount.seconds(2)
    static let serviceDiscoveryTimeout = TimeAmount.seconds(5)
    static let reconnectInterval = TimeAmount.seconds(5)

    static let protocolVersionMajor: UInt16 = 5
    static let protocolVersionMinor: UInt16 = 0

    enum SessionMessage: UInt16 {
        case createServiceInstance = 0
        case invocationEnvelope = 1
        case invocationResult = 2
        case suspendEndpoint = 3
        case resumeEndpoint = 4
        case ping = 5
        case pong = 6
    }

    public let systemName: String
    private let addressTag: String?

    let eventLoopGroup: EventLoopGroup
    let consul: Consul
    // Some services expect service discovery to be available from distributed system
    public let consulServiceDiscovery: ConsulServiceDiscovery

    private enum ActorInfo {
        struct Outbound {
            var suspended: Bool
            var continuations: [CheckedContinuation<Void, Error>]

            init(_ continuations: [CheckedContinuation<Void, Error>] = []) {
                self.suspended = false
                self.continuations = continuations
            }
        }

        struct Inbound {
            let continuation: AsyncStream<InvocationEnvelope>.Continuation
            let queueState: ManagedAtomic<UInt64>

            init(_ continuation: AsyncStream<InvocationEnvelope>.Continuation, _ queueState: ManagedAtomic<UInt64>) {
                self.continuation = continuation
                self.queueState = queueState
            }
        }

        case newClient(any DistributedActor) // retain the client actor instance while it will not be linked to local or remote service
        case localService(ServiceFactory)
        case remoteClient(Outbound)
        case remoteService(Outbound)
        case clientForRemoteService(Inbound)
        case serviceForRemoteClient(Inbound)
    }

    final class TargetFuncsIndex: @unchecked Sendable {
        private var index = [String: UInt32]()

        func getId(for name: String) -> UInt32? {
            if let id = index[name] {
                return id
            } else {
                let id = UInt32(index.count)
                index[name] = id
                return nil
            }
        }
    }

    struct ChannelInfo {
        let channel: Channel
        var bytesReceived = 0
        var bytesReceivedCheckpoint = 0
        var bytesReceivedTimeouts = 0
        var targetFuncsIndex = TargetFuncsIndex()
        var pendingSyncCalls = Set<UInt64>()

        init(_ channel: Channel) {
            self.channel = channel
        }
    }

    private var lock = NIOLock()
    private var actors: [EndpointIdentifier: ActorInfo] = [:]
    private var channels: [UInt32: ChannelInfo] = [:]
    private var stats: [String: UInt64] = [:]

    private let _nextChannelID = ManagedAtomic<UInt32>(1) // 0 reserved for local endpoints
    var nextChannelID: UInt32 { _nextChannelID.loadThenWrappingIncrement(ordering: .relaxed) }

    private let _nextInstanceID = ManagedAtomic<UInt32>(1)
    private var nextInstanceID: UInt32 { _nextInstanceID.loadThenWrappingIncrement(ordering: .relaxed) }

    var discoveryManager: DiscoveryManager
    private var syncCallManager: SyncCallManager
    var compressionMode: CompressionMode

    public typealias ServiceFilter = (NodeService) -> Bool
    public typealias ServiceFactory = (DistributedSystem) throws -> any ServiceEndpoint

    enum ChannelOrFactory {
        case channel(UInt32, Channel)
        case factory(ServiceFactory)
    }

    typealias ConnectionHandler = (UUID, ConsulServiceDiscovery.Instance, ChannelOrFactory) -> Void

    @TaskLocal
    private static var actorID: ActorID? // supposed to be private, but need to make it internal for tests

    public convenience init(
        systemName: String,
        addressTag: String? = nil,
        compressionMode: CompressionMode = .disabled,
        logLevel: Logger.Level = .info
    ) {
        self.init(name: systemName, addressTag: addressTag, compressionMode: compressionMode, logLevel: logLevel)
    }

    public init(
        name systemName: String,
        addressTag: String? = nil,
        compressionMode: CompressionMode = .disabled,
        logLevel: Logger.Level = .info
    ) {
        var logger = Logger(label: "ds")
        logger.logLevel = logLevel
        let loggerBox = Box(logger)

        self.loggerBox = loggerBox
        self.systemName = systemName
        self.addressTag = addressTag
        eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        consul = Consul(logLevel: logLevel)
        consulServiceDiscovery = ConsulServiceDiscovery(consul)
        discoveryManager = DiscoveryManager(loggerBox)
        syncCallManager = SyncCallManager(loggerBox)
        self.compressionMode = compressionMode
    }

    deinit {
        logger.debug("deinit")
    }

    func makeServiceEndpoint(_ channelID: UInt32) -> EndpointIdentifier {
        let instanceID = (nextInstanceID << 1)
        return EndpointIdentifier.makeServiceEndpoint(channelID, instanceID)
    }

    func connectToProcessAt(_ address: SocketAddress) {
        logger.debug("connect to process @ \(address)")
        ClientBootstrap(group: eventLoopGroup)
            .channelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
            .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .channelInitializer { channel in
                let writeBufferWaterMark = ChannelOptions.Types.WriteBufferWaterMark(
                    low: Int(self.endpointQueueWarningSize/2),
                    high: Int(self.endpointQueueWarningSize))
                _ = channel.setOption(ChannelOptions.writeBufferWaterMark, value: writeBufferWaterMark)
                let pipeline = channel.pipeline
                let channelHandler = ChannelHandler(self.nextChannelID, self, address, self.endpointQueueWarningSize)
                return pipeline.addHandler(ChannelCounters(self), name: ChannelCounters.name).flatMap { _ in
                    pipeline.addHandler(ChannelHandshakeClient(self, channelHandler), name: ChannelHandshakeClient.name)
                }
            }
            .connect(to: address)
            .whenComplete { result in
                if case let .failure(error) = result {
                    self.connectionEstablishmentFailed(error, address)
                }
            }
    }

    private func sendCreateService(_ serviceName: String, _ serviceID: UUID, _ instanceID: UInt32, to channel: Channel) {
        let payloadSize =
            MemoryLayout<SessionMessage.RawValue>.size
                + ULEB128.size(UInt(serviceName.count))
                + serviceName.count
                + MemoryLayout<uuid_t>.size
                + ULEB128.size(instanceID)
        var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt32>.size + payloadSize)
        buffer.writeInteger(UInt32(payloadSize))
        buffer.writeInteger(SessionMessage.createServiceInstance.rawValue)
        buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in ULEB128.encode(UInt(serviceName.count), to: ptr.baseAddress!) }
        buffer.writeString(serviceName)
        buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in
            var uuid = serviceID.uuid
            withUnsafeBytes(of: &uuid) { ptr.copyMemory(from: $0) }
            return MemoryLayout<uuid_t>.size
        }
        buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in ULEB128.encode(instanceID, to: ptr.baseAddress!) }
        logger.debug("\(channel.addressDescription): send create \(serviceName) \(EndpointIdentifier.instanceIdentifierDescription(instanceID))")
        _ = channel.writeAndFlush(buffer, promise: nil)
    }

    private func sendSuspendEndpoint(_ endpointID: EndpointIdentifier, to channel: Channel) {
        let instanceID = endpointID.instanceID
        let payloadSize = MemoryLayout<SessionMessage.RawValue>.size + ULEB128.size(instanceID)
        var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt32>.size + payloadSize)
        buffer.writeInteger(UInt32(payloadSize))
        buffer.writeInteger(SessionMessage.suspendEndpoint.rawValue)
        buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in ULEB128.encode(instanceID, to: ptr.baseAddress!) }
        logger.debug("\(channel.addressDescription): send suspend endpoint \(endpointID)")
        _ = channel.writeAndFlush(buffer, promise: nil)
    }

    private func sendResumeEndpoint(_ endpointID: EndpointIdentifier, to channel: Channel) {
        let instanceID = endpointID.instanceID
        let payloadSize = MemoryLayout<SessionMessage.RawValue>.size + ULEB128.size(instanceID)
        var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt32>.size + payloadSize)
        buffer.writeInteger(UInt32(payloadSize))
        buffer.writeInteger(SessionMessage.resumeEndpoint.rawValue)
        buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in ULEB128.encode(instanceID, to: ptr.baseAddress!) }
        logger.debug("\(channel.addressDescription): send resume endpoint \(endpointID)")
        _ = channel.writeAndFlush(buffer, promise: nil)
    }

    func setChannel(_ channelID: UInt32, _ channel: Channel, forProcessAt address: SocketAddress?) {
        lock.withLockVoid {
            self.channels[channelID] = ChannelInfo(channel)
        }
        if let address {
            discoveryManager.setChannel(channelID, channel, forProcessAt: address)
        }
        if Self.pingInterval > TimeAmount.zero {
            sendPing(to: channelID, channel, with: eventLoopGroup.next())
        }
    }

    func channelInactive(_ channelID: UInt32, _ address: SocketAddress?) {
        // It is better to remove the process from the discovery manager first.
        // Otherwise the connection state listener called at the end of inbound task
        // can try to initiate a new connection to service of the same type,
        // and it will be found created but with invalid endpoint.
        let reconnect = if let address {
            discoveryManager.channelInactive(address)
        } else {
            false
        }

        let (streamContinuations, endpointContinuations, pendingSyncCalls) = lock.withLock {
            var streamContinuations = [AsyncStream<InvocationEnvelope>.Continuation]()
            var endpointContinuations = [CheckedContinuation<Void, Error>]()
            var actors: [EndpointIdentifier] = []
            for (actorID, actorInfo) in self.actors {
                if actorID.channelID == channelID {
                    switch actorInfo {
                    case let .clientForRemoteService(inbound),
                         let .serviceForRemoteClient(inbound):
                        streamContinuations.append(inbound.continuation)
                    case let .remoteClient(outbound),
                         let .remoteService(outbound):
                        actors.append(actorID)
                        if outbound.suspended {
                            endpointContinuations.append(contentsOf: outbound.continuations)
                        } else {
                            assert(outbound.continuations.isEmpty)
                        }
                    default:
                        break
                    }
                }
            }

            for actorID in actors {
                self.actors.removeValue(forKey: actorID)
            }

            if let channelInfo = self.channels.removeValue(forKey: channelID) {
                return (streamContinuations, endpointContinuations, channelInfo.pendingSyncCalls)
            } else {
                return (streamContinuations, endpointContinuations, [])
            }
        }

        for continuation in streamContinuations {
            continuation.finish()
        }

        for continuation in endpointContinuations {
            continuation.resume(throwing: DistributedSystemErrors.connectionLost)
        }

        if !pendingSyncCalls.isEmpty {
            syncCallManager.resumeWithConnectionLoss(pendingSyncCalls)
        }

        if let address, reconnect {
            connectToProcessAt(address)
        }
    }

    private func connectionEstablishmentFailed(_ error: Error, _ address: SocketAddress) {
        discoveryManager.connectionEstablishmentFailed(address)
        logger.info("failed to connect to process @ \(address): \(error)")
    }

    private func addressForService(_ service: NodeService) -> SocketAddress? {
        let serviceAddress: String
        if let addressTag {
            guard let taggedAddresses = service.taggedAddresses else {
                logger.warning("skip service \(service.serviceID), missing '\(NodeService.CodingKeys.taggedAddresses)'")
                return nil
            }
            guard let str = taggedAddresses[addressTag] else {
                logger.warning("skip service \(service.serviceID), missing address for tag \(addressTag)")
                return nil
            }
            serviceAddress = str
        } else {
            if let str = service.serviceAddress, !str.isEmpty {
                serviceAddress = str
            } else if let str = service.address {
                serviceAddress = str
            } else {
                logger.warning("skip service \(service.serviceID), missing address")
                return nil
            }
        }

        guard let servicePort = service.servicePort else {
            logger.warning("skip service \(service.serviceID), missing '\(NodeService.CodingKeys.servicePort)'")
            return nil
        }

        do {
            return try SocketAddress(ipAddress: serviceAddress, port: servicePort)
        } catch {
            logger.error("\(error)")
            return nil
        }
    }

    public func makeCancellationToken() -> CancellationToken {
        return CancellationToken(self)
    }

    /// To be used to connect to multiple services of the same type.
    /// Service will be discovered with using a discovery system (consul by default).
    /// - Parameters:
    ///     - serviceEndpointType - type of the service endpoint
    ///     - serviceFilter: user can filter out services and create a distributed actors only to needed
    ///     - clientFactory: a closure creating a client side endpoint instance
    ///     - serviceHandler: a clusure getting an instance of the service endpoint and a service where the endpoint is connected to
    /// - Returns: false, if cancellation token was cancelled before the call
    ///
    @discardableResult public func connectToServices<S: ServiceEndpoint, C>(
        _ serviceEndpointType: S.Type,
        withFilter serviceFilter: @escaping ServiceFilter,
        clientFactory: ((DistributedSystem, ConsulServiceDiscovery.Instance) -> C)? = nil,
        serviceHandler: @escaping (S, ConsulServiceDiscovery.Instance) -> Void,
        cancellationToken: CancellationToken? = nil
    ) -> Bool
        where S.ID == EndpointIdentifier, S.ActorSystem == DistributedSystem {
        let serviceName = S.serviceName
        logger.debug("connectTo: \(serviceName)")
        let connectionHandler = { (serviceID: UUID, service: ConsulServiceDiscovery.Instance, channelOrFactory: ChannelOrFactory) -> Void in
            let serviceEndpointID = {
                switch channelOrFactory {
                case let .channel(channelID, channel):
                    // remote service
                    let serviceEndpointID = self.lock.withLock {
                        let serviceEndpointID = self.makeServiceEndpoint(channelID)
                        self.actors[serviceEndpointID] = .remoteService(.init())
                        return serviceEndpointID
                    }
                    self.sendCreateService(serviceName, serviceID, serviceEndpointID.instanceID, to: channel)
                    return serviceEndpointID
                case let .factory(factory):
                    let serviceEndpointID = self.makeServiceEndpoint(0)
                    self.lock.withLockVoid {
                        assert(self.actors[serviceEndpointID] == nil)
                        self.actors[serviceEndpointID] = .localService(factory)
                    }
                    return serviceEndpointID
                }
            }()

            if let clientFactory {
                let clientEndpointID = serviceEndpointID.makeClientEndpoint()
                Self.$actorID.withValue(clientEndpointID) {
                    _ = clientFactory(self, service)
                }
            }

            do {
                let serviceEndpoint = try S.resolve(id: serviceEndpointID, using: self)
                serviceHandler(serviceEndpoint, service)
            } catch {
                self.logger.error("\(error) (\(#file):\(#line))")
            }
        }

        let cancellationToken = cancellationToken ?? makeCancellationToken()
        let result = discoveryManager.discoverService(serviceName, serviceFilter, connectionHandler, cancellationToken)
        switch result {
        case .cancelled:
            return false
        case let .started(discover, addresses):
            if discover {
                let lastServicesBox = NIOLockedValueBox<[NodeService]>([])
                _ = consulServiceDiscovery.subscribe(
                    to: serviceName,
                    onNext: { result in
                        switch result {
                        case let .success(services):
                            var lastServices = lastServicesBox.withLockedValue { exchange(&$0, with: []) }
                            for service in services {
                                self.logger.trace("Found service \(service)")

                                guard let serviceSystemName = service.serviceMeta?[ServiceMetadata.systemName.rawValue] else {
                                    self.logger.debug("service \(serviceName)/\(service.serviceID) has no '\(ServiceMetadata.systemName)' in the metadata")
                                    continue
                                }

                                guard serviceSystemName == self.systemName else {
                                    self.logger.debug("skip service \(serviceName)/\(service.serviceID), different system")
                                    continue
                                }

                                guard let address = self.addressForService(service) else {
                                    continue
                                }

                                guard let serviceID = UUID(uuidString: service.serviceID) else {
                                    self.logger.debug("skip service \(serviceName)/\(service.serviceID), invalid service identifier")
                                    continue
                                }

                                if let idx = lastServices.firstIndex(where: { $0.serviceID == service.serviceID }) {
                                    lastServices.remove(at: idx)
                                }

                                let connect = self.discoveryManager.setAddress(address, for: serviceName, serviceID, service)
                                self.logger.debug("setAddress \(address) for \(serviceName)/\(service.serviceID), connect=\(connect)")
                                if connect {
                                    self.connectToProcessAt(address)
                                }
                            }

                            for service in lastServices {
                                if let serviceID = UUID(uuidString: service.serviceID) {
                                    self.discoveryManager.removeService(serviceName, serviceID)
                                }
                            }

                            lastServicesBox.withLockedValue { $0 = services }

                        case let .failure(error):
                            self.logger.debug("\(error)")
                        }
                    },
                    onComplete: { _ in
                        self.logger.debug("onComplete")
                    }
                )
            }

            for address in addresses {
                connectToProcessAt(address)
            }

            return true
        }
    }

    /// To be used to connect to a single service of the particular type.
    /// Function returns the service endpoint instance after connection to service is established and service endpoint is ready for use.
    /// - Parameters:
    ///     - serviceEndpointType - type of the service endpoint
    ///     - serviceFilter: user can filter out services and create a distributed actors only to needed
    ///     - clientFactory: a closure creating a client side endpoint instance
    ///     - serviceHandler: a clusure getting an instance of the service endpoint and a service where the endpoint is connected to
    /// - Returns: service endpoint
    public func connectToService<S: ServiceEndpoint, C>(
        _ serviceEndpointType: S.Type,
        withFilter serviceFilter: @escaping ServiceFilter,
        clientFactory: ((DistributedSystem) -> C)?,
        serviceHandler: ((S, ConsulServiceDiscovery.Instance) -> Void)? = nil,
        deadline: DispatchTime? = nil
    ) async throws -> S where S.ID == EndpointIdentifier, S.ActorSystem == DistributedSystem {
        let monitor = NIOLockedValueBox<(cancelled: Bool, continuation: CheckedContinuation<S, Error>?)>((false, nil))
        let cancellationToken = self.makeCancellationToken()
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in

                let taskCancelled = monitor.withLockedValue {
                    if $0.cancelled {
                        return true
                    } else {
                        $0.continuation = continuation
                        return false
                    }
                }

                if taskCancelled {
                    continuation.resume(throwing: DistributedSystemErrors.cancelled("\(S.self)"))
                    return
                }

                if let deadline {
                    let eventLoop = self.eventLoopGroup.next()
                    eventLoop.scheduleTask(deadline: NIODeadline.uptimeNanoseconds(deadline.uptimeNanoseconds)) {
                        let cancelled = cancellationToken.cancel()
                        if cancelled {
                            let continuation = monitor.withLockedValue { $0.continuation.take() }
                            continuation?.resume(throwing: DistributedSystemErrors.serviceDiscoveryTimeout(S.serviceName))
                        }
                    }
                }

                _ = self.connectToServices(
                    S.self,
                    withFilter: serviceFilter,
                    clientFactory: { actorSystem, _ in clientFactory?(actorSystem) },
                    serviceHandler: { serviceEndpoint, service in
                        let cancelled = cancellationToken.cancel()
                        if cancelled {
                            if let serviceHandler {
                                serviceHandler(serviceEndpoint, service)
                            }
                            let continuation = monitor.withLockedValue { $0.continuation.take() }
                            continuation?.resume(returning: serviceEndpoint)
                        }
                    },
                    cancellationToken: cancellationToken
                )
            }
        } onCancel: {
            let continuation = monitor.withLockedValue {
                assert(!$0.cancelled)
                $0.cancelled = true
                return $0.continuation.take()
            }

            _ = cancellationToken.cancel()
            continuation?.resume(throwing: DistributedSystemErrors.cancelled("\(S.self)"))
        }
    }

    /// To be used to connect to a single service of the particular type.
    /// Function returns the service endpoint instance after connection to service is established and service endpoint is ready for use.
    /// - Parameters:
    ///     - serviceEndpointType - type of the service endpoint
    ///     - serviceFilter: user can filter out services and create a distributed actors only to needed
    ///     - serviceHandler: a clusure getting an instance of the service endpoint and a service where the endpoint is connected to
    ///
    public func connectToService<S: ServiceEndpoint>(
         _ serviceEndpointType: S.Type,
         withFilter serviceFilter: @escaping ServiceFilter,
         serviceHandler: ((S, ConsulServiceDiscovery.Instance) -> Void)? = nil,
         deadline: DispatchTime? = nil
    ) async throws -> S where S.ID == EndpointIdentifier, S.ActorSystem == DistributedSystem {
        let clientFactory: ((DistributedSystem) -> Any)? = nil
        return try await connectToService(serviceEndpointType,
                                          withFilter: serviceFilter,
                                          clientFactory: clientFactory,
                                          serviceHandler: serviceHandler,
                                          deadline: deadline)
    }

    private func cancel(_ token: CancellationToken) -> Bool {
        return self.discoveryManager.cancel(token)
    }

    func addService(
        _ serviceName: String,
        _ metadata: [String: String],
        _ factory: @escaping ServiceFactory
    ) -> (serviceID: UUID, updateHealthStatus: Bool) {
        let serviceID = UUID()
        let service = NodeService(serviceID: "\(serviceID)", serviceMeta: metadata, serviceName: serviceName)
        let updateHealthStatus = discoveryManager.addService(serviceName, serviceID, service, factory)
        return (serviceID, updateHealthStatus)
    }

    func updateMetadata(_ metadata: [String: String], forService serviceID: UUID) throws -> (String, [String: String]) {
        try discoveryManager.updateMetadata(metadata, forService: serviceID)
    }

    func removeService(_ serviceID: UUID) -> Int? {
        discoveryManager.removeLocalService(serviceID)
    }

    func getLocalServices() -> ([NodeService], Int) {
        discoveryManager.getLocalServices()
    }

    private func sendPing(to channelID: UInt32, _ channel: Channel, with eventLoop: EventLoop) {
        let payloadSize = MemoryLayout<SessionMessage.RawValue>.size
        var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt32>.size + payloadSize)
        buffer.writeInteger(UInt32(payloadSize))
        buffer.writeInteger(SessionMessage.ping.rawValue)
        logger.trace("\(channel.addressDescription): send ping")
        let promise: EventLoopPromise<Void> = eventLoop.makePromise()
        promise.futureResult.whenSuccess {
            eventLoop.scheduleTask(in: Self.pingInterval) {
                self.sendPing(to: channelID, channel, with: eventLoop)
            }
        }
        _ = channel.writeAndFlush(buffer, promise: promise)

        typealias Res = (
            state: ConnectionState,
            actors: [(EndpointIdentifier, AsyncStream<InvocationEnvelope>.Continuation, ManagedAtomic<UInt64>)]
        )

        let res: Res? = lock.withLock {
            guard var channelInfo = self.channels[channelID] else { return nil }
            var res: Res? = nil
            if channelInfo.bytesReceived == channelInfo.bytesReceivedCheckpoint {
                channelInfo.bytesReceivedTimeouts += 1
                if channelInfo.bytesReceivedTimeouts == 2 {
                    logger.warning("\(channel.debugDescription): session timeout")
                    res = (.stale, actorsForChannelLocked(channelID))
                }
            } else {
                channelInfo.bytesReceivedCheckpoint = channelInfo.bytesReceived
                if channelInfo.bytesReceivedTimeouts > 1 {
                    logger.warning("\(channel.debugDescription): session recovered after \(channelInfo.bytesReceivedTimeouts * Self.pingInterval)")
                    res = (.active, actorsForChannelLocked(channelID))
                }
                channelInfo.bytesReceivedTimeouts = 0
            }
            self.channels[channelID] = channelInfo
            return res
        }

        if let res {
            let stateSize = MemoryLayout<ConnectionState.RawValue>.size
            let bufferSize = ULEB128.size(UInt(stateSize)) + stateSize
            var buffer = ByteBufferAllocator().buffer(capacity: bufferSize)
            buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in ULEB128.encode(UInt(stateSize), to: ptr.baseAddress!) }
            _ = res.state.rawValue.withUnsafeBytesSerialization { bytes in buffer.writeBytes(bytes) }

            let envelope = InvocationEnvelope(0, "", [], buffer)
            for entry in res.actors {
                dispatchInvocation(envelope, for: entry.0, channel, entry.1, entry.2)
            }
        }
    }

    private func actorsForChannelLocked(_ channelID: UInt32) -> [(EndpointIdentifier, AsyncStream<InvocationEnvelope>.Continuation, ManagedAtomic<UInt64>)] {
        // lock supposed to be locked by the caller
        var actors = [(EndpointIdentifier, AsyncStream<InvocationEnvelope>.Continuation, ManagedAtomic<UInt64>)]()
        for (actorID, actorInfo) in self.actors {
            if actorID.channelID == channelID {
                switch actorInfo {
                case let .clientForRemoteService(inbound),
                     let .serviceForRemoteClient(inbound):
                    actors.append((actorID, inbound.continuation, inbound.queueState))
                default:
                    break
                }
            }
        }
        return actors
    }

    /// Service lifecycle start
    public func start() throws {
        if type(of: self) == DistributedSystem.self {
            logger.info("starting system '\(systemName)' (compression mode = \(compressionMode))")
        }
    }

    /// Service lifecycle stop
    public func stop() {
        logger.info("stop")

        let services = discoveryManager.stop()
        for serviceID in services {
            do {
                try consul.agent.deregisterServiceID("\(serviceID)").wait()
            } catch {
                // Deregistration can fail if the <future result> returned from the addService()
                // was not waited and the stop() called before the service was actually registered
                // in the Consul. Not an issue probably.
                // As a side effect will have a dead service in the consul for some time.
                logger.debug("\(error)")
            }
        }

        // consul should be stopped before the event loops,
        // otherwise consul could trigger some events which
        // will be scheduled to the stopped event loop
        do {
            try consul.syncShutdown()
        } catch {
            logger.error("\(error) (\(#file):\(#line))")
        }

        do {
            try eventLoopGroup.syncShutdownGracefully()
        } catch {
            logger.error("\(error) (\(#file):\(#line))")
        }

        let continuations = lock.withLock {
            var continuations: [AsyncStream<InvocationEnvelope>.Continuation] = []
            for (_, actorInfo) in self.actors {
                switch actorInfo {
                case let .clientForRemoteService(cfrs):
                    continuations.append(cfrs.continuation)
                case let .serviceForRemoteClient(sfrc):
                    continuations.append(sfrc.continuation)
                default:
                    break
                }
            }
            return continuations
        }

        continuations.forEach { $0.finish() }

        logger.info("stopped")

        let str = lock.withLock {
            var strs = [String]()
            if let bytesSent = stats[ChannelCounters.keyBytesSent] {
                strs.append("bytes_sent=\(bytesSent)")
                if let bytesCompressed = stats[ChannelCompressionOutboundHandler.statsKey] {
                    strs.append(", bytes_compressed=\(bytesCompressed), ")
                    strs.append("outbound space saving=\(Self.calculateSpaceSaving(dataSize: bytesCompressed, compressedSize: bytesSent))%")
                }
            }
            if let bytesReceived = stats[ChannelCounters.keyBytesReceived] {
                strs.append(", bytes_received=\(bytesReceived)")
                if let bytesDecompressed = stats[ChannelCompressionInboundHandler.statsKey] {
                    strs.append(", bytes_decompressed=\(bytesDecompressed), ")
                    strs.append("inbound space saving=\(Self.calculateSpaceSaving(dataSize: bytesDecompressed, compressedSize: bytesReceived))%")
                }
            }
            return strs.joined()
        }
        logger.info("stats: \(str)")
    }

    private static func calculateSpaceSaving(dataSize: UInt64, compressedSize: UInt64) -> Int {
        Int((1.0 - Double(compressedSize)/Double(dataSize)) * 100)
    }

    public func assignID<Actor>(_: Actor.Type) -> EndpointIdentifier
        where Actor: DistributedActor, EndpointIdentifier == Actor.ID {
        if let actorID = Self.$actorID.get() {
            logger.debug("assign<\(Actor.self)>: \(actorID)")
            return actorID
        } else {
            fatalError("Internal error: missing actor identifier")
        }
    }

    public func resolve<Actor>(id: EndpointIdentifier, as _: Actor.Type) throws -> Actor?
        where Actor: DistributedActor,
        EndpointIdentifier == Actor.ID {
        logger.debug("resolve<\(Actor.self)>: \(id)")

        if Actor.self is any ServiceEndpoint.Type {
            lock.lock()
            guard let actorInfo = self.actors[id] else {
                throw DistributedSystemErrors.unknownActor(id)
            }
            if case .remoteService = actorInfo {
                lock.unlock()
                return nil
            } else if case let .localService(serviceFactory) = actorInfo {
                lock.unlock()
                // We can't call the factory under the lock,
                // because after actor instantiation actorReady<> will be called,
                // and it will try to take the lock again
                let actor = try Self.$actorID.withValue(id) { try serviceFactory(self) }
                guard let actor = actor as? Actor else {
                    fatalError("internal error: factory for \(id) created not a \(Actor.self)")
                }
                return actor
            } else {
                fatalError("internal error: invalid actor state \(actorInfo)")
            }
        } else if Actor.self is any ClientEndpoint.Type {
            return try lock.withLock {
                guard let actorInfo = self.actors[id] else {
                    throw DistributedSystemErrors.unknownActor(id)
                }
                switch actorInfo {
                case let .newClient(actor):
                    // self.actors.removeValue(forKey: id)
                    guard let actor = actor as? Actor else {
                        fatalError("Internal error: invalid actor \(id) type")
                    }
                    return actor
                case .remoteClient:
                    return nil
                default:
                    fatalError("Internal error: invalid actor state \(actorInfo)")
                }
            }
        } else {
            fatalError("internal error: unsuppoted actor type '\(Actor.self)")
        }
    }

    public func actorReady<Actor>(_ actor: Actor) where Actor: DistributedActor, EndpointIdentifier == Actor.ID {
        logger.debug("actorReady<\(Actor.self)>: \(actor.id)")
        if Actor.self is any ServiceEndpoint.Type {
            lock.withLockVoid {
                if let actorInfo = self.actors[actor.id] {
                    if case .localService = actorInfo {
                        self.actors.removeValue(forKey: actor.id)
                    } else {
                        fatalError("internal error: invalid actor \(actor.id) state: \(actorInfo)")
                    }
                } else {
                    assert(actor.id.channelID != 0)
                }
                let clientEndpointID = actor.id.makeClientEndpoint()
                if let actorInfo = self.actors[clientEndpointID] {
                    switch actorInfo {
                    case .newClient:
                        break // do nothing here
                    case .remoteClient:
                        let (stream, continuation) = AsyncStream.makeStream(of: InvocationEnvelope.self)
                        let queueSize = ManagedAtomic<UInt64>(0)
                        let sfrc = ActorInfo.Inbound(continuation, queueSize)
                        self.actors[actor.id] = .serviceForRemoteClient(sfrc)
                        if let channelInfo = self.channels[actor.id.channelID] {
                            let channelAddressDescription = channelInfo.channel.addressDescription
                            Task { await self.streamTask(stream, actor.id, actor, channelInfo.channel, channelAddressDescription, queueSize) }
                        } else {
                            logger.error("internal error: channel \(actor.id.channelID) not registered")
                        }
                    default:
                        fatalError("Internal error: unexpected actor state \(actorInfo)")
                    }
                }
            }
        } else if Actor.self is any ClientEndpoint.Type {
            lock.withLockVoid {
                let channelID = actor.id.channelID
                if channelID == 0 {
                    // local client
                    if self.actors.updateValue(.newClient(actor), forKey: actor.id) != nil {
                        fatalError("Internal error: duplicate actor id \(actor.id)")
                    }
                } else if let channelInfo = self.channels[channelID] {
                    let (stream, continuation) = AsyncStream.makeStream(of: InvocationEnvelope.self)
                    let queueSize = ManagedAtomic<UInt64>(0)
                    let cfrs = ActorInfo.Inbound(continuation, queueSize)
                    self.actors[actor.id] = .clientForRemoteService(cfrs)
                    let channelAddressDescription = channelInfo.channel.addressDescription
                    Task { await self.streamTask(stream, actor.id, actor, channelInfo.channel, channelAddressDescription, queueSize) }
                } else {
                    // seems like connection was closed right after establishment
                    Task {
                        do {
                            if let connectionStateHandler = actor as? (any ConnectionStateHandler) {
                                try await connectionStateHandler.handleConnectionState(.closed)
                            } else {
                                logger.error("unexpected actor type '\(type(of: actor))'")
                            }
                        } catch {
                            logger.error("\(error)")
                        }
                    }
                }
            }
        } else {
            fatalError("internal error: unsuppoted actor type '\(Actor.self)")
        }
    }

    public func resignID(_ id: EndpointIdentifier) {
        logger.debug("resign: \(id)")
        // In some cases ActorInfo holds a reference to the distributed actor instance,
        // in a case if it is a last reference then it will be released,
        // and it will entail a nested call to the resignID() for that actor,
        // and we will have a recursive lock.
        // Returning ActorInfo from under the lock let as release an actor instance outside the lock.
        _ = lock.withLock { () -> ActorInfo? in
            if let actorInfo = self.actors[id] {
                switch actorInfo {
                case .remoteClient:
                    fatalError("internal error: unexpected actor state")
                default:
                    self.actors.removeValue(forKey: id)
                    return actorInfo
                }
            }
            return nil
        }

        if (id.channelID == 0) && ((id.instanceID & EndpointIdentifier.serviceFlag) != 0) {
            let clientEndpointID = id.makeClientEndpoint()
            _ = lock.withLock { () -> ActorInfo? in
                if let actorInfo = self.actors[clientEndpointID] {
                    if case .newClient = actorInfo {
                        self.actors.removeValue(forKey: clientEndpointID)
                        return actorInfo
                    } else {
                        logger.error("internal error: unexpected actor state for \(clientEndpointID)")
                    }
                }
                return nil
            }
        }
    }

    public func makeInvocationEncoder() -> RemoteCallEncoder {
        RemoteCallEncoder()
    }

    public func remoteCall<Actor, Err, Res>(
        on actor: Actor,
        target: RemoteCallTarget,
        invocation: inout InvocationEncoder,
        throwing _: Err.Type,
        returning _: Res.Type
    ) async throws -> Res where Actor: DistributedActor, Actor.ID == ActorID, Err: Error, Res: Transferable {
        let callID = syncCallManager.nextCallID
        try await remoteCall(on: actor, target, &invocation, callID)
        let res: Res = try await syncCallManager.waitResult(callID)
        lock.withLockVoid {
            let channelID = actor.id.channelID
            if var channelInfo = self.channels[channelID] {
                channelInfo.pendingSyncCalls.remove(callID)
                self.channels[channelID] = channelInfo
            } else {
                logger.error("internal error: channel \(channelID) not found")
            }
        }
        return res
    }

    private func remoteCall<Actor>(
        on actor: Actor,
        _ target: RemoteCallTarget,
        _ invocation: inout InvocationEncoder,
        _ callID: UInt64
    ) async throws where Actor: DistributedActor, Actor.ID == ActorID {
        let (channel, targetFuncsIndex, suspended) = try lock.withLock {
            guard let actorInfo = self.actors[actor.id] else {
                throw DistributedSystemErrors.error("Actor \(Actor.self)/\(actor.id) not registered")
            }
            let suspended = switch actorInfo {
            case let .remoteClient(rcs), let .remoteService(rcs):
                rcs.suspended
            default:
                fatalError("internal error: invalid actor state \(actor.id): \(actorInfo)")
            }

            let channelID = actor.id.channelID
            guard var channelInfo = self.channels[channelID] else {
                logger.error("internal error: no connection for actor \(Actor.self)/\(actor.id)")
                throw DistributedSystemErrors.error("No connection for actor \(Actor.self)/\(actor.id)")
            }

            if callID != 0 {
                channelInfo.pendingSyncCalls.insert(callID)
                self.channels[channelID] = channelInfo
                logger.trace("add sync call \(callID) for \(channelID)")
            }

            return (channelInfo.channel, channelInfo.targetFuncsIndex, suspended)
        }

        if suspended {
            try await withCheckedThrowingContinuation { continuation in
                lock.withLockVoid {
                    let actorInfo = self.actors[actor.id]
                    switch actorInfo {
                    case var .remoteClient(remoteClient):
                        if remoteClient.suspended {
                            remoteClient.continuations.append(continuation)
                            self.actors[actor.id] = .remoteClient(remoteClient)
                        } else {
                            continuation.resume()
                        }
                    case var .remoteService(remoteService):
                        if remoteService.suspended {
                            remoteService.continuations.append(continuation)
                            self.actors[actor.id] = .remoteService(remoteService)
                        } else {
                            continuation.resume()
                        }
                    default:
                        if let actorInfo {
                            logger.error("Internal error: invalid actor state \(actorInfo)")
                        }
                        continuation.resume()
                    }
                }
            }
        }

        let payloadSize =
            MemoryLayout<SessionMessage.RawValue>.size
                + ULEB128.size(actor.id.instanceID)
                + InvocationEnvelope.wireSize(callID, invocation.genericSubstitutions, invocation.arguments, target)
        // Even if we carefully calculated the capacity of the desired buffer and know it to the nearest byte,
        // swift-nio still allocates a buffer with a storage capacity rounded up to nearest power of 2...
        // Weird...
        var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt32>.size + payloadSize)
        buffer.writeInteger(UInt32(payloadSize))
        buffer.writeInteger(SessionMessage.invocationEnvelope.rawValue)
        buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: 0) { ptr in ULEB128.encode(actor.id.instanceID, to: ptr.baseAddress!) }
        let targetOffset = InvocationEnvelope.encode(callID, invocation.genericSubstitutions, &invocation.arguments, to: &buffer)
        let targetIdentifier = target.identifier

        channel.eventLoop.execute { [buffer] in
            // target functions table must be modified serially
            // in the event loop tied to the channel to be sure it will not
            // be modified by another task sending data to the same channel
            var buffer = buffer
            if let targetId = targetFuncsIndex.getId(for: targetIdentifier) {
                InvocationEnvelope.setTargetId(targetId, in: &buffer, at: targetOffset)
            } else {
                InvocationEnvelope.setTargetId(targetIdentifier, in: &buffer, at: targetOffset)
            }
            let messageSize = buffer.readableBytes - MemoryLayout<UInt32>.size
            buffer.setInteger(UInt32(messageSize), at: buffer.readerIndex)

            self.logger.trace("\(channel.addressDescription): send \(buffer.readableBytes) bytes for \(actor.id)")
            channel.writeAndFlush(buffer, promise: nil)
        }
    }

    public func remoteCallVoid<Actor, Err>(
        on actor: Actor,
        target: RemoteCallTarget,
        invocation: inout InvocationEncoder,
        throwing _: Err.Type
    ) async throws
        where Actor: DistributedActor,
        Actor.ID == ActorID,
        Err: Error {
        try await remoteCall(on: actor, target, &invocation, 0)
    }

    private static func readULEB128<T: UnsignedInteger>(from buffer: inout ByteBuffer, as: T.Type) throws -> T {
        let (size, value) = try buffer.withUnsafeReadableBytes { ptr in try ULEB128.decode(ptr, as: T.self) }
        buffer.moveReaderIndex(forwardBy: size)
        return value
    }

    func channelRead(_ channelID: UInt32, _ channel: Channel, _ buffer: inout ByteBuffer, _ targetFuncs: inout [String]) {
        let bytesReceived = buffer.readableBytes
        guard buffer.readInteger(as: UInt32.self) != nil, // skip message size
              let rawMessageType = buffer.readInteger(as: SessionMessage.RawValue.self)
        else {
            logger.error("\(channel.addressDescription): invalid message received (\(bytesReceived)), closing connection")
            channel.close(promise: nil)
            return
        }

        guard let messageType = SessionMessage(rawValue: rawMessageType) else {
            logger.error("\(channel.addressDescription): unknown message \(rawMessageType), closing connection")
            channel.close(promise: nil)
            return
        }

        do {
            switch messageType {
            case .createServiceInstance:
                let serviceNameLength = try Self.readULEB128(from: &buffer, as: UInt16.self)
                guard let serviceName = buffer.readString(length: Int(serviceNameLength)) else {
                    throw DecodeError.error("failed to decode service name")
                }
                let serviceID = buffer.withUnsafeReadableBytes { ptr in
                    var uuid = uuid_t(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
                    withUnsafeMutableBytes(of: &uuid) { _ = ptr.copyBytes(to: $0) }
                    return UUID(uuid: uuid)
                }
                buffer.moveReaderIndex(forwardBy: MemoryLayout<uuid_t>.size)
                let instanceID = try Self.readULEB128(from: &buffer, as: EndpointIdentifier.InstanceIdentifier.self)
                createService(serviceName, serviceID, instanceID, for: channelID, channel)
            case .invocationEnvelope:
                let instanceID = try Self.readULEB128(from: &buffer, as: EndpointIdentifier.InstanceIdentifier.self)
                let endpointID = EndpointIdentifier(channelID, instanceID)
                let envelope = try InvocationEnvelope(from: &buffer, &targetFuncs)
                try invokeLocalCall(envelope, for: endpointID, channel)
            case .invocationResult:
                try syncCallManager.handleResult(&buffer)
            case .suspendEndpoint:
                let instanceID = try Self.readULEB128(from: &buffer, as: EndpointIdentifier.InstanceIdentifier.self)
                let endpointID = EndpointIdentifier(channelID, instanceID)
                suspendEndpoint(endpointID)
            case .resumeEndpoint:
                let instanceID = try Self.readULEB128(from: &buffer, as: EndpointIdentifier.InstanceIdentifier.self)
                let endpointID = EndpointIdentifier(channelID, instanceID)
                resumeEndpoint(endpointID)
            case .ping:
                logger.trace("\(channel.addressDescription): ping, send pong")
                let payloadSize = MemoryLayout<SessionMessage.RawValue>.size
                var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt32>.size + payloadSize)
                buffer.writeInteger(UInt32(payloadSize))
                buffer.writeInteger(SessionMessage.pong.rawValue)
                _ = channel.writeAndFlush(buffer, promise: nil)
            case .pong:
                logger.trace("\(channel.addressDescription): pong")
            }
        } catch {
            logger.error("\(channel.addressDescription): \(error), close connection")
            _ = channel.close()
        }

        lock.withLockVoid {
            let channelInfo = self.channels[channelID]
            if var channelInfo {
                channelInfo.bytesReceived += bytesReceived
                self.channels[channelID] = channelInfo
            } else {
                logger.error("internal error: channel \(channelID) not registered")
            }
        }
    }

    private func createService(
        _ serviceName: String,
        _ serviceID: UUID,
        _ instanceID: EndpointIdentifier.InstanceIdentifier,
        for channelID: UInt32,
        _ channel: Channel
    ) {
        let serviceFactory = discoveryManager.factoryFor(serviceName, serviceID)
        guard let serviceFactory else {
            logger.error("\(channel.addressDescription): service \(serviceName) for \(instanceID) not registered")
            return
        }

        let clientEndpointID: EndpointIdentifier? = lock.withLock {
            let serviceEndpointID = EndpointIdentifier(channelID, instanceID)
            let clientEndpointID = serviceEndpointID.makeClientEndpoint()
            if self.actors[clientEndpointID] != nil {
                logger.error("\(channel.addressDescription): duplicate endpoint identifier \(clientEndpointID)")
                return nil
            }
            self.actors[clientEndpointID] = .remoteClient(.init())
            return clientEndpointID
        }

        if let clientEndpointID {
            let serviceEndpointID = clientEndpointID.makeServiceEndpoint()
            logger.debug("\(channel.addressDescription): create service \(serviceName) \(serviceEndpointID)")
            do {
                let _ = try Self.$actorID.withValue(serviceEndpointID) { try serviceFactory(self) }
            } catch {
                lock.withLockVoid {
                    self.actors.removeValue(forKey: clientEndpointID)
                }
                logger.error("\(channel.addressDescription): \(error)")
            }
        }
    }

    private func suspendEndpoint(_ endpointID: EndpointIdentifier) {
        lock.withLockVoid {
            let actorInfo = self.actors[endpointID]
            guard let actorInfo else {
                logger.error("internal error: suspend unknown enpoint \(endpointID)")
                return
            }
            switch actorInfo {
            case var .remoteClient(remoteClient):
                if remoteClient.suspended {
                    logger.error("internal error: endpoint \(endpointID) already suspended")
                } else {
                    if !remoteClient.continuations.isEmpty {
                        logger.error("internal error: not suspended endpoint \(endpointID) has \(remoteClient.continuations.count) continuations")
                    }
                    logger.debug("suspend endpoint \(endpointID)")
                    remoteClient.suspended = true
                    self.actors[endpointID] = .remoteClient(remoteClient)
                }
            case var .remoteService(remoteService):
                if remoteService.suspended {
                    logger.error("internal error: endpoint \(endpointID) already suspended")
                } else {
                    if !remoteService.continuations.isEmpty {
                        logger.error("internal error: not suspended endpoint \(endpointID) has \(remoteService.continuations.count) continuations")
                    }
                    logger.debug("suspend endpoint \(endpointID)")
                    remoteService.suspended = true
                    self.actors[endpointID] = .remoteService(remoteService)
                }
            default:
                logger.error("internal error: suspend endpoint \(endpointID) \(actorInfo)")
            }
        }
    }

    private func resumeEndpoint(_ endpointID: EndpointIdentifier) {
        let continuations: [CheckedContinuation<Void, Error>]? = lock.withLock {
            let actorInfo = self.actors[endpointID]
            guard let actorInfo else {
                logger.error("internal error: endpoint \(endpointID) not registered")
                return nil
            }

            var continuations = [CheckedContinuation<Void, Error>]()
            switch actorInfo {
            case var .remoteClient(remoteClient):
                if remoteClient.suspended {
                    continuations = remoteClient.continuations
                    remoteClient.suspended = false
                    remoteClient.continuations = []
                    self.actors[endpointID] = .remoteClient(remoteClient)
                } else {
                    logger.error("internal error: endpoint \(endpointID) not suspended")
                }
            case var .remoteService(remoteService):
                if remoteService.suspended {
                    continuations = remoteService.continuations
                    remoteService.suspended = false
                    remoteService.continuations = []
                    self.actors[endpointID] = .remoteService(remoteService)
                } else {
                    logger.error("internal error: endpoint \(endpointID) not suspended")
                }
            default:
                logger.error("internal error: resume endpoint \(endpointID) \(actorInfo)")
            }

            return continuations
        }

        if let continuations {
            logger.debug("resume \(continuations.count) continuations for \(endpointID)")
            for continuation in continuations {
                continuation.resume()
            }
        }
    }

    private func streamTask(_ stream: AsyncStream<InvocationEnvelope>,
                            _ endpointID: EndpointIdentifier,
                            _ actor: any DistributedActor,
                            _ channel: Channel,
                            _ channelAddressDescription: String,
                            _ queueState: ManagedAtomic<UInt64>) async {
        logger.debug("\(channelAddressDescription): start stream task for \(actor.id)")

        for await envelope in stream {
            var decoder = RemoteCallDecoder(envelope: envelope)
            do {
                if envelope.targetFunc.isEmpty {
                    let state: ConnectionState = try decoder.decodeNextArgument()
                    if let connectionStateHandler = actor as? (any ConnectionStateHandler) {
                        try await connectionStateHandler.handleConnectionState(state)
                    } else {
                        logger.error("unexpected actor type '\(type(of: actor))'")
                    }
                } else {
                    let resultHandler = ResultHandler(loggerBox, envelope.targetFunc)
                    try await executeDistributedTarget(on: actor,
                                                       target: RemoteCallTarget(envelope.targetFunc),
                                                       invocationDecoder: &decoder,
                                                       handler: resultHandler)
                    if resultHandler.hasResult {
                        try resultHandler.sendTo(channel, for: envelope.callID)
                    } else {
                        if envelope.callID != 0 {
                            logger.error("internal error: missing result")
                        }
                    }
                }
            } catch {
                // TODO: should we propagate throw back? or close connection?
                logger.error("Target function error: \(error)")
            }
            decoder._releaseArguments()

            let sizeMask = ((UInt64(1) << Self.endpointQueueSizeBits) - 1)
            var oldState = queueState.load(ordering: .relaxed)
            while true {
                let oldSize = (oldState & sizeMask)
                assert(oldSize >= envelope.size)
                var newState = (oldState - envelope.size)
                let newSize = (newState & sizeMask)
                if (newSize < endpointQueueLowWatermark) && (oldSize >= endpointQueueLowWatermark) && ((oldState & Self.endpointQueueSuspendIndicator) != 0) {
                    newState -= Self.endpointQueueSuspendIndicator
                }
                let (exchanged, original) = queueState.compareExchange(expected: oldState, desired: newState, ordering: .relaxed)
                if exchanged {
                    if ((oldState & Self.endpointQueueSuspendIndicator) != 0) && ((newState & Self.endpointQueueSuspendIndicator) == 0) {
                        sendResumeEndpoint(endpointID, to: channel)
                    }
                    break
                }
                oldState = original
            }
        }

        do {
            if let connectionStateHandler = actor as? (any ConnectionStateHandler) {
                try await connectionStateHandler.handleConnectionState(.closed)
            } else {
                logger.error("unexpected actor type '\(type(of: actor))'")
            }
        } catch {
            logger.error("\(error)")
        }

        logger.debug("\(channelAddressDescription): streamTask for \(actor.id) done")
    }

    private func invokeLocalCall(_ envelope: InvocationEnvelope, for endpointID: EndpointIdentifier, _ channel: Channel) throws {
        let ret = try lock.withLock {
            let actorInfo = self.actors[endpointID]
            guard let actorInfo else {
                throw DistributedSystemErrors.unknownActor(endpointID)
            }

            switch actorInfo {
            case let .newClient(actor):
                var continuation: AsyncStream<InvocationEnvelope>.Continuation?
                let stream = AsyncStream(InvocationEnvelope.self, bufferingPolicy: .unbounded) { continuation = $0 }
                guard let continuation else { fatalError("Internal error: continuation unexpectedly nil") }
                let queueState = ManagedAtomic<UInt64>(0)
                let inbound = ActorInfo.Inbound(continuation, queueState)
                self.actors[endpointID] = .clientForRemoteService(inbound)
                let channelAddressDescription = channel.addressDescription
                Task { await self.streamTask(stream, endpointID, actor, channel, channelAddressDescription, queueState) }
                return (continuation, queueState)
            case let .serviceForRemoteClient(inbound):
                return (inbound.continuation, inbound.queueState)
            case let .clientForRemoteService(inbound):
                return (inbound.continuation, inbound.queueState)
            default:
                throw DistributedSystemErrors.invalidActorState("\(actorInfo)")
            }
        }
        dispatchInvocation(envelope, for: endpointID, channel, ret.0, ret.1)
    }

    private func dispatchInvocation(_ envelope: InvocationEnvelope,
                                    for endpointID: EndpointIdentifier,
                                    _ channel: Channel,
                                    _ continuation: AsyncStream<InvocationEnvelope>.Continuation,
                                    _ queueState: ManagedAtomic<UInt64>) {
        let sizeBits = Self.endpointQueueSizeBits
        let sizeMask = ((UInt64(1) << sizeBits) - 1)
        var oldState = queueState.load(ordering: .relaxed)
        while true {
            var newState = (oldState + envelope.size)
            let oldSize = (oldState & sizeMask)
            let newSize = (newState & sizeMask)
            let warningSize = (endpointQueueWarningSize << ((oldState >> sizeBits) & 0x7F))
            var logWarning = false
            if (oldSize < warningSize) && (newSize >= warningSize) {
                newState += (UInt64(1) << sizeBits)
                logWarning = true
            }
            var suspendEndpoint = false
            if (oldSize < endpointQueueHighWatermark) && (newSize >= endpointQueueHighWatermark) && ((oldState & Self.endpointQueueSuspendIndicator) == 0) {
                newState |= Self.endpointQueueSuspendIndicator
                suspendEndpoint = true
            }
            let (exchanged, original) = queueState.compareExchange(expected: oldState, desired: newState, ordering: .relaxed)
            if exchanged {
                if logWarning {
                    // The warning threshold multiplied by 2 each time is breached,
                    // so we will have warnings for 1, 2, 4, 8, etc megabytes
                    logger.debug("Input queue size for \(endpointID) reached \(newSize) bytes")
                }
                if suspendEndpoint {
                    sendSuspendEndpoint(endpointID, to: channel)
                }
                break
            }
            oldState = original
        }

        continuation.yield(envelope)
    }

    func closeConnectionFor(_ endpointID: EndpointIdentifier) throws {
        let channel = try lock.withLock {
            if let channelInfo = self.channels[endpointID.channelID] {
                return channelInfo.channel
            } else {
                throw DistributedSystemErrors.error("No connection for \(endpointID)")
            }
        }
        logger.debug("close connection for \(endpointID)")
        channel.close(promise: nil)
    }

    public func getBytesSent() async throws -> UInt64 {
        let (bytesSent, futures) = lock.withLock {
            let bytesSent = self.stats[ChannelCounters.keyBytesSent, default: 0]
            var futures = [EventLoopFuture<UInt64>]()
            futures.reserveCapacity(self.channels.count)
            for (_, channelInfo) in self.channels {
                let future = channelInfo.channel.pipeline.context(handlerType: ChannelCounters.self).flatMap { context in
                    let eventLoop = context.eventLoop
                    if let counter = context.handler as? ChannelCounters {
                        let counterStats = counter.stats
                        let channelBytesSent = counterStats[ChannelCounters.keyBytesSent, default: 0]
                        return eventLoop.makeSucceededFuture(channelBytesSent)
                    } else {
                        return eventLoop.makeSucceededFuture(0)
                    }
                }
                futures.append(future)
            }
            return (bytesSent, futures)
        }
        let eventLoop = eventLoopGroup.next()
        let future = EventLoopFuture.whenAllComplete(futures, on: eventLoop).flatMap { results in
            let ret = results.reduce(bytesSent, { nextResult, futureResult in
                var nextResult = nextResult
                if case let .success(bytesSent) = futureResult {
                    nextResult += bytesSent
                }
                return nextResult
            })
            return eventLoop.makeSucceededFuture(ret)
        }
        return try await future.get()
    }

    func incrementStats(_ stats: [String: UInt64]) {
        lock.withLock {
            for (key, value) in stats {
                self.stats[key, default: 0] += value
            }
        }
    }
}
