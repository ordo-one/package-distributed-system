// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

// swiftformat:disable opaqueGenericParameters

import PackageConcurrencyHelpers
import ConsulServiceDiscovery
import Dispatch
import Distributed
import DistributedSystemConformance
import Frostflake
import Helpers
import Logging
import Atomics
internal import NIOCore
internal import NIOPosix

extension Channel {
    var debugDescription: String {
        remoteAddress?.description ?? "?"
    }
}

public class DistributedSystem: DistributedActorSystem, @unchecked Sendable {
    public typealias ActorID = EndpointIdentifier
    public typealias InvocationEncoder = RemoteCallEncoder
    public typealias InvocationDecoder = RemoteCallDecoder
    public typealias ResultHandler = RemoteCallResultHandler
    public typealias SerializationRequirement = Transferable

    public struct ModuleIdentifier: Hashable, Codable, CustomStringConvertible {
        public let rawValue: FrostflakeIdentifier

        public var description: String {
            String(describing: rawValue)
        }

        public init(_ rawValue: FrostflakeIdentifier) {
            self.rawValue = rawValue
        }

        public init?(_ rawValue: FrostflakeIdentifier?) {
            if let rawValue {
                self.rawValue = rawValue
            } else {
                return nil
            }
        }

        public init?(_ str: String) {
            if let rawValue = FrostflakeIdentifier(str) {
                self.init(rawValue)
            } else {
                return nil
            }
        }
    }

    public final class CancellationToken: Hashable {
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

    public var loggerBox = Box<Logger>(Logger(label: "ds"))
    public var logger: Logger { loggerBox.value }

    private static let endpointQueueWarningSize: UInt64 = (1024 * 1024)
    private static let endpointQueueHighWatermark: UInt64 = (10 * 1024 * 1024)
    private static let endpointQueueLowWatermark: UInt64 = (1024 * 1024)
    private static let endpointQueueSuspendIndicator: UInt64 = 0x8000_0000_0000_0000
    private static let endpointQueueSizeBits: Int = (UInt64.bitWidth - 8)

    // TODO: replace with configuration
    private static let pingInterval = TimeAmount.seconds(2)
    private static let serviceDiscoveryTimeout = TimeAmount.seconds(5)

    enum SessionMessage: UInt16 {
        case createServiceInstance = 0
        case invocationEnvelope = 1
        case invocationResult = 2
        case duplicatedEndpointIdentifier = 3
        case suspendEndpoint = 4
        case resumeEndpoint = 5
    }

    public let systemName: String

    let eventLoopGroup: EventLoopGroup
    let consul: Consul
    // Some services expect service discovery to be available from distributed system
    public let consulServiceDiscovery: ConsulServiceDiscovery

    private enum ActorInfo {
        struct Outbound {
            let channel: Channel
            var suspended: Bool
            var continuations: [CheckedContinuation<Void, Error>]

            init(_ channel: Channel, _ suspended: Bool = false, _ continuations: [CheckedContinuation<Void, Error>] = []) {
                self.channel = channel
                self.suspended = suspended
                self.continuations = continuations
            }
        }

        case newClient(any DistributedActor) // retain the client actor instance while it will not be linked to local or remote service
        case serviceForLocalClient(any DistributedActor) // retain the client actor until the related service actor will not be resigned
        case remoteClient(Outbound)
        case remoteService(Outbound)
        case clientForRemoteService(Channel, AsyncStream<InvocationEnvelope>.Continuation, ManagedAtomic<UInt64>)
        case serviceForRemoteClient(Channel, AsyncStream<InvocationEnvelope>.Continuation, ManagedAtomic<UInt64>)
    }

    struct ChannelInfo {
        var bytesReceived = 0
        var bytesReceivedCheckpoint = 0
        var bytesReceivedTimeouts = 0
        var connectionLossHandlers = [ConnectionLossHandler]()
        var pendingSyncCalls = Set<UInt64>()
    }

    private var lock = Lock()
    private var actors: [EndpointIdentifier: ActorInfo] = [:]
    private var channels: [UnsafeRawPointer: ChannelInfo] = [:]

    private var discoveryManager: DiscoveryManager
    private var syncCallManager: SyncCallManager

    public var duplicatedEndpointIdentifierHook: (EndpointIdentifier) -> Void
    var duplicatedEndpointIdentifier: EndpointIdentifier?

    public typealias ConnectionLossHandler = () -> Void
    public typealias ServiceFilter = (NodeService) -> Bool
    public typealias ServiceFactory = (DistributedSystem) throws -> (any DistributedActor, ConnectionLossHandler?)
    typealias ConnectionHandler = (ServiceIdentifier, ConsulServiceDiscovery.Instance, Channel?) -> ConnectionLossHandler?

    @TaskLocal
    private static var actorID: ActorID? // supposed to be private, but need to make it internal for tests

    public convenience init(systemName: String) {
        self.init(name: systemName)
    }

    public init(name: String) {
        systemName = name
        eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        consul = Consul()
        consulServiceDiscovery = ConsulServiceDiscovery(consul)
        discoveryManager = DiscoveryManager(loggerBox)
        syncCallManager = SyncCallManager(loggerBox)
        duplicatedEndpointIdentifierHook = Self.duplicatedEndpointIdentifier

        // loggerBox.value.logLevel = .debug
        // Consul.logger.logLevel = .debug
    }

    deinit {
        logger.debug("deinit")
    }

    private static func duplicatedEndpointIdentifier(_ endpointID: EndpointIdentifier) {
        fatalError("duplicated endpoint identifier \(endpointID)")
    }

    func connectToProcessAt(_ address: SocketAddress) {
        logger.debug("connect to process @ \(address)")
        ClientBootstrap(group: eventLoopGroup)
            .channelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
            .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .channelInitializer { channel in
                channel.pipeline.addHandler(ByteToMessageHandler(StreamDecoder(self.loggerBox))).flatMap { _ in
                    channel.pipeline.addHandler(ChannelHandler(self, address))
                }
            }
            .connect(to: address)
            .whenComplete { result in
                if case let .failure(error) = result {
                    self.connectionEstablishmentFailed(error, address)
                }
            }
    }

    private func sendCreateService(_ serviceName: String, _ endpointID: EndpointIdentifier, to channel: Channel) {
        let payloadSize =
            MemoryLayout<SessionMessage.RawValue>.size
                + MemoryLayout<UInt16>.size
                + serviceName.count
                + endpointID.wireSize
        var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt32>.size + payloadSize)
        buffer.writeInteger(UInt32(payloadSize))
        buffer.writeInteger(SessionMessage.createServiceInstance.rawValue)
        buffer.writeInteger(UInt16(serviceName.count))
        buffer.writeString(serviceName)
        endpointID.encode(to: &buffer)
        logger.debug("\(channel.remoteAddressDescription): send create \(serviceName) \(endpointID)")
        _ = channel.writeAndFlush(buffer, promise: nil)
    }

    private func sendSuspendEndpoint(_ endpointID: EndpointIdentifier, to channel: Channel) {
        let payloadSize = MemoryLayout<SessionMessage.RawValue>.size + endpointID.wireSize
        var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt32>.size + payloadSize)
        buffer.writeInteger(UInt32(payloadSize))
        buffer.writeInteger(SessionMessage.suspendEndpoint.rawValue)
        endpointID.encode(to: &buffer)
        logger.debug("\(channel.remoteAddressDescription): send suspend endpoint \(endpointID)")
        _ = channel.writeAndFlush(buffer, promise: nil)
    }

    private func sendResumeEndpoint(_ endpointID: EndpointIdentifier, to channel: Channel) {
        let payloadSize = MemoryLayout<SessionMessage.RawValue>.size + endpointID.wireSize
        var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt32>.size + payloadSize)
        buffer.writeInteger(UInt32(payloadSize))
        buffer.writeInteger(SessionMessage.resumeEndpoint.rawValue)
        endpointID.encode(to: &buffer)
        logger.debug("\(channel.remoteAddressDescription): send resume endpoint \(endpointID)")
        _ = channel.writeAndFlush(buffer, promise: nil)
    }

    func setChannel(_ channel: Channel, forProcessAt address: SocketAddress) {
        discoveryManager.setChannel(channel, forProcessAt: address)
    }

    static func ptr(for channel: some Channel) -> UnsafeRawPointer {
        UnsafeRawPointer(Unmanaged.passUnretained(channel).toOpaque())
    }

    func channelInactive(_ channel: Channel) {
        discoveryManager.channelInactive(channel)

        let (streamContinuations, endpointContinuations, connectionLossHandlers, pendingSyncCalls) = lock.withLock {
            var streamContinuations = [AsyncStream<InvocationEnvelope>.Continuation]()
            var endpointContinuations = [CheckedContinuation<Void, Error>]()
            var actors: [EndpointIdentifier] = []
            for (actorID, actorInfo) in self.actors {
                switch actorInfo {
                case let .clientForRemoteService(actorChannel, continuation, _):
                    if actorChannel === channel {
                        streamContinuations.append(continuation)
                    }
                case let .serviceForRemoteClient(actorChannel, continuation, _):
                    if actorChannel === channel {
                        streamContinuations.append(continuation)
                    }
                case let .remoteClient(rcs), let .remoteService(rcs):
                    if rcs.channel === channel {
                        actors.append(actorID)
                        if rcs.suspended {
                            endpointContinuations.append(contentsOf: rcs.continuations)
                        } else {
                            assert(rcs.continuations.isEmpty)
                        }
                    }
                default:
                    break
                }
            }

            for actorID in actors {
                self.actors.removeValue(forKey: actorID)
            }

            let ptr = Self.ptr(for: channel)
            if let channelInfo = self.channels.removeValue(forKey: ptr) {
                return (streamContinuations, endpointContinuations, channelInfo.connectionLossHandlers, channelInfo.pendingSyncCalls)
            } else {
                return (streamContinuations, endpointContinuations, [], [])
            }
        }

        for continuation in streamContinuations {
            continuation.finish()
        }

        for continuation in endpointContinuations {
            continuation.resume(throwing: DistributedSystemErrors.connectionLost)
        }

        for connectionLossHandler in connectionLossHandlers {
            connectionLossHandler()
        }

        if !pendingSyncCalls.isEmpty {
            syncCallManager.resumeWithConnectionLoss(pendingSyncCalls)
        }
    }

    private func connectionEstablishmentFailed(_ error: Error, _ address: SocketAddress) {
        logger.debug("failed to connect to process @ \(address): \(error)")
        discoveryManager.connectionEstablishmentFailed(address)
    }

    private func addressForService(_ service: NodeService) -> SocketAddress? {
        let serviceAddress = service.serviceAddress.flatMap { $0.isEmpty ? nil : $0 } ?? service.address

        guard let serviceAddress else {
            logger.debug("skip service \(service.serviceID), missing address")
            return nil
        }

        guard let servicePort = service.servicePort else {
            logger.debug("skip service \(service.serviceID), missing '\(NodeService.CodingKeys.servicePort)'")
            return nil
        }

        do {
            let address = try SocketAddress(ipAddress: serviceAddress, port: servicePort)
            return address
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
        serviceHandler: @escaping (S, ConsulServiceDiscovery.Instance) -> ConnectionLossHandler?,
        cancellationToken: CancellationToken? = nil
    ) -> Bool
        where S.ID == EndpointIdentifier, S.ActorSystem == DistributedSystem {
        let serviceName = S.serviceName
        logger.debug("connectTo: \(serviceName)")

        let connectionHandler = { (serviceID: ServiceIdentifier, service: ConsulServiceDiscovery.Instance, channel: Channel?) -> ConnectionLossHandler? in
            let serviceEndpointID = self.duplicatedEndpointIdentifier.flatMap { (serviceEndpointType != PingServiceEndpoint.self) ? $0 : nil } ?? EndpointIdentifier(serviceID)

            if let channel {
                self.lock.withLockVoid {
                    self.actors[serviceEndpointID] = .remoteService(.init(channel))
                }
                self.sendCreateService(serviceName, serviceEndpointID, to: channel)
            } else {
                if serviceEndpointType == PingServiceEndpoint.self {
                    // do not connect to PingService in the same process
                    return nil
                }
            }

            if let clientFactory {
                let clientEndpointID = serviceEndpointID.makeClientEndpoint()
                Self.$actorID.withValue(clientEndpointID) {
                    _ = clientFactory(self, service)
                }
            }

            do {
                let serviceEndpoint = try S.resolve(id: serviceEndpointID, using: self)
                return serviceHandler(serviceEndpoint, service)
            } catch {
                self.logger.error("\(error) (\(#file):\(#line))")
                return nil
            }
        }

        let cancellationToken = cancellationToken ?? makeCancellationToken()
        let result = discoveryManager.discoverService(serviceName, serviceFilter, connectionHandler, cancellationToken)
        switch result {
        case .cancelled:
            return false
        case let .started(discover, addresses):
            if discover {
                _ = consulServiceDiscovery.subscribe(
                    to: serviceName,
                    onNext: { result in
                        switch result {
                        case let .success(services):
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

                                guard let serviceID = ServiceIdentifier(service.serviceID) else {
                                    self.logger.debug("skip service \(serviceName)/\(service.serviceID), invalid service identifier")
                                    continue
                                }

                                let connect = self.discoveryManager.setAddress(address, for: serviceName, serviceID, service)
                                self.logger.debug("setAddress \(address) for \(serviceName)/\(service.serviceID), connect=\(connect)")
                                if connect {
                                    self.connectToProcessAt(address)
                                }
                            }
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

    private final class Monitor<T> {
        let lock: Lock = Lock()
        var cancelled = false
        var value: T?

        func setValue(_ value: T) -> Bool {
            lock.withLock {
                if cancelled {
                    return false
                } else {
                    self.value = value
                    return true
                }
            }
        }

        func cancel() -> T? {
            lock.withLock {
                assert(cancelled == false)
                cancelled = true
                return value
            }
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
        serviceHandler: ((S, ConsulServiceDiscovery.Instance) -> ConnectionLossHandler?)? = nil,
        deadline: DispatchTime? = nil) async throws -> S
        where S.ID == EndpointIdentifier, S.ActorSystem == DistributedSystem {
        let monitor = Monitor<CheckedContinuation<S, Error>>()
        let cancellationToken = self.makeCancellationToken()
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in

                if let deadline {
                    let eventLoop = self.eventLoopGroup.next()
                    eventLoop.scheduleTask(deadline: NIODeadline.uptimeNanoseconds(deadline.uptimeNanoseconds)) {
                        let cancelled = cancellationToken.cancel()
                        if cancelled {
                            continuation.resume(throwing: DistributedSystemErrors.serviceDiscoveryTimeout(S.serviceName))
                        }
                    }
                }

                guard monitor.setValue(continuation) else {
                    continuation.resume(throwing: DistributedSystemErrors.error("Cancelled"))
                    return
                }

                let started = self.connectToServices(
                    S.self,
                    withFilter: serviceFilter,
                    clientFactory: { actorSystem, _ in clientFactory?(actorSystem) },
                    serviceHandler: { serviceEndpoint, service in
                        let cancelled = cancellationToken.cancel()
                        if cancelled {
                            let connectionLossHandler: ConnectionLossHandler?
                            if let serviceHandler {
                                connectionLossHandler = serviceHandler(serviceEndpoint, service)
                            } else {
                                connectionLossHandler = nil
                            }
                            continuation.resume(returning: serviceEndpoint)
                            return connectionLossHandler
                        } else {
                            return nil
                        }
                    },
                    cancellationToken: cancellationToken
                )

                if !started {
                    continuation.resume(throwing: DistributedSystemErrors.error("Cancelled"))
                }
            }
        } onCancel: {
            let continuation = monitor.cancel()
            if let continuation {
                if cancellationToken.cancel() {
                    continuation.resume(throwing: DistributedSystemErrors.error("Cancelled"))
                }
            }
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
         serviceHandler: ((S, ConsulServiceDiscovery.Instance) -> ConnectionLossHandler?)? = nil,
         deadline: DispatchTime? = nil) async throws -> S
        where S.ID == EndpointIdentifier, S.ActorSystem == DistributedSystem {
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

    func addService(_ serviceName: String,
                    _ metadata: [String: String],
                    _ factory: @escaping ServiceFactory) -> ServiceIdentifier {
        let serviceID = ServiceIdentifier(FrostflakeIdentifier())
        let service = NodeService(serviceID: "\(serviceID)", serviceMeta: metadata, serviceName: serviceName)
        let updateHealthStatus = discoveryManager.addService(serviceName, serviceID, service, factory)
        if updateHealthStatus {
            let eventLoop = eventLoopGroup.next()
            eventLoop.scheduleTask(in: DistributedSystemServer.healthStatusUpdateInterval) {
                self.updateHealthStatus(with: eventLoop)
            }
        }
        return serviceID
    }

    func addService(_ serviceName: String,
                    _ metadata: [String: String],
                    _ factory: @escaping (DistributedSystem) throws -> any DistributedActor) -> ServiceIdentifier {
        addService(serviceName, metadata) { actorSystem in
            try (factory(actorSystem), nil)
        }
    }

    private func updateHealthStatus(with eventLoop: EventLoop) {
        let services = discoveryManager.getLocalServices()
        logger.trace("update health status for \(services.count) services")

        for serviceID in services {
            let checkID = "service:\(serviceID)"
            _ = consul.agent.check(checkID, status: .passing)
        }

        eventLoop.scheduleTask(in: DistributedSystemServer.healthStatusUpdateInterval) {
            self.updateHealthStatus(with: eventLoop)
        }
    }

    private func sendPing(to endpoint: PingEndpoint, id: EndpointIdentifier, with eventLoop: EventLoop) {
        Task {
            do {
                try await endpoint.ping()

                self.lock.withLockVoid {
                    guard let actorInfo = self.actors[id] else { return }

                    var channel: Channel?
                    switch actorInfo {
                    case let .remoteClient(rcs), let .remoteService(rcs):
                        channel = rcs.channel
                    case let .serviceForRemoteClient(clientChannel, _, _):
                        channel = clientChannel
                    case let .clientForRemoteService(serviceChannel, _, _):
                        channel = serviceChannel
                    default:
                        logger.error("Internal error: unexpected actor state \(actorInfo) for \(id)")
                    }

                    if let channel {
                        let ptr = Self.ptr(for: channel)
                        let channelInfo = self.channels[ptr]
                        if var channelInfo {
                            if channelInfo.bytesReceived == channelInfo.bytesReceivedCheckpoint {
                                channelInfo.bytesReceivedTimeouts += 1
                                if channelInfo.bytesReceivedTimeouts == 2 {
                                    logger.warning("session \(channel.debugDescription) timeout")
                                }
                            } else {
                                channelInfo.bytesReceivedCheckpoint = channelInfo.bytesReceived
                                if channelInfo.bytesReceivedTimeouts > 1 {
                                    logger.warning("session \(channel.debugDescription) recovered after \(channelInfo.bytesReceivedTimeouts * Self.pingInterval)")
                                }
                                channelInfo.bytesReceivedTimeouts = 0
                            }
                            self.channels[ptr] = channelInfo
                        }
                    }
                }

                eventLoop.scheduleTask(in: Self.pingInterval) {
                    self.sendPing(to: endpoint, id: id, with: eventLoop)
                }
            } catch {
                // seems connection to service lost
                logger.debug("\(error)")
            }
        }
    }

    func sendPing(to endpoint: PingEndpoint, id: EndpointIdentifier) {
        let eventLoop = eventLoopGroup.next()
        sendPing(to: endpoint, id: id, with: eventLoop)
    }

    /// Service lifecycle start
    public func start() throws {
        logger.debug("starting system '\(systemName)'")

        let eventLoop = eventLoopGroup.next()
        connectToServices(
            PingServiceEndpoint.self,
            withFilter: { _ in true },
            clientFactory: { actorSystem, service in
                PingServiceClientEndpoint(actorSystem: actorSystem, ServiceIdentifier(service.serviceID))
            },
            serviceHandler: { serviceEndpoint, _ in
                self.sendPing(to: serviceEndpoint, id: serviceEndpoint.id, with: eventLoop)
                return nil
            }
        )
    }

    /// Service lifecycle stop
    public func stop() {
        logger.debug("stop")

        let services = discoveryManager.getLocalServices()
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

        // consul should be stopped before the even loops,
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
                case let .clientForRemoteService(_, continuation, _):
                    continuations.append(continuation)
                case let .serviceForRemoteClient(_, continuation, _):
                    continuations.append(continuation)
                default:
                    break
                }
            }
            return continuations
        }

        continuations.forEach { $0.finish() }

        logger.debug("stopped")
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
        if id.serviceID.rawValue == 0 {
            return try lock.withLock {
                if let actorInfo = self.actors[id] {
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
                        fatalError("Internal error: invalid actor state")
                    }
                } else {
                    throw DistributedSystemErrors.unknownActor
                }
            }
        } else {
            guard let actorType = Actor.self as? any ServiceEndpoint.Type else {
                fatalError("Invalid remote actor type \(Actor.self), should conform to ServiceEndpoint")
            }
            let serviceName = actorType.serviceName
            let serviceFactory = discoveryManager.factoryFor(serviceName, id.serviceID)
            if let serviceFactory {
                // service registered in the same distributed system instance
                // connection to the local service is never lost
                let (actor, _ /* connectionLossHandler */ ) = try Self.$actorID.withValue(id) { try serviceFactory(self) }
                guard let actor = actor as? Actor else {
                    fatalError("Factory \(serviceName)/\(id.serviceID) created not a \(Actor.self)")
                }
                return actor
            } else {
                /*
                let channel = lock.withLock {
                    guard let actorInfo = self.actors[id] else {
                        fatalError("Internal error: actor \(id) not registered")
                    }
                    if case let .remoteService(channel) = actorInfo {
                        return channel
                    } else {
                        fatalError("Internal error: invalid actor state \(actorInfo)")
                    }
                }
                sendCreateService(serviceName, id, to: channel)
                */
                return nil
            }
        }
    }

    public func actorReady<Actor>(_ actor: Actor) where Actor: DistributedActor, EndpointIdentifier == Actor.ID {
        logger.debug("actorReady<\(Actor.self)>: \(actor.id)")
        lock.withLockVoid {
            if actor.id.serviceID.rawValue == 0 {
                if self.actors.updateValue(.newClient(actor), forKey: actor.id) != nil {
                    fatalError("Internal error: duplicate actor id \(actor.id)")
                }
            } else {
                if self.actors[actor.id] != nil {
                    fatalError("Internal error: invalid actor state")
                }
                let clientEndpointID = actor.id.makeClientEndpoint()
                if let actorInfo = self.actors[clientEndpointID] {
                    switch actorInfo {
                    case .newClient:
                        break // do nothing here
                    case let .remoteClient(remoteClient):
                        var continuation: AsyncStream<InvocationEnvelope>.Continuation?
                        let stream = AsyncStream(InvocationEnvelope.self, bufferingPolicy: .unbounded) { continuation = $0 }
                        guard let continuation else { fatalError("Internal error: continuation unexpectedly nil") }
                        let queueSize = ManagedAtomic<UInt64>(0)
                        self.actors[actor.id] = .serviceForRemoteClient(remoteClient.channel, continuation, queueSize)
                        Task { await self.streamTask(stream, actor, remoteClient.channel, queueSize) }
                    default:
                        fatalError("Internal error: unexpected actor state \(actorInfo)")
                    }
                }
            }
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
                    fatalError("Internal error: unexpected actor state")
                default:
                    self.actors.removeValue(forKey: id)
                    return actorInfo
                }
            }
            return nil
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
    ) async throws -> Res
        where Actor: DistributedActor,
        Actor.ID == ActorID,
        Err: Error,
        Res: Transferable {
        let callID = syncCallManager.nextCallID
        try await remoteCall(on: actor, target, &invocation, callID)
        let res: Res = try await syncCallManager.waitResult(callID)
        lock.withLockVoid {
            guard let actorInfo = self.actors[actor.id] else {
                logger.error("Internal error: actor \(actor.id) not registered")
                return
            }
            let channel = switch actorInfo {
            case let .remoteClient(rcs), let .remoteService(rcs):
                rcs.channel
            default:
                fatalError("Internal error: invalid actor state \(actorInfo)")
            }
            let ptr = Self.ptr(for: channel)
            if var channelInfo = self.channels[ptr] {
                channelInfo.pendingSyncCalls.remove(callID)
                self.channels[ptr] = channelInfo
            } else {
                logger.error("Channel not found")
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
        let (channel, suspended): (Channel, Bool) = try lock.withLock {
            guard let actorInfo = self.actors[actor.id] else {
                throw DistributedSystemErrors.noConnectionForActor(actor.id)
            }
            let (channel, suspended) = switch actorInfo {
            case let .remoteClient(rcs), let .remoteService(rcs):
                (rcs.channel, rcs.suspended)
            default:
                fatalError("Internal error: invalid actor state \(actor.id): \(actorInfo)")
            }

            if callID != 0 {
                let ptr = Self.ptr(for: channel)
                self.channels[ptr, default: ChannelInfo()].pendingSyncCalls.insert(callID)
                logger.trace("add sync call \(callID) for \(ptr)")
            }

            return (channel, suspended)
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

        let payloadSize = MemoryLayout<SessionMessage.RawValue>.size + InvocationEnvelope.wireSize(actor.id, callID, target, invocation.genericSubstitutions, invocation.arguments)
        // Even if we carefully calculated the capacity of the desired buffer and know it to the nearest byte,
        // swift-nio still allocate a buffer with a storage capacity rounded up to nearest power of 2...
        // Weird...
        var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt32>.size + payloadSize)
        buffer.writeInteger(UInt32(payloadSize))
        buffer.writeInteger(SessionMessage.invocationEnvelope.rawValue)
        InvocationEnvelope.encode(actor.id, callID, target, invocation.genericSubstitutions, &invocation.arguments, to: &buffer)
        logger.trace("\(channel.remoteAddressDescription): send \(buffer.readableBytes) bytes for \(actor.id)")
        channel.writeAndFlush(buffer, promise: nil)
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

    func channelRead(_ channel: Channel, _ buffer: inout ByteBuffer) {
        let bytesReceived = buffer.readableBytes
        guard buffer.readInteger(as: UInt32.self) != nil, // skip message size
              let messageType = buffer.readInteger(as: SessionMessage.RawValue.self)
        else {
            logger.error("\(String(describing: channel.remoteAddress)): invalid message received")
            return
        }

        do {
            switch SessionMessage(rawValue: messageType) {
            case .createServiceInstance:
                guard let serviceNameLength = buffer.readInteger(as: UInt16.self),
                      let serviceName = buffer.readString(length: Int(serviceNameLength)) else {
                    logger.error("Invalid message from \(String(describing: channel.remoteAddress)), close connection.")
                    _ = channel.close()
                    return
                }
                let endpointID = try EndpointIdentifier(from: &buffer)
                createService(serviceName, endpointID, for: channel)
            case .invocationEnvelope:
                let envelope = try InvocationEnvelope(from: &buffer)
                invokeLocalCall(envelope: envelope, for: channel)
            case .invocationResult:
                try syncCallManager.handleResult(&buffer)
            case .duplicatedEndpointIdentifier:
                let endpointID = try EndpointIdentifier(from: &buffer)
                duplicatedEndpointIdentifierHook(endpointID)
            case .suspendEndpoint:
                let endpointID = try EndpointIdentifier(from: &buffer)
                suspendEndpoint(endpointID)
            case .resumeEndpoint:
                let endpointID = try EndpointIdentifier(from: &buffer)
                resumeEndpoint(endpointID)
            case .none:
                logger.error("\(channel.remoteAddressDescription): unexpected session message")
            }
        } catch {
            logger.error("\(channel.remoteAddressDescription): \(error)")
        }

        let ptr = Self.ptr(for: channel)
        lock.withLockVoid {
            self.channels[ptr, default: ChannelInfo()].bytesReceived += bytesReceived
        }
    }

    private func createService(_ serviceName: String, _ endpointID: EndpointIdentifier, for channel: Channel) {
        let serviceID = endpointID.serviceID
        let serviceFactory = discoveryManager.factoryFor(serviceName, serviceID)
        guard let serviceFactory else {
            logger.error("\(channel.remoteAddressDescription): service \(serviceName)/\(serviceID) not registered, close connection.")
            _ = channel.close()
            return
        }

        let clientEndpointID = endpointID.makeClientEndpoint()
        let duplicatedEndpointID = lock.withLock {
            if self.actors[clientEndpointID] != nil {
                return true
            } else {
                self.actors[clientEndpointID] = .remoteClient(.init(channel))
                return false
            }
        }

        if duplicatedEndpointID {
            logger.error("\(channel.remoteAddressDescription): duplicated endpoint identifier \(clientEndpointID)")
            let payloadSize = MemoryLayout<SessionMessage.RawValue>.size + clientEndpointID.wireSize
            var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt32>.size + payloadSize)
            buffer.writeInteger(UInt32(payloadSize))
            buffer.writeInteger(SessionMessage.duplicatedEndpointIdentifier.rawValue)
            clientEndpointID.encode(to: &buffer)
            _ = channel.writeAndFlush(buffer)
        } else {
            logger.debug("\(channel.remoteAddressDescription): create service \(serviceName) \(endpointID)")
            do {
                let (_, connectionLossHandler) = try Self.$actorID.withValue(endpointID) { try serviceFactory(self) }
                if let connectionLossHandler {
                    let ptr = Self.ptr(for: channel)
                    lock.withLockVoid {
                        self.channels[ptr, default: ChannelInfo()].connectionLossHandlers.append(connectionLossHandler)
                    }
                }
            } catch {
                lock.withLockVoid {
                    self.actors.removeValue(forKey: clientEndpointID)
                }
                logger.error("\(channel.remoteAddressDescription): \(error)")
            }
        }
    }

    private func suspendEndpoint(_ endpointID: EndpointIdentifier) {
        lock.withLockVoid {
            let actorInfo = self.actors[endpointID]
            switch actorInfo {
            case var .remoteClient(remoteClient):
                if remoteClient.suspended {
                    logger.error("Internal error: endpoint \(endpointID) already suspended")
                } else {
                    if !remoteClient.continuations.isEmpty {
                        logger.error("Internal error: not suspended endpoint \(endpointID) has \(remoteClient.continuations.count) continuations")
                    }
                    logger.debug("suspend enpoint \(endpointID)")
                    remoteClient.suspended = true
                    self.actors[endpointID] = .remoteClient(remoteClient)
                }
            case var .remoteService(remoteService):
                if remoteService.suspended {
                    logger.error("Internal error: endpoint \(endpointID) already suspended")
                } else {
                    if !remoteService.continuations.isEmpty {
                        logger.error("Internal error: not suspended endpoint \(endpointID) has \(remoteService.continuations.count) continuations")
                    }
                    logger.debug("resume endpoint \(endpointID)")
                    remoteService.suspended = true
                    self.actors[endpointID] = .remoteService(remoteService)
                }
            default:
                if let actorInfo {
                    logger.error("Internal error: suspend enpoint \(endpointID) \(actorInfo)")
                } else {
                    logger.error("Internal error: suspend unknown enpoint \(endpointID)")
                }
            }
        }
    }

    private func resumeEndpoint(_ endpointID: EndpointIdentifier) {
        let continuations = lock.withLock {
            var continuations = [CheckedContinuation<Void, Error>]()
            let actorInfo = self.actors[endpointID]
            switch actorInfo {
            case var .remoteClient(remoteClient):
                if remoteClient.suspended {
                    continuations = remoteClient.continuations
                    remoteClient.suspended = false
                    remoteClient.continuations = []
                    self.actors[endpointID] = .remoteClient(remoteClient)
                } else {
                    logger.error("Internal error: endpoint \(endpointID) not suspended")
                }
            case var .remoteService(remoteService):
                if remoteService.suspended {
                    continuations = remoteService.continuations
                    remoteService.suspended = false
                    remoteService.continuations = []
                    self.actors[endpointID] = .remoteService(remoteService)
                } else {
                    logger.error("Internal error: endpoint \(endpointID) not suspended")
                }
            default:
                if let actorInfo {
                    logger.error("Internal error: resume endpoint \(endpointID) \(actorInfo)")
                } else {
                    logger.error("Internal error: resume endpoint \(endpointID)")
                }
            }
            return continuations
        }

        logger.debug("resume \(continuations.count) continuations for \(endpointID)")

        for continuation in continuations {
            continuation.resume()
        }
    }

    private func streamTask(_ stream: AsyncStream<InvocationEnvelope>, _ actor: any DistributedActor, _ channel: Channel, _ queueState: ManagedAtomic<UInt64>) async {
        let remoteAddressDescription = channel.remoteAddressDescription
        logger.debug("\(remoteAddressDescription): start stream task for \(actor.id)")
        let resultHandler = ResultHandler()
        for await envelope in stream {
            var decoder = RemoteCallDecoder(envelope: envelope)
            do {
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
                if (newSize < Self.endpointQueueLowWatermark) && (oldSize >= Self.endpointQueueLowWatermark) && ((oldState & Self.endpointQueueSuspendIndicator) != 0) {
                    newState -= Self.endpointQueueSuspendIndicator
                }
                let (exchanged, original) = queueState.compareExchange(expected: oldState, desired: newState, ordering: .relaxed)
                if exchanged {
                    if ((oldState & Self.endpointQueueSuspendIndicator) != 0) && ((newState & Self.endpointQueueSuspendIndicator) == 0) {
                        sendResumeEndpoint(envelope.targetID, to: channel)
                    }
                    break
                }
                oldState = original
            }
        }
        logger.debug("\(remoteAddressDescription): streamTask for \(actor.id) done")
    }

    private func invokeLocalCall(envelope: InvocationEnvelope, for channel: Channel) {
        let targetID = envelope.targetID
        let res: (continuation: AsyncStream<InvocationEnvelope>.Continuation, queueState: ManagedAtomic<UInt64>)? = lock.withLock {
            let actorInfo = self.actors[targetID]
            guard let actorInfo else {
                logger.error("\(channel.remoteAddressDescription): actor \(targetID) not found")
                return nil
            }
            switch actorInfo {
            case let .newClient(actor):
                var continuation: AsyncStream<InvocationEnvelope>.Continuation?
                let stream = AsyncStream(InvocationEnvelope.self, bufferingPolicy: .unbounded) { continuation = $0 }
                guard let continuation else { fatalError("Internal error: continuation unexpectedly nil") }
                let queueState = ManagedAtomic<UInt64>(0)
                self.actors[targetID] = .clientForRemoteService(channel, continuation, queueState)
                Task { await self.streamTask(stream, actor, channel, queueState) }
                return (continuation, queueState)
            case let .serviceForRemoteClient(_, continuation, queueState):
                return (continuation, queueState)
            case let .clientForRemoteService(_, continuation, queueState):
                return (continuation, queueState)
            default:
                logger.error("\(channel.remoteAddressDescription): invalid actor state \(actorInfo)")
                return nil
            }
        }

        if let res {
            let sizeBits = Self.endpointQueueSizeBits
            let sizeMask = ((UInt64(1) << sizeBits) - 1)
            var oldState = res.queueState.load(ordering: .relaxed)
            while true {
                var newState = (oldState + envelope.size)
                let oldSize = (oldState & sizeMask)
                let newSize = (newState & sizeMask)
                let warningSize = (Self.endpointQueueWarningSize << ((oldState >> sizeBits) & 0x7F))
                var logWarning = false
                if (oldSize < warningSize) && (newSize >= warningSize) {
                    newState += (UInt64(1) << sizeBits)
                    logWarning = true
                }
                var suspendEndpoint = false
                if (oldSize < Self.endpointQueueHighWatermark) && (newSize >= Self.endpointQueueHighWatermark) && ((oldState & Self.endpointQueueSuspendIndicator) == 0) {
                    newState |= Self.endpointQueueSuspendIndicator
                    suspendEndpoint = true
                }
                let (exchanged, original) = res.queueState.compareExchange(expected: oldState, desired: newState, ordering: .relaxed)
                if exchanged {
                    if logWarning {
                        // The warning threshold multiplied by 2 each time is breached,
                        // so we will have warnings for 1, 2, 4, 8, etc megabytes
                        logger.debug("Input queue size for \(envelope.targetID) reached \(newSize) bytes")
                    }
                    if suspendEndpoint {
                        sendSuspendEndpoint(targetID, to: channel)
                    }
                    break
                }
                oldState = original
            }

            res.continuation.yield(envelope)
        }
    }

    func closeConnectionFor(_ endpointID: EndpointIdentifier) throws {
        let channel = try lock.withLock {
            switch self.actors[endpointID] {
            case let .remoteClient(rcs), let .remoteService(rcs):
                rcs.channel
            default:
                throw DistributedSystemErrors.noConnectionForActor(endpointID)
            }
        }
        logger.debug("close connection for \(Self.ptr(for: channel))")
        channel.close(promise: nil)
    }
}
