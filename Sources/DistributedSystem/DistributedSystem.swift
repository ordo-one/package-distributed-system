// swiftformat:disable opaqueGenericParameters

import PackageConcurrencyHelpers
import ConsulServiceDiscovery
import Distributed
import DistributedSystemConformance
import Frostflake
import Helpers
import Logging
import Atomics
import NIOCore
import NIOPosix

public class DistributedSystem: DistributedActorSystem, @unchecked Sendable {
    public typealias ActorID = EndpointIdentifier
    public typealias InvocationEncoder = RemoteCallEncoder
    public typealias InvocationDecoder = RemoteCallDecoder
    public typealias ResultHandler = RemoteCallResultHandler
    public typealias SerializationRequirement = Transferable

    public struct InstanceIdentifier: Hashable, Codable, CustomStringConvertible {
        public let rawValue: FrostflakeIdentifier

        public var description: String {
            String(describing: rawValue)
        }

        public var wireSize: Int {
            MemoryLayout<FrostflakeIdentifier>.size
        }

        public init(_ rawValue: FrostflakeIdentifier = Frostflake.generate()) {
            self.rawValue = rawValue
        }

        public init(from buffer: inout ByteBuffer) throws {
            if let rawValue = buffer.readInteger(as: FrostflakeIdentifier.self) {
                self.rawValue = rawValue
            } else {
                throw DistributedSystemErrors.error("Failed to decode InstanceIdentifier")
            }
        }

        public func encode(to buffer: inout ByteBuffer) {
            buffer.writeInteger(rawValue)
        }
    }

    public struct ServiceIdentifier: Hashable, Codable, CustomStringConvertible {
        public let rawValue: FrostflakeIdentifier

        public var description: String {
            String(describing: rawValue)
        }

        public var wireSize: Int {
            MemoryLayout<FrostflakeIdentifier>.size
        }

        public init(_ rawValue: FrostflakeIdentifier) {
            self.rawValue = rawValue
        }

        public init?(_ str: String) {
            if let rawValue = FrostflakeIdentifier(str) {
                self.init(rawValue)
            } else {
                return nil
            }
        }

        public init(from buffer: inout ByteBuffer) throws {
            if let rawValue = buffer.readInteger(as: FrostflakeIdentifier.self) {
                self.rawValue = rawValue
            } else {
                throw DistributedSystemErrors.error("Failed to decode ServiceIdentifier")
            }
        }

        public func encode(to buffer: inout ByteBuffer) {
            buffer.writeInteger(rawValue)
        }
    }

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

    public struct CancellationToken {
        let serviceName: String
        let id: UInt64
        let actorSystem: DistributedSystem

        init(_ serviceName: String, _ id: UInt64, _ actorSystem: DistributedSystem) {
            self.serviceName = serviceName
            self.id = id
            self.actorSystem = actorSystem
        }

        public func cancel() -> Bool {
            actorSystem.cancel(serviceName, id)
        }
    }

    public enum ServiceMetadata: String {
        case moduleIdentifier
        case processIdentifier
        case systemName
        case datacenter
    }

    public static var logger = Logger(label: "ds")
    var logger: Logger { Self.logger }

    var logMetadataBox = Box<Logger.Metadata?>(nil)
    var logMetadata: Logger.Metadata? { logMetadataBox.value }

    // TODO: replace with configuration
    private static let pingInterval = TimeAmount.seconds(5)
    public static let serviceDiscoveryTimeout = TimeAmount.seconds(5)

    enum SessionMessage: UInt16 {
        case createServiceInstance = 0
        case invocationEnvelope = 1
        case invocationResult = 2
        case duplicatedEndpointIdentifier = 3
    }

    public let systemName: String

    let eventLoopGroup: EventLoopGroup
    let consul: Consul
    // Some services expect service discovery to be available from distributed system
    public let consulServiceDiscovery: ConsulServiceDiscovery

    private enum ActorInfo {
        case newClient(any DistributedActor) // retain the client actor instance while it will not be linked to local or remote service
        case serviceForLocalClient(any DistributedActor) // retain the client actor until the related service actor will not be resigned
        case remoteClient(Channel)
        case remoteService(Channel)
        case clientForRemoteService(Channel, AsyncStream<InvocationEnvelope>.Continuation)
        case serviceForRemoteClient(Channel, AsyncStream<InvocationEnvelope>.Continuation)
    }

    private var lock = Lock()
    private var actors: [EndpointIdentifier: ActorInfo] = [:]
    private var connectionLossHandlers: [UnsafeMutableRawPointer: [ConnectionLossHandler]] = [:]

    private var nextCancellationID = ManagedAtomic<UInt64>(0)
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
        discoveryManager = DiscoveryManager(logMetadataBox)
        syncCallManager = SyncCallManager()
        duplicatedEndpointIdentifierHook = Self.duplicatedEndpointIdentifier

        // Self.logger.logLevel = .debug
        // Consul.logger.logLevel = .debug
    }

    deinit {
        logger.debug("deinit", metadata: logMetadata)
    }

    private static func duplicatedEndpointIdentifier(_ endpointID: EndpointIdentifier) {
        fatalError("duplicated endpoint identifier \(endpointID)")
    }

    func connectToProcessAt(_ address: SocketAddress) {
        logger.debug("connect to process @ \(address)", metadata: logMetadata)
        ClientBootstrap(group: eventLoopGroup)
            .channelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
            .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .channelInitializer { channel in
                channel.pipeline.addHandler(ByteToMessageHandler(StreamDecoder())).flatMap { _ in
                    channel.pipeline.addHandler(ChannelHandler(.client, self))
                }
            }
            .connect(to: address)
            .whenComplete { result in
                switch result {
                case let .success(channel):
                    self.setChannel(channel, forProcessAt: address)
                case let .failure(error):
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

    private func setChannel(_ channel: Channel, forProcessAt address: SocketAddress) {
        discoveryManager.setChannel(channel, forProcessAt: address)
    }

    func channelInactive(_ channelContext: ChannelHandlerContext) {
        let channel = channelContext.channel
        discoveryManager.channelInactive(channel)

        let (continuations, connectionLossHandlers) = lock.withLock {
            var continuations: [AsyncStream<InvocationEnvelope>.Continuation] = []
            var actors: [EndpointIdentifier] = []
            for (actorID, actorInfo) in self.actors {
                switch actorInfo {
                case let .clientForRemoteService(actorChannel, continuation):
                    if actorChannel === channel {
                        continuations.append(continuation)
                    }
                case let .serviceForRemoteClient(actorChannel, continuation):
                    if actorChannel === channel {
                        continuations.append(continuation)
                    }
                case let .remoteClient(actorChannel):
                    if actorChannel === channel {
                        actors.append(actorID)
                    }
                case let .remoteService(actorChannel):
                    if actorChannel === channel {
                        actors.append(actorID)
                    }
                default:
                    break
                }
            }
            for actorID in actors {
                self.actors.removeValue(forKey: actorID)
            }

            let ptr = Unmanaged.passUnretained(channelContext).toOpaque()
            let connectionLossHandlers = self.connectionLossHandlers[ptr]
            if let connectionLossHandlers {
                self.connectionLossHandlers.removeValue(forKey: ptr)
                return (continuations, connectionLossHandlers)
            } else {
                return (continuations, [])
            }
        }

        for continuation in continuations {
            continuation.finish()
        }

        for connectionLossHandler in connectionLossHandlers {
            connectionLossHandler()
        }
    }

    private func connectionEstablishmentFailed(_ error: Error, _ address: SocketAddress) {
        logger.debug("failed to connect to process @ \(address): \(error)", metadata: logMetadata)
        discoveryManager.connectionEstablishmentFailed(address)
    }

    private func addressForService(_ service: NodeService) -> SocketAddress? {
        let serviceAddress = service.serviceAddress.flatMap { $0.isEmpty ? nil : $0 } ?? service.address

        guard let serviceAddress else {
            logger.debug("skip service \(service.serviceID), missing address", metadata: logMetadata)
            return nil
        }

        guard let servicePort = service.servicePort else {
            logger.debug("skip service \(service.serviceID), missing '\(NodeService.CodingKeys.servicePort)'",
                         metadata: logMetadata)
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

    private func cancellationTokenForService(_ serviceName: String) -> CancellationToken {
        let id = nextCancellationID.wrappingIncrementThenLoad(ordering: .releasing)
        return CancellationToken(serviceName, id, self)
    }

    /// To be used to connect to multiple services of the same type.
    /// Service will be discovered with using a discovery system (consul by default).
    /// - parameters:
    ///     - serviceEndpointType - type of the service endpoint
    ///     - serviceFilter: user can filter out services and create a distributed actors only to needed
    ///     - clientFactory: a closure creating a client side endpoint instance
    ///     - serviceHandler: a clusure getting an instance of the service endpoint and a service where the endpoint is connected to
    ///
    public func connectToServices<S: ServiceEndpoint, C>(
        _ serviceEndpointType: S.Type,
        withFilter serviceFilter: @escaping ServiceFilter,
        clientFactory: ((DistributedSystem) -> C)? = nil,
        serviceHandler: @escaping (S, ConsulServiceDiscovery.Instance) -> ConnectionLossHandler?,
        cancellationToken: CancellationToken? = nil
    )
        where S.ID == EndpointIdentifier, S.ActorSystem == DistributedSystem {
        let serviceName = S.serviceName
        logger.debug("connectTo: \(serviceName)", metadata: logMetadata)

        let connectionHandler = { (serviceID: ServiceIdentifier, service: ConsulServiceDiscovery.Instance, channel: Channel?) -> ConnectionLossHandler? in
            let serviceEndpointID = {
                if (serviceEndpointType == PingServiceEndpoint.self) || (self.duplicatedEndpointIdentifier == nil) {
                    return EndpointIdentifier(serviceID)
                } else {
                    return self.duplicatedEndpointIdentifier!
                }
            }()

            if let channel {
                self.lock.withLockVoid {
                    self.actors[serviceEndpointID] = .remoteService(channel)
                }
            }

            if let clientFactory {
                let clientEndpointID = serviceEndpointID.makeClientEndpoint()
                Self.$actorID.withValue(clientEndpointID) {
                    _ = clientFactory(self)
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

        var cancellationToken = cancellationToken
        if cancellationToken == nil {
            cancellationToken = cancellationTokenForService(serviceName)
        }

        guard let cancellationToken else { fatalError("Internal error: cancellationToken unexpectedly nil") }

        let (discover, addresses) = discoveryManager.discoverService(serviceName, serviceFilter, connectionHandler, cancellationToken)
        if discover {
            _ = consulServiceDiscovery.subscribe(
                to: serviceName,
                onNext: { result in
                    switch result {
                    case let .success(services):
                        for service in services {
                            self.logger.trace("Found service \(service)", metadata: self.logMetadata)

                            guard let serviceSystemName = service.serviceMeta?[ServiceMetadata.systemName.rawValue] else {
                                self.logger.debug("service \(serviceName)/\(service.serviceID) has no '\(ServiceMetadata.systemName)' in the metadata",
                                                  metadata: self.logMetadata)
                                continue
                            }

                            guard serviceSystemName == self.systemName else {
                                self.logger.debug("skip service \(serviceName)/\(service.serviceID), different system",
                                                  metadata: self.logMetadata)
                                continue
                            }

                            guard let address = self.addressForService(service) else {
                                continue
                            }

                            guard let serviceID = ServiceIdentifier(service.serviceID) else {
                                self.logger.debug("skip service \(serviceName)/\(service.serviceID), invalid service identifier",
                                                  metadata: self.logMetadata)
                                continue
                            }

                            let connect = self.discoveryManager.setAddress(address, for: serviceName, serviceID, service)
                            self.logger.debug("setAddress \(address) for \(serviceName)/\(service.serviceID), connect=\(connect)",
                                              metadata: self.logMetadata)
                            if connect {
                                self.connectToProcessAt(address)
                            }
                        }
                    case let .failure(error):
                        self.logger.debug("\(error)")
                    }
                },
                onComplete: { _ in
                    self.logger.debug("onComplete", metadata: self.logMetadata)
                }
            )
        }

        for address in addresses {
            connectToProcessAt(address)
        }
    }

    /// To be used to connect to a single service of the particular type.
    /// Function returns the service endpoint instance after connection to service is established and service endpoint is ready for use.
    /// - Parameters:
    ///     - serviceEndpointType - type of the service endpoint
    ///     - serviceFilter: user can filter out services and create a distributed actors only to needed
    ///     - clientFactory: a closure creating a client side endpoint instance
    ///     - serviceHandler: a clusure getting an instance of the service endpoint and a service where the endpoint is connected to
    ///
    public func connectToService<S: ServiceEndpoint, C>(
        _ /* serviceEndpointType */: S.Type,
        withFilter serviceFilter: @escaping ServiceFilter,
        clientFactory: ((DistributedSystem) -> C)?,
        serviceHandler: ((S, ConsulServiceDiscovery.Instance) -> ConnectionLossHandler?)? = nil
    )
        async throws -> S
        where S.ID == EndpointIdentifier, S.ActorSystem == DistributedSystem {
        let cancellationToken = cancellationTokenForService(S.serviceName)
        return try await withCheckedThrowingContinuation { continuation in
            self.connectToServices(
                S.self,
                withFilter: serviceFilter,
                clientFactory: clientFactory,
                serviceHandler: { serviceEndpoint, service in
                    let cancelled = cancellationToken.cancel()
                    assert(cancelled)
                    let connectionLossHandler: ConnectionLossHandler?
                    if let serviceHandler {
                        connectionLossHandler = serviceHandler(serviceEndpoint, service)
                    } else {
                        connectionLossHandler = nil
                    }
                    continuation.resume(returning: serviceEndpoint)
                    return connectionLossHandler
                },
                cancellationToken: cancellationToken
            )
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
         serviceHandler: ((S, ConsulServiceDiscovery.Instance) -> ConnectionLossHandler?)? = nil) async throws -> S
        where S.ID == EndpointIdentifier, S.ActorSystem == DistributedSystem {
        let clientFactory: ((DistributedSystem) -> Any)? = nil
        return try await connectToService(serviceEndpointType, withFilter: serviceFilter, clientFactory: clientFactory, serviceHandler: serviceHandler)
    }

    private func cancel(_ serviceName: String, _ id: UInt64) -> Bool {
        self.discoveryManager.cancel(serviceName, id)
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
        logger.trace("update health status for \(services.count) services", metadata: logMetadata)

        for serviceID in services {
            let checkID = "service:\(serviceID)"
            _ = consul.agent.check(checkID, status: .passing)
        }

        eventLoop.scheduleTask(in: DistributedSystemServer.healthStatusUpdateInterval) {
            self.updateHealthStatus(with: eventLoop)
        }
    }

    private static func sendPing(to serviceEndpoint: PingServiceEndpoint, with eventLoop: EventLoop) {
        Task {
            do {
                try await serviceEndpoint.ping()
                eventLoop.scheduleTask(in: Self.pingInterval) {
                    sendPing(to: serviceEndpoint, with: eventLoop)
                }
            } catch {
                // seems connection to service lost
                logger.debug("\(error)")
            }
        }
    }

    /// Service lifecycle start
    public func start() throws {
        logger.debug("starting system '\(systemName)'", metadata: logMetadata)

        let eventLoop = eventLoopGroup.next()
        connectToServices(
            PingServiceEndpoint.self,
            withFilter: { _ in true },
            clientFactory: { actorSystem in
                PingServiceClientEndpoint(PingServiceClientImpl(), in: actorSystem)
            },
            serviceHandler: { serviceEndpoint, _ in
                Self.sendPing(to: serviceEndpoint, with: eventLoop)
                return nil
            }
        )
    }

    /// Service lifecycle stop
    public func stop() {
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
                case let .clientForRemoteService(_, continuation):
                    continuations.append(continuation)
                case let .serviceForRemoteClient(_, continuation):
                    continuations.append(continuation)
                default:
                    break
                }
            }
            return continuations
        }

        continuations.forEach { $0.finish() }

        logger.debug("stopped", metadata: logMetadata)
    }

    public func assignID<Actor>(_: Actor.Type) -> EndpointIdentifier
        where Actor: DistributedActor, EndpointIdentifier == Actor.ID {
        if let actorID = Self.$actorID.get() {
            logger.debug("assign<\(Actor.self)>: \(actorID)", metadata: logMetadata)
            return actorID
        } else {
            fatalError("Internal error: missing actor identifier")
        }
    }

    public func resolve<Actor>(id: EndpointIdentifier, as _: Actor.Type) throws -> Actor?
        where Actor: DistributedActor,
        EndpointIdentifier == Actor.ID {
        logger.debug("resolve<\(Actor.self)>: \(id)", metadata: logMetadata)
        if id.serviceID.rawValue == 0 {
            return lock.withLock {
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
                    fatalError("Internal error: client actor \(id) not registered")
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
                return nil
            }
        }
    }

    public func actorReady<Actor>(_ actor: Actor) where Actor: DistributedActor, EndpointIdentifier == Actor.ID {
        logger.debug("actorReady<\(Actor.self)>: \(actor.id)", metadata: logMetadata)
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
                    case let .remoteClient(channel):
                        var continuation: AsyncStream<InvocationEnvelope>.Continuation?
                        let stream = AsyncStream(InvocationEnvelope.self, bufferingPolicy: .unbounded) { continuation = $0 }
                        guard let continuation else { fatalError("Internal error: continuation unexpectedly nil") }
                        self.actors[actor.id] = .serviceForRemoteClient(channel, continuation)
                        Task { await self.streamTask(stream, actor, channel) }
                    default:
                        fatalError("Internal error: unexpected actor state \(actorInfo)")
                    }
                }
            }
        }
    }

    public func resignID(_ id: EndpointIdentifier) {
        logger.debug("resign: \(id)", metadata: logMetadata)
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
        let result: Res = try await syncCallManager.addCall { callID in
            try remoteCall(on: actor, target, callID: callID, &invocation)
        }
        return result
    }

    private func remoteCall<Actor>(
        on actor: Actor,
        _ target: RemoteCallTarget,
        callID: UInt64,
        _ invocation: inout InvocationEncoder
    ) throws where Actor: DistributedActor, Actor.ID == ActorID {
        let actorInfo = lock.withLock { self.actors[actor.id] }
        guard let actorInfo else {
            throw DistributedSystemErrors.connectionForActorLost(actor.id)
        }

        let channel = {
            switch actorInfo {
            case let .remoteClient(channel):
                return channel
            case let .remoteService(channel):
                return channel
            default:
                fatalError("Internal error: invalid actor state \(actor.id): \(actorInfo)")
            }
        }()

        let payloadSize = MemoryLayout<SessionMessage.RawValue>.size + InvocationEnvelope.wireSize(actor.id, callID, target, invocation.genericSubstitutions, invocation.arguments)
        // Even if we carefully calculated the capacity of the desired buffer and know it to the nearest byte,
        // swift-nio still allocate a buffer with a storage capacity rounded up to nearest power of 2...
        // Weird...
        var buffer = ByteBufferAllocator().buffer(capacity: MemoryLayout<UInt32>.size + payloadSize)
        buffer.writeInteger(UInt32(payloadSize))
        buffer.writeInteger(SessionMessage.invocationEnvelope.rawValue)
        InvocationEnvelope.encode(actor.id, callID, target, invocation.genericSubstitutions, &invocation.arguments, to: &buffer)
        logger.trace("\(channel.remoteAddressDescription): send \(buffer.readableBytes) bytes for \(actor.id)", metadata: logMetadata)
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
        try remoteCall(on: actor, target, callID: 0, &invocation)
    }

    func channelRead(_ channelContext: ChannelHandlerContext, _ buffer: inout ByteBuffer) {
        let channel = channelContext.channel
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
                createService(serviceName, endpointID, for: channelContext)
            case .invocationEnvelope:
                let envelope = try InvocationEnvelope(from: &buffer)
                invokeLocalCall(envelope: envelope, for: channel)
            case .invocationResult:
                try syncCallManager.handleResult(&buffer)
            case .duplicatedEndpointIdentifier:
                let endpointID = try EndpointIdentifier(from: &buffer)
                duplicatedEndpointIdentifierHook(endpointID)
            case .none:
                logger.error("\(channel.remoteAddressDescription): unexpected session message")
            }
        } catch {
            logger.error("\(channel.remoteAddressDescription): \(error)")
        }
    }

    private func createService(_ serviceName: String, _ endpointID: EndpointIdentifier, for channelContext: ChannelHandlerContext) {
        let serviceID = endpointID.serviceID
        let serviceFactory = discoveryManager.factoryFor(serviceName, serviceID)
        let channel = channelContext.channel
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
                self.actors[clientEndpointID] = .remoteClient(channel)
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
                    let ptr = Unmanaged.passUnretained(channelContext).toOpaque()
                    lock.withLockVoid {
                        self.connectionLossHandlers[ptr, default: []].append(connectionLossHandler)
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

    private func streamTask(_ stream: AsyncStream<InvocationEnvelope>, _ actor: any DistributedActor, _ channel: Channel) async {
        let remoteAddressDescription = channel.remoteAddressDescription
        logger.debug("\(remoteAddressDescription): start stream task for \(actor.id)", metadata: logMetadata)
        let resultHandler = ResultHandler()
        for await envelope in stream {
            var decoder = RemoteCallDecoder(envelope: envelope)
            do {
                try await executeDistributedTarget(on: actor,
                                                   target: RemoteCallTarget(envelope.targetFunc),
                                                   invocationDecoder: &decoder,
                                                   handler: resultHandler)
                if resultHandler.hasResult {
                    resultHandler.sendTo(channel, for: envelope.callID)
                } else {
                    if envelope.callID != 0 {
                        logger.error("internal error: missing result")
                    }
                }
            } catch {
                // TODO: should we propagate throw back? or close connection?
                logger.error("can't invoke target function: \(error)", metadata: logMetadata)
            }
        }
        logger.debug("\(remoteAddressDescription): streamTask for \(actor.id) done", metadata: logMetadata)
    }

    private func invokeLocalCall(envelope: InvocationEnvelope, for channel: Channel) {
        let targetID = envelope.targetID
        let continuation: AsyncStream<InvocationEnvelope>.Continuation? = lock.withLock {
            let actorInfo = self.actors[targetID]
            guard let actorInfo else {
                logger.error("\(channel.remoteAddressDescription): actor \(targetID) not found", metadata: logMetadata)
                return nil
            }
            switch actorInfo {
            case let .newClient(actor):
                var continuation: AsyncStream<InvocationEnvelope>.Continuation?
                let stream = AsyncStream(InvocationEnvelope.self, bufferingPolicy: .unbounded) { continuation = $0 }
                guard let continuation else { fatalError("Internal error: continuation unexpectedly nil") }
                self.actors[targetID] = .clientForRemoteService(channel, continuation)
                Task { await self.streamTask(stream, actor, channel) }
                return continuation
            case let .serviceForRemoteClient(_, continuation):
                return continuation
            case let .clientForRemoteService(_, continuation):
                return continuation
            default:
                logger.error("\(channel.remoteAddressDescription): invalid actor state \(actorInfo)", metadata: logMetadata)
                return nil
            }
        }

        if let continuation {
            continuation.yield(envelope)
        }
    }
}
