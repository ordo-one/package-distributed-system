import Atomics
import ConsulServiceDiscovery
import Distributed
@testable import DistributedSystem
import Logging
import NIOCore
@testable import TestMessages
import XCTest

var logger = Logger(label: "ds-test")

struct Client: TestableClient {
    func snapshotDone(for stream: TestMessages.Stream) async {
        logger.info("CLIENT: stream #\(stream) snapshot")
    }

    func streamOpened(_ reply: TestMessages.StreamOpened) {
        logger.info("CLIENT: stream #\(reply.requestIdentifier) opened")
    }

    func handleMonster(_ monster: TestMessages.Monster, for stream: TestMessages.Stream) {
        logger.info("CLIENT: got monster \(monster.identifier) from stream #\(stream.streamIdentifier)")
    }

    func handleConnectionState(_ state: ConnectionState) async {
        logger.info("CLIENT: connection state: \(state)")
    }
}

class Service: TestableService {
    var clientEndpoint: TestClientEndpoint?
    let stream: AsyncStream<Result<Void, Error>>
    private let streamContinuation: AsyncStream<Result<Void, Error>>.Continuation

    init() {
        var streamContinuation: AsyncStream<Result<Void, Error>>.Continuation?
        self.stream = AsyncStream<Result<Void, Error>>() { streamContinuation = $0 }
        guard let streamContinuation else { fatalError("streamContinuation unexpectedly nil") }
        self.streamContinuation = streamContinuation
    }

    func openStream(byRequest request: TestMessages.OpenRequest) async {
        logger.info("SERVER: open stream #\(request.requestIdentifier) request received")
        do {
            guard let clientEndpoint else { fatalError("Internal error: clientEndpoint unexpectedly nil") }
            try await clientEndpoint.streamOpened(StreamOpened(_StreamOpenedStruct(requestIdentifier: request.id)))
        } catch {
            fatalError("Unexpected error")
        }
    }

    func getMonster() -> Monster {
        var monster = _MonsterStruct(identifier: 5)
        monster.name = "orc"
        monster.hp = 100
        monster.mana = 100
        return Monster(monster)
    }

    func doNothing() {
        fatalError("Should not be called")
    }

    func handleMonsters(_ monsters: [Monster]) async {
        logger.info("SERVICE: got \(monsters.count) monsters")
    }

    func handleConnectionState(_ state: ConnectionState) async {
        logger.info("SERVICE: connection state: \(state)")
    }
}

final class DistributedSystemTests: XCTestCase {
    class Flags {
        var serviceDeallocated = false
        var serviceConnectionClosed = false
        var clientDeallocated = false
        var clientConnectionClosed = false
    }

    class ServiceWithLeakCheckImpl: TestableService {
        let flags: Flags
        var clientEndpoint: TestClientEndpoint?

        init(_ flags: Flags) {
            self.flags = flags
        }

        deinit {
            flags.serviceDeallocated = true
        }

        func openStream(byRequest request: TestMessages.OpenRequest) async {
            logger.info("SERVER: open stream #\(request.requestIdentifier) request received")
            do {
                guard let clientEndpoint else { fatalError("Internal error: clientEndpoint unexpectedly nil") }
                try await clientEndpoint.streamOpened(StreamOpened(_StreamOpenedStruct(requestIdentifier: request.id)))
                try await clientEndpoint.snapshotDone(for: Stream(_StreamStruct(streamIdentifier: request.id)))
            } catch {
                print("\(error)")
            }
        }

        func getMonster() -> Monster {
            fatalError("Should never be called")
        }

        func doNothing() {
            fatalError("Should never be called")
        }

        func handleMonsters(_ monsters: [Monster]) async {
            fatalError("Should never be called")
        }

        func handleConnectionState(_ state: ConnectionState) async {
            if case .closed = state {
                flags.serviceConnectionClosed = true
            }
        }
    }

    class ClientWithLeakCheckImpl: TestableClient {
        let flags: Flags
        let stream: AsyncStream<Void>
        private let continuation: AsyncStream<Void>.Continuation

        init(_ flags: Flags) {
            self.flags = flags
            var continuation: AsyncStream<Void>.Continuation?
            self.stream = AsyncStream<Void> { continuation = $0 }
            self.continuation = continuation!
        }

        deinit {
            flags.clientDeallocated = true
        }

        func snapshotDone(for stream: TestMessages.Stream) {
            logger.info("CLIENT: stream #\(stream.streamIdentifier) snapshot done")
            continuation.yield()
        }

        func streamOpened(_ reply: TestMessages.StreamOpened) {
            logger.info("CLIENT: request #\(reply.requestIdentifier) open")
        }

        func handleMonster(_ monster: TestMessages.Monster, for _: TestMessages.Stream) {
            fatalError("Should never be called")
        }

        func handleConnectionState(_ state: ConnectionState) async {
            if case .closed = state {
                flags.clientConnectionClosed = true
            }
        }
    }

    func testLocalService() async throws {
        // Checking the distributed actors do not leak,
        // use a closure here to be sure at the check point all references will be released
        let flags = Flags()
        _ = try await {
            let processInfo = ProcessInfo.processInfo
            let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"

            let moduleID = DistributedSystem.ModuleIdentifier(UInt64(processInfo.processIdentifier))
            let actorSystem = DistributedSystemServer(name: systemName)
            try await actorSystem.start()
            try await actorSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleID) { actorSystem in
                let service = ServiceWithLeakCheckImpl(flags)
                let serviceEndpoint = try TestServiceEndpoint(service, in: actorSystem)
                let clientEndpointID = serviceEndpoint.id.makeClientEndpoint()
                service.clientEndpoint = try TestClientEndpoint.resolve(id: clientEndpointID, using: actorSystem)
                return serviceEndpoint
            }

            var client: ClientWithLeakCheckImpl? = ClientWithLeakCheckImpl(flags)

            let serviceEndpoint = try await actorSystem.connectToService(
                TestServiceEndpoint.self,
                withFilter: { _ in true },
                clientFactory: { actorSystem in
                    TestClientEndpoint(client!, in: actorSystem)
                }
            )

            logger.info("TEST: open streams...")

            let openRequest = _OpenRequestStruct(requestIdentifier: 1)
            logger.info("CLIENT: send open request for stream #\(openRequest.id)")
            try await serviceEndpoint.openStream(byRequest: OpenRequest(openRequest))

            for await _ in client!.stream { break }
            client = nil

            actorSystem.stop()
        }()
        XCTAssertTrue(flags.serviceDeallocated)
        XCTAssertFalse(flags.serviceConnectionClosed)
        XCTAssertTrue(flags.clientDeallocated)
        XCTAssertFalse(flags.clientConnectionClosed)
    }

    struct ResourceLoadError: Error {
        let description: String
        init(_ description: String) {
            self.description = description
        }
    }

    func loadResource(_ resourceName: String) throws -> Data {
        let bundle = Bundle.module
        guard let resourceURL = bundle.url(forResource: resourceName, withExtension: nil) else {
            throw ResourceLoadError("Missing resource '\(resourceName)'")
        }
        return try Data(contentsOf: resourceURL)
    }

    func testRemoteService() async throws {
        // Checking the distributed actors do not leak,
        // use a closure here to be sure at the check point all references will be released
        let flags = Flags()
        _ = try await {
            let processInfo = ProcessInfo.processInfo
            let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"

            let moduleID = DistributedSystem.ModuleIdentifier(1)
            let serverDictionary = try loadResource("dict4Kb-mix")
            let serverCompressionMode: CompressionMode = .dictionary(.init(serverDictionary))
            let serverSystem = DistributedSystemServer(name: systemName, compressionMode: serverCompressionMode)
            try await serverSystem.start()
            try await serverSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleID) { actorSystem in
                let service = ServiceWithLeakCheckImpl(flags)
                let serviceEndpoint = try TestServiceEndpoint(service, in: actorSystem)
                let clientEndpointID = serviceEndpoint.id.makeClientEndpoint()
                service.clientEndpoint = try TestClientEndpoint.resolve(id: clientEndpointID, using: actorSystem)
                return serviceEndpoint
            }

            let clientDictionary = try loadResource("dict4Kb-pt")
            let clientCompressionMode: CompressionMode = .dictionary(.init(clientDictionary))
            let clientSystem = DistributedSystem(name: systemName, compressionMode: clientCompressionMode)
            try clientSystem.start()

            var client: ClientWithLeakCheckImpl? = ClientWithLeakCheckImpl(flags)

            let serviceEndpoint = try await clientSystem.connectToService(
                TestServiceEndpoint.self,
                withFilter: { _ in true },
                clientFactory: { actorSystem in
                    TestClientEndpoint(client!, in: actorSystem)
                }
            )

            let openRequest = _OpenRequestStruct(requestIdentifier: 1)
            try await serviceEndpoint.openStream(byRequest: OpenRequest(openRequest))

            for await _ in client!.stream { break }
            client = nil

            clientSystem.stop()
            serverSystem.stop()
        }()

        XCTAssertTrue(flags.serviceDeallocated)
        XCTAssertTrue(flags.serviceConnectionClosed)
        XCTAssertTrue(flags.clientDeallocated)
        XCTAssertTrue(flags.clientConnectionClosed)
    }

    /*
    func testWaitRemoteServiceDiscoveryTimeout() async throws {
        let client = Client()
        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)"

        let clientSystem = DistributedSystem(name: systemName)
        clientSystem.serviceDiscoveryTimeout = TimeAmount.seconds(2)
        try clientSystem.start()

        let moduleID = DistributedSystem.ModuleIdentifier(FrostflakeIdentifier(processInfo.processIdentifier))
        let clientEndpoint = TestClientEndpoint(client, in: clientSystem)
        let serviceEndpointID = clientEndpoint.id.makeEndpointForModule(moduleID)
        let serviceEndpoint = try TestServiceEndpoint.resolve(id: serviceEndpointID, using: clientSystem)
        do {
            try await clientSystem.waitServiceReady(serviceEndpoint)
        } catch let DistributedSystemErrors.serviceDiscoveryTimeout(timeoutServiceName, timeoutModuleID) {
            XCTAssertEqual(timeoutServiceName, TestServiceEndpoint.serviceName)
            XCTAssertEqual(timeoutModuleID, moduleID)
        }

        clientSystem.stop()
    }
    */

    func testReconnect() async throws {
        // Checking the distributed actors do not leak,
        // use a closure here to be sure at the check point all references will be released
        let flags = Flags()
        _ = try await {
            let processInfo = ProcessInfo.processInfo
            let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"

            let moduleID = DistributedSystem.ModuleIdentifier(1)
            let serverSystem = DistributedSystemServer(name: systemName)
            try await serverSystem.start()
            try await serverSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleID) { actorSystem in
                let service = ServiceWithLeakCheckImpl(flags)
                let serviceEndpoint = try TestServiceEndpoint(service, in: actorSystem)
                let clientEndpointID = serviceEndpoint.id.makeClientEndpoint()
                service.clientEndpoint = try TestClientEndpoint.resolve(id: clientEndpointID, using: actorSystem)
                return serviceEndpoint
            }

            let clientSystem = DistributedSystem(name: systemName)
            try clientSystem.start()

            let clientIdAtomic = ManagedAtomic<Int>(0)
            var client: ClientWithLeakCheckImpl? = ClientWithLeakCheckImpl(flags)

            clientSystem.connectToServices(
                TestServiceEndpoint.self,
                withFilter: { _ in true },
                clientFactory: { actorSystem, _ in TestClientEndpoint(client!, in: actorSystem) },
                serviceHandler: { serviceEndpoint, _ in
                    Task {
                        do {
                            let clientId = clientIdAtomic.wrappingIncrementThenLoad(ordering: .relaxed)
                            if clientId == 1 {
                                try serverSystem.closeConnectionFor(serviceEndpoint.id)
                            } else if clientId == 2 {
                                let openRequest = _OpenRequestStruct(requestIdentifier: 1)
                                try await serviceEndpoint.openStream(byRequest: OpenRequest(openRequest))
                            } else {
                                fatalError("internal error")
                            }
                        } catch {
                            logger.error("\(error)")
                        }
                    }
                }
            )

            for await _ in client!.stream { break }
            client = nil

            clientSystem.stop()
            serverSystem.stop()
        }()

        XCTAssertTrue(flags.serviceDeallocated)
        XCTAssertTrue(flags.serviceConnectionClosed)
        XCTAssertTrue(flags.clientDeallocated)
        XCTAssertTrue(flags.clientConnectionClosed)
    }

    func testConcurrentRemoteCalls() async throws {
        class ServiceImpl: TestableService {
            private let totalCount: Int
            private let count: ManagedAtomic<Int>
            let stream: AsyncStream<Void>
            private let continuation: AsyncStream<Void>.Continuation
            var clientEndpoint: TestClientEndpoint?

            init(_ totalCount: Int) {
                self.totalCount = totalCount
                self.count = ManagedAtomic(0)
                var continuation: AsyncStream<Void>.Continuation?
                self.stream = AsyncStream<Void> { continuation = $0 }
                guard let continuation else { fatalError("Internal error: continuation unexpectedly nil") }
                self.continuation = continuation
            }

            func openStream(byRequest request: TestMessages.OpenRequest) async {
                if count.wrappingIncrementThenLoad(ordering: .releasing) == totalCount {
                    logger.info("openStream() called \(totalCount) times, done")
                    continuation.finish()
                }
            }

            func getMonster() async -> Monster {
                fatalError("should never be called")
            }

            func doNothing() async {
                fatalError("should never be called")
            }

            func handleMonsters(_ monsters: [Monster]) async {
                fatalError("should never be called")
            }

            func handleConnectionState(_ state: ConnectionState) async {
                logger.info("SERVICE: connection state: \(state)")
            }
        }

        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"
        let count = 5_000

        let service = ServiceImpl(count * 2)
        let moduleID = DistributedSystem.ModuleIdentifier(UInt64(processInfo.processIdentifier))
        let serverSystem = DistributedSystemServer(name: systemName)
        try await serverSystem.start()
        try await serverSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleID) { actorSystem in
            try TestServiceEndpoint(service, in: actorSystem)
        }

        let clientSystem = DistributedSystem(name: systemName)
        try clientSystem.start()

        clientSystem.connectToServices(
            TestServiceEndpoint.self,
            withFilter: { _ in true },
            clientFactory: { actorSystem, _ in
                TestClientEndpoint(Client(), in: actorSystem)
            },
            serviceHandler: { serviceEndpoint, _ in
                Task {
                    let range = (1 ... count)
                    for id in range {
                        let openRequest = _OpenRequestStruct(requestIdentifier: UInt64(id))
                        try await serviceEndpoint.openStream(byRequest: OpenRequest(openRequest))
                    }
                }
            }
        )

        clientSystem.connectToServices(
            TestServiceEndpoint.self,
            withFilter: { _ in true },
            clientFactory: { actorSystem, _ in
                TestClientEndpoint(Client(), in: actorSystem)
            },
            serviceHandler: { serviceEndpoint, _ in
                Task {
                    let range = (count+1 ... count*2)
                    for id in range {
                        let openRequest = _OpenRequestStruct(requestIdentifier: UInt64(id))
                        try await serviceEndpoint.openStream(byRequest: OpenRequest(openRequest))
                    }
                }
            }
        )

        for await _ in service.stream { break }

        clientSystem.stop()
        serverSystem.stop()
    }

    func testOpenStream() async throws {
        class ServiceImpl: TestableService {
            private let count: Int
            var clientEndpoint: TestClientEndpoint?

            init(_ count: Int) {
                self.count = count
            }

            func openStream(byRequest request: TestMessages.OpenRequest) async {
                do {
                    guard let clientEndpoint else { fatalError("Internal error: clientEndpoint unexpectedly nil") }
                    try await clientEndpoint.streamOpened(StreamOpened(_StreamOpenedStruct(requestIdentifier: request.id)))
                    let stream = Stream(_StreamStruct(streamIdentifier: request.id))
                    for idx in 1...count {
                        let monster = _MonsterStruct(identifier: UInt64(idx))
                        logger.info("SERVER: send monster with id \(monster.id) to \(stream.id)")
                        try await clientEndpoint.handleMonster(Monster(monster), for: stream)
                    }
                    try await clientEndpoint.snapshotDone(for: stream)
                } catch {
                    logger.error("\(error)")
                }
            }

            func getMonster() async -> Monster {
                fatalError("should never be called")
            }

            func doNothing() async {
                fatalError("should never be called")
            }

            func handleMonsters(_ monsters: [Monster]) async {
                fatalError("should never be called")
            }

            func handleConnectionState(_ state: ConnectionState) async {
                logger.info("SERVICE: connection state: \(state)")
            }
        }

        class ClientImpl: TestableClient {
            private let expectedSnapshots: Int
            private let expectedMonsters: Int

            private var snapshots: Int = 0
            private var monsters: Int = 0

            private let continuation: AsyncStream<Void>.Continuation
            let stream: AsyncStream<Void>

            init(_ expectedSnapshots: Int, _ expectedMonsters: Int) {
                self.expectedSnapshots = expectedSnapshots
                self.expectedMonsters = expectedMonsters

                var continuation: AsyncStream<Void>.Continuation?
                self.stream = AsyncStream<Void> { continuation = $0 }
                guard let continuation else { fatalError("Internal error: continuation unexpectedly nil") }
                self.continuation = continuation
            }

            func snapshotDone(for stream: TestMessages.Stream) async {
                logger.info("CLIENT: stream #\(stream) snapshot")
                snapshots += 1
                if (snapshots == expectedSnapshots) && (monsters == (expectedMonsters * expectedSnapshots)) {
                    continuation.yield()
                }
            }

            func streamOpened(_ reply: TestMessages.StreamOpened) {
                logger.info("CLIENT: stream #\(reply.requestIdentifier) opened")
            }

            func handleMonster(_ monster: TestMessages.Monster, for stream: TestMessages.Stream) {
                logger.info("CLIENT: got monster \(monster.identifier) from stream #\(stream.streamIdentifier)")
                monsters += 1
            }

            func handleConnectionState(_ state: ConnectionState) async {
                logger.info("CLIENT: connection state: \(state)")
            }
        }

        let streams = 3
        let monsters = 5
        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)"

        let moduleID = DistributedSystem.ModuleIdentifier(UInt64(processInfo.processIdentifier))
        let serverSystem = DistributedSystemServer(name: systemName)
        try await serverSystem.start()
        try await serverSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleID) { actorSystem in
            let service = ServiceImpl(monsters)
            let serviceEndpoint = try TestServiceEndpoint(service, in: actorSystem)
            let clientEndpointID = serviceEndpoint.id.makeClientEndpoint()
            service.clientEndpoint = try TestClientEndpoint.resolve(id: clientEndpointID, using: actorSystem)
            return serviceEndpoint
        }

        let clientSystem = DistributedSystem(name: systemName)
        try clientSystem.start()

        var client: ClientImpl? = ClientImpl(streams, monsters)

        let distributedService = try await clientSystem.connectToService(
            TestServiceEndpoint.self,
            withFilter: { _ in true },
            clientFactory: { actorSystem in
                TestClientEndpoint(client!, in: actorSystem)
            }
        )

        logger.info("TEST: open streams...")

        for id in 1...streams {
            let openRequest = _OpenRequestStruct(requestIdentifier: RequestIdentifier(id))
            logger.info("CLIENT: send open request for stream #\(openRequest.id)")
            try await distributedService.openStream(byRequest: OpenRequest(openRequest))
        }

        for await _ in client!.stream { break }
        client = nil

        clientSystem.stop()
        serverSystem.stop()
    }

    func testNonVoidCall() async throws {
        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"

        let moduleID = DistributedSystem.ModuleIdentifier(UInt64(processInfo.processIdentifier))
        let serverSystem = DistributedSystemServer(name: systemName)
        try await serverSystem.start()
        try await serverSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleID) { actorSystem in
            try TestServiceEndpoint(Service(), in: actorSystem)
        }

        let clientSystem = DistributedSystem(name: systemName)
        try clientSystem.start()

        let distributedService = try await clientSystem.connectToService(
            TestServiceEndpoint.self,
            withFilter: { _ in true },
            clientFactory: { _ in }
        )

        let monster = try await distributedService.getMonster()
        XCTAssertEqual(monster.name, "orc")
        XCTAssertEqual(monster.hp, 100)
        XCTAssertEqual(monster.mana, 100)

        clientSystem.stop()
        serverSystem.stop()
    }

    func testThrowsErrorOnConnectionLostWhenWaitingNonVoidCall() async throws {
        distributed actor TestServiceEndpoint: ServiceEndpoint {
            public typealias ActorSystem = DistributedSystem
            public typealias SerializationRequirement = Transferable

            public static var serviceName: String { "test_service" }

            public distributed func getMonster() async throws -> Monster {
                var monster = _MonsterStruct(identifier: 5)
                monster.name = "orc"
                monster.hp = 100
                monster.mana = 100
                try actorSystem.closeConnectionFor(id.makeClientEndpoint())
                return Monster(monster)
            }

            public distributed func handleConnectionState(_ state: ConnectionState) async throws {
                // do nothing
            }
        }

        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"

        let moduleID = DistributedSystem.ModuleIdentifier(UInt64(processInfo.processIdentifier))
        let serverSystem = DistributedSystemServer(name: systemName)
        try await serverSystem.start()
        try await serverSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleID) { actorSystem in
            TestServiceEndpoint(actorSystem: actorSystem)
        }

        let clientSystem = DistributedSystem(name: systemName)
        try clientSystem.start()

        let distributedService = try await clientSystem.connectToService(
            TestServiceEndpoint.self,
            withFilter: { _ in true },
            clientFactory: { _ in }
        )

        var connectionLost = false
        do {
            let _ = try await distributedService.getMonster()
        } catch {
            if let error = error as? DistributedSystemErrors, case .connectionLost = error {
                connectionLost = true
            } else {
                throw error
            }
        }

        XCTAssertTrue(connectionLost)

        clientSystem.stop()
        serverSystem.stop()
    }

    func testSC1682() async throws {
        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"

        let distributedSystem = DistributedSystemServer(name: systemName)
        try await distributedSystem.start()

        let moduleID = DistributedSystem.ModuleIdentifier(UInt64(processInfo.processIdentifier))
        try await distributedSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleID) { actorSystem in
            let service = Service()
            let serviceEndpoint = try TestServiceEndpoint(service, in: actorSystem)
            let clientEndpointID = serviceEndpoint.id.makeClientEndpoint()
            service.clientEndpoint = try TestClientEndpoint.resolve(id: clientEndpointID, using: actorSystem)
            return serviceEndpoint
        }

        let distributedService = try await distributedSystem.connectToService(
            TestServiceEndpoint.self,
            withFilter: { _ in true },
            clientFactory: { actorSystem in
                TestClientEndpoint(Client(), in: actorSystem)
            }
        )

        let openRequest = _OpenRequestStruct(requestIdentifier: 1)
        try await distributedService.openStream(byRequest: OpenRequest(openRequest))

        logger.info("id=\(distributedService.id)")

        distributedSystem.stop()
    }

    func testConnectBeforeAddService() async throws {
        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"

        let distributedSystem = DistributedSystemServer(name: systemName)
        try await distributedSystem.start()

        var continuation: AsyncStream<Void>.Continuation?
        let stream = AsyncStream<Void>() { continuation = $0 }
        guard let continuation else { fatalError("Internal error: continuation unexpectedly nil") }

        distributedSystem.connectToServices(
            TestServiceEndpoint.self,
            withFilter: { _ in true },
            clientFactory: { actorSystem, _ in
                TestClientEndpoint(Client(), in: actorSystem)
            },
            serviceHandler: { _, _ in
                continuation.yield()
            }
        )

        let moduleID = DistributedSystem.ModuleIdentifier(1)
        try await distributedSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleID) { actorSystem in
            let serviceEndpoint = try TestServiceEndpoint(Service(), in: actorSystem)
            return serviceEndpoint
        }

        for await _ in stream { break }

        distributedSystem.stop()
    }

    func testSC2564() async throws {
        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"

        let createAndStartServerSystem = {
            let serverSystem = DistributedSystemServer(name: systemName)
            try await serverSystem.start()

            let moduleID = DistributedSystem.ModuleIdentifier(1)
            try await serverSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleID) { actorSystem in
                let serviceEndpoint = try TestServiceEndpoint(Service(), in: actorSystem)
                Task { actorSystem.stop() }
                return serviceEndpoint
            }
        }

        try await createAndStartServerSystem()

        let connects = ManagedAtomic<Int>(0)
        let clientSystem = DistributedSystem(name: systemName)
        try clientSystem.start()

        _ = try await clientSystem.connectToService(
            TestServiceEndpoint.self,
            withFilter: { _ in true },
            clientFactory: { _ in
                let cc = connects.wrappingIncrementThenLoad(ordering: .releasing)
                XCTAssertEqual(cc, 1)
            }
        )

        try await Task.sleep(for: Duration.seconds(3))
        clientSystem.stop()
    }

    func testConnectWithDeadline() async throws {
        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"
        let clientSystem = DistributedSystem(name: systemName)
        try clientSystem.start()

        do {
            _ = try await clientSystem.connectToService(
                TestServiceEndpoint.self,
                withFilter: { _ in true },
                clientFactory: { _ in
                    XCTFail("Should not be called")
                },
                serviceHandler: { _, _ in
                    XCTFail("Should not be called")
                },
                deadline: DispatchTime.now() + 2.0
            )
        } catch DistributedSystemErrors.serviceDiscoveryTimeout(let str) {
            XCTAssertEqual(str, TestServiceEndpoint.serviceName)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }

        clientSystem.stop()
    }

    func testCancelTokenBeforeConnect() async throws {
        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"

        let moduleID = DistributedSystem.ModuleIdentifier(1)
        let serverSystem = DistributedSystemServer(name: systemName)
        try await serverSystem.start()
        try await serverSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleID) { actorSystem in
            XCTFail("should not be called")
            return try TestServiceEndpoint(Service(), in: actorSystem)
        }

        let clientSystem = DistributedSystem(name: systemName)
        try clientSystem.start()

        let cancellationToken = clientSystem.makeCancellationToken()
        _ = cancellationToken.cancel()

        let clientFactory: ((DistributedSystem, ConsulServiceDiscovery.Instance) -> Any)? = nil
        let started = clientSystem.connectToServices(
            TestServiceEndpoint.self,
            withFilter: { _ in fatalError("should not be called") },
            clientFactory: clientFactory,
            serviceHandler: { _, _ in fatalError("should not be called") },
            cancellationToken: cancellationToken
        )
        XCTAssertFalse(started)

        try await Task.sleep(for: .seconds(1))

        clientSystem.stop()
        serverSystem.stop()
    }

    func testCancelTaskCallingConnectToService() async throws {
        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"

        let clientSystem = DistributedSystem(name: systemName)
        try clientSystem.start()

        let task = Task {
            let _ = try await clientSystem.connectToService(
                TestServiceEndpoint.self,
                withFilter: { _ in true }
            )
        }
        task.cancel()
        _ = await task.result

        clientSystem.stop()
    }

    func testResolveClientAfterConnectionLoss() async throws {
        distributed actor TestServiceEndpoint: ServiceEndpoint {
            typealias ActorSystem = DistributedSystem
            typealias SerializationRequirement = Transferable

            static var serviceName: String { "test_service" }
            let clientSystem: DistributedSystem
            let continuation: AsyncStream<Void>.Continuation

            init(_ actorSystem: DistributedSystem, _ clientSystem: DistributedSystem, _ continuation: AsyncStream<Void>.Continuation) {
                self.actorSystem = actorSystem
                self.clientSystem = clientSystem
                self.continuation = continuation
            }

            distributed func openStream() async throws {
                clientSystem.stop()
                let clientEndpointID = id.makeClientEndpoint()
                do {
                    // client system is stopped now,
                    // but server system at that point of time may not receive socket
                    // close notification yet, so resolve<> call will not throw error,
                    // but in that case handleConnectionState() should be called.
                    _ = try TestClientEndpoint.resolve(id: clientEndpointID, using: actorSystem)
                } catch DistributedSystemErrors.unknownActor {
                    continuation.yield()
                } catch {
                    throw error
                }
            }

            distributed func handleConnectionState(_ state: ConnectionState) async throws {
                continuation.yield()
            }
        }

        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"

        let clientSystem = DistributedSystem(name: systemName)

        var continuation: AsyncStream<Void>.Continuation?
        let stream = AsyncStream<Void>() { continuation = $0 }
        guard let continuation else { fatalError("continuation unexpectedly nil") }

        let moduleID = DistributedSystem.ModuleIdentifier(1)
        let serverSystem = DistributedSystemServer(name: systemName)
        try await serverSystem.start()
        try await serverSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleID) { actorSystem in
            TestServiceEndpoint(actorSystem, clientSystem, continuation)
        }

        try clientSystem.start()

        let serviceEndpoint = try await clientSystem.connectToService(
            TestServiceEndpoint.self,
            withFilter: { _ in true },
            clientFactory: { actorSystem in
                TestClientEndpoint(Client(), in: actorSystem)
            }
        )

        try await serviceEndpoint.openStream()
        for await _ in stream { break }

        serverSystem.stop()
    }

    func testSuspendClientEndpoint() async throws {
        // client send many requests while service slowly process them

        distributed actor TestServiceEndpoint: ServiceEndpoint {
            typealias ActorSystem = DistributedSystem
            typealias SerializationRequirement = Transferable

            static var serviceName: String { "test_service" }

            private var requests: Int
            private let continuation: AsyncStream<Void>.Continuation

            init(_ actorSystem: DistributedSystem, _ requests: Int, _ continuation: AsyncStream<Void>.Continuation) {
                self.actorSystem = actorSystem
                self.requests = requests
                self.continuation = continuation
            }

            distributed func openStream() async throws {
                self.requests -= 1
                if self.requests == 0 {
                    continuation.yield()
                }
                try await Task.sleep(for: .milliseconds(20))
            }

            distributed func handleConnectionState(_ state: ConnectionState) async throws {
            }
        }

        distributed actor TestClientEndpoint: ClientEndpoint {
            typealias ActorSystem = DistributedSystem
            typealias SerializationRequirement = Transferable

            distributed func handleConnectionState(_ state: ConnectionState) async throws {
            }
        }

        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"

        // let run test for 3 seconds
        let requests = 150

        var continuation: AsyncStream<Void>.Continuation?
        let stream = AsyncStream<Void>() { continuation = $0 }
        guard let continuation else { fatalError("continuation unexpectedly nil") }

        let moduleID = DistributedSystem.ModuleIdentifier(1)
        let serverSystem = DistributedSystemServer(name: systemName)

        // each invocation envelope is about 100 bytes,
        // will trigger suspend/resume few times per test
        serverSystem.endpointQueueHighWatermark = (2 * 1024)
        serverSystem.endpointQueueLowWatermark = 1024

        try await serverSystem.start()
        try await serverSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleID) { actorSystem in
            TestServiceEndpoint(actorSystem, requests, continuation)
        }

        let clientSystem = DistributedSystem(name: systemName)
        try clientSystem.start()

        let serviceEndpoint = try await clientSystem.connectToService(
            TestServiceEndpoint.self,
            withFilter: { _ in true },
            clientFactory: { actorSystem in
                TestClientEndpoint(actorSystem: actorSystem)
            }
        )

        for _ in 0..<5 {
            for _ in 0..<30 {
                try await serviceEndpoint.openStream()
            }
            try await Task.sleep(for: .milliseconds(600))
        }

        for await _ in stream { break }

        serverSystem.stop()
        clientSystem.stop()
    }

    func testConnectionStateHandlerCalledForRemoteClient() async throws {
        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"

        distributed actor TestServiceEndpoint: ServiceEndpoint {
            public typealias ActorSystem = DistributedSystem
            public typealias SerializationRequirement = Transferable

            public static var serviceName: String { "test_service" }

            private let connectionClosed: ManagedAtomic<Int>

            init(_ actorSystem: DistributedSystem, _ connectionClosed: ManagedAtomic<Int>) {
                self.actorSystem = actorSystem
                self.connectionClosed = connectionClosed
            }

            public distributed func handleConnectionState(_ state: ConnectionState) async throws {
                if case .closed = state {
                    connectionClosed.wrappingIncrement(ordering: .relaxed)
                }
            }
        }

        distributed actor TestClientEndpoint: ClientEndpoint {
            public typealias ActorSystem = DistributedSystem
            public typealias SerializationRequirement = Transferable

            public static var serviceName: String { "test_service" }

            private let connectionClosed: ManagedAtomic<Int>

            init(_ actorSystem: DistributedSystem, _ connectionClosed: ManagedAtomic<Int>) {
                self.actorSystem = actorSystem
                self.connectionClosed = connectionClosed
            }

            public distributed func handleConnectionState(_ state: ConnectionState) async throws {
                if case .closed = state {
                    connectionClosed.wrappingIncrement(ordering: .relaxed)
                }
            }
        }

        let connectionClosed = ManagedAtomic<Int>(0)

        let moduleID = DistributedSystem.ModuleIdentifier(1)
        let serverSystem = DistributedSystemServer(name: systemName)
        try await serverSystem.start()
        try await serverSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleID) { actorSystem in
            let serviceEndpoint = TestServiceEndpoint(actorSystem, connectionClosed)
            return serviceEndpoint
        }

        let clientSystem = DistributedSystem(name: systemName)
        try clientSystem.start()

        _ = try await clientSystem.connectToService(
            TestServiceEndpoint.self,
            withFilter: { _ in true },
            clientFactory: { actorSystem in
                TestClientEndpoint(actorSystem, connectionClosed)
            }
        )

        clientSystem.stop()
        serverSystem.stop()

        XCTAssertEqual(connectionClosed.load(ordering: .relaxed), 2)
    }

    // Test implemented to validate service reregister functionality if
    // health check update fails. The minimum time in Consul for critical
    // service removal is 1 minute.
    // Let's run test only manually to avoid long tests run.
    func testReRegisterServiceIfCheckFails() async throws {
        let processInfo = ProcessInfo.processInfo
        let runReRegisterServiceTestEnv = "RUN_REREGISTER_SERVICE_TEST"
        guard let runReRegisterServiceTest = processInfo.environment[runReRegisterServiceTestEnv],
              let runReRegisterServiceTest = Bool(runReRegisterServiceTest.lowercased()),
              runReRegisterServiceTest else {
            throw XCTSkip("set \(runReRegisterServiceTestEnv) environment variable to run it")
        }

        distributed actor TestServiceEndpoint: ServiceEndpoint {
            public typealias ActorSystem = DistributedSystem
            public typealias SerializationRequirement = Transferable

            public static var serviceName: String { "test_service" }

            public distributed func handleConnectionState(_ state: ConnectionState) async throws {
                // do nothing
            }
        }

        let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"

        let moduleID = DistributedSystem.ModuleIdentifier(1)
        let serverSystem = DistributedSystemServer(name: systemName)

        serverSystem.healthStatusUpdateInterval = TimeAmount.seconds(90)
        serverSystem.healthStatusTTL = TimeAmount.seconds(5)

        try await serverSystem.start()
        try await serverSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleID) { actorSystem in
            TestServiceEndpoint(actorSystem: actorSystem)
        }

        try await Task.sleep(for: .seconds(180))
    }
}
