import Atomics
@testable import DistributedSystem
@testable import DistributedSystemConformance
import Frostflake
import Logging
import NIOCore
@testable import TestMessages
import XCTest

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
}

final class DistributedSystemTests: XCTestCase {
    override func setUp() {
        super.setUp()
        FrostflakeInitializer.initialize()
    }

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
    }

    class ClientWithLeakCheckImpl: TestableClient {
        private var logger: Logger { DistributedSystem.logger }
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
    }

    func testLocalService() async throws {
        // Checking the distributed actors do not leak,
        // use a closure here to be sure at the check point all references will be released
        let flags = Flags()
        _ = try await {
            let processInfo = ProcessInfo.processInfo
            let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"

            let moduleID = DistributedSystem.ModuleIdentifier(FrostflakeIdentifier(processInfo.processIdentifier))
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
                },
                serviceHandler: { _, _ in
                    let connectionLossHandler: DistributedSystem.ConnectionLossHandler = {
                        fatalError("Internal error: should never happen")
                    }
                    return connectionLossHandler
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
        // XCTAssertTrue(flags.clientDeallocated)
        XCTAssertFalse(flags.clientConnectionClosed)
    }

    func testRemoteService() async throws {
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
                let connectionLossHandler: DistributedSystem.ConnectionLossHandler = {
                    flags.serviceConnectionClosed = true
                }
                return (serviceEndpoint, connectionLossHandler)
            }

            let clientSystem = DistributedSystem(name: systemName)
            try clientSystem.start()

            var client: ClientWithLeakCheckImpl? = ClientWithLeakCheckImpl(flags)

            let serviceEndpoint = try await clientSystem.connectToService(
                TestServiceEndpoint.self,
                withFilter: { _ in true },
                clientFactory: { actorSystem in
                    TestClientEndpoint(client!, in: actorSystem)
                },
                serviceHandler: { _, _ in
                    let connectionLossHandler = {
                        flags.clientConnectionClosed = true
                    }
                    return connectionLossHandler
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

    func testClientDuplicate() async throws {
        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"

        let moduleID = DistributedSystem.ModuleIdentifier(FrostflakeIdentifier(processInfo.processIdentifier))
        let serverSystem = DistributedSystemServer(name: systemName)
        try await serverSystem.start()
        try await serverSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleID) { actorSystem in
            try TestServiceEndpoint(Service(), in: actorSystem)
        }

        var continuation: AsyncStream<Void>.Continuation?
        let stream = AsyncStream<Void>() { continuation = $0 }
        guard let continuation else { fatalError("Internal error: continuation unexpectedly nil") }

        let clientSystem1 = DistributedSystem(name: systemName)
        try clientSystem1.start()

        let distributedService1 = try await clientSystem1.connectToService(
            TestServiceEndpoint.self,
            withFilter: { _ in true },
            clientFactory: { actorSystem in TestClientEndpoint(Client(), in: actorSystem) }
        )

        let clientSystem2 = DistributedSystem(name: systemName)
        clientSystem2.duplicatedEndpointIdentifierHook = { endpointID in
            print("Duplicate endpoint identifier \(endpointID)")
            continuation.finish()
        }

        clientSystem2.duplicatedEndpointIdentifier = distributedService1.id
        try clientSystem2.start()

        _ = try await clientSystem2.connectToService(
            TestServiceEndpoint.self,
            withFilter: { _ in true },
            clientFactory: { actorSystem in TestClientEndpoint(Client(), in: actorSystem) }
        )

        _ = await stream.first(where: { true })

        clientSystem2.stop()
        clientSystem1.stop()
        serverSystem.stop()
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

    func testConcurrentRemoteCalls() async throws {
        class ServiceImpl: TestableService {
            private var logger: Logger { DistributedSystem.logger }
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
        }

        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"
        let count = 5_000

        let service = ServiceImpl(count * 2)
        let moduleID = DistributedSystem.ModuleIdentifier(FrostflakeIdentifier(processInfo.processIdentifier))
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
            clientFactory: { actorSystem in
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
                return nil
            }
        )

        clientSystem.connectToServices(
            TestServiceEndpoint.self,
            withFilter: { _ in true },
            clientFactory: { actorSystem in
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
                return nil
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
                    for _ in 1...count {
                        let monster = _MonsterStruct(identifier: Frostflake.generate())
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
        }

        let streams = 3
        let monsters = 5
        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)"

        let moduleID = DistributedSystem.ModuleIdentifier(FrostflakeIdentifier(processInfo.processIdentifier))
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

        let moduleID = DistributedSystem.ModuleIdentifier(FrostflakeIdentifier(processInfo.processIdentifier))
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

    func testSC1682() async throws {
        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"

        let distributedSystem = DistributedSystemServer(name: systemName)
        try await distributedSystem.start()

        let moduleID = DistributedSystem.ModuleIdentifier(FrostflakeIdentifier(processInfo.processIdentifier))
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
            clientFactory: { actorSystem in
                TestClientEndpoint(Client(), in: actorSystem)
            },
            serviceHandler: { _, _ in
                continuation.yield()
                return nil
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
            },
            serviceHandler: { _, _ in
                let connectionLossHandler = { () -> Void in
                    Task { try await createAndStartServerSystem() }
                }
                return connectionLossHandler
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
                    return nil
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
            let serviceEndpoint = try TestServiceEndpoint(Service(), in: actorSystem)
            return (serviceEndpoint, nil)
        }

        let clientSystem = DistributedSystem(name: systemName)
        try clientSystem.start()

        let cancellationToken = clientSystem.makeCancellationToken()
        _ = cancellationToken.cancel()

        let clientFactory: ((DistributedSystem) -> Any)? = nil
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
                withFilter: { _ in true },
                serviceHandler: { _, _ in
                    return nil
                }
            )
        }
        task.cancel()
        _ = await task.result

        clientSystem.stop()
    }
}
