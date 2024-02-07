import Distributed
@testable import DistributedSystem
@testable import DistributedSystemConformance
import XCTest

private let VALUE: UInt32 = 0x01020304

private distributed actor TestServiceEndpoint: ServiceEndpoint {
    public typealias ActorSystem = DistributedSystem
    public typealias SerializationRequirement = DistributedSystemConformance.Transferable

    static var serviceName = "TestService"

    distributed func handleRequest() async throws {
        let clientEndpointID = id.makeClientEndpoint()
        let clientEndpoint = try TestClientEndpoint<UInt32>.resolve(id: clientEndpointID, using: actorSystem)
        try await clientEndpoint.handleValue(VALUE)
    }

    distributed func handleConnectionState(_ state: ConnectionState) async throws {
        // do nothing
    }
}

private distributed actor TestClientEndpoint<T>: ClientEndpoint where T: Transferable {
    public typealias ActorSystem = DistributedSystem
    public typealias SerializationRequirement = DistributedSystemConformance.Transferable

    private var continuation: AsyncStream<T>.Continuation

    init(_ continuation: AsyncStream<T>.Continuation, in actorSystem: ActorSystem) {
        self.actorSystem = actorSystem
        self.continuation = continuation
    }

    distributed func handleValue(_ value: T) async throws {
        continuation.yield(value)
    }

    distributed func handleConnectionState(_ state: ConnectionState) async throws {
        // do nothing
    }
}

final class GenericDistributedActorTests: XCTestCase {
    override func setUp() {
        super.setUp()
        FrostflakeInitializer.initialize()
    }

    func test1() async throws {
        // DistributedSystem.logger.logLevel = .debug

        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"

        let moduleID = DistributedSystem.ModuleIdentifier(1)
        let serverSystem = DistributedSystemServer(name: systemName)
        try await serverSystem.start()
        try await serverSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleID) { actorSystem in
            TestServiceEndpoint(actorSystem: actorSystem)
        }

        let clientSystem = DistributedSystem(name: systemName)
        try clientSystem.start()

        var streamContinuation: AsyncStream<UInt32>.Continuation?
        let stream = AsyncStream<UInt32>() { streamContinuation = $0 }
        guard let streamContinuation else { fatalError("Internal error") }

        let serviceEndpoint = try await clientSystem.connectToService(
            TestServiceEndpoint.self,
            withFilter: { _ in true },
            clientFactory: { actorSystem in
                TestClientEndpoint(streamContinuation, in: actorSystem)
            }
        )

        try await serviceEndpoint.handleRequest()

        for await v in stream {
            XCTAssertEqual(v, VALUE)
            break
        }

        clientSystem.stop()
        serverSystem.stop()
    }
}
