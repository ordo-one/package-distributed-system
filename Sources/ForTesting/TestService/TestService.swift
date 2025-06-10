import ArgumentParser
import Dispatch
import Distributed
import DistributedSystem
import class Foundation.ProcessInfo
import Logging
import TestMessages

@main
public struct ServiceStarter: AsyncParsableCommand {
    @Option(
        help: "Service port number"
    )
    var port: Int = 0

    public init() {}

    public mutating func run() async throws {
        let service = TestService(port: port)
        try await service.run()
    }
}

public class TestService: TestableService, @unchecked Sendable {
    private let actorSystem: DistributedSystemServer

    private let logger: Logger
    private let serverPort: Int

    private var clientEndpointID: EndpointIdentifier?
    private var clientEndpoint: TestClientEndpoint?

    public init(port: Int) {
        var logger = Logger(label: "server")
        logger.logLevel = .info
        self.logger = logger
        serverPort = port

        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-test_system"
        actorSystem = DistributedSystemServer(systemName: systemName)
    }

    public func openStream(byRequest request: OpenRequest) async {
        do {
            logger.info("Server: open stream")
            let clientEndpoint = try TestClientEndpoint.resolve(id: clientEndpointID!, using: actorSystem)
            self.clientEndpoint = clientEndpoint

            Task {
                let streamOpened = StreamOpened(requestIdentifier: request.id)
                try await clientEndpoint.streamOpened(streamOpened)

                let stream = Stream(streamIdentifier: 0)

                let count = 100_000
                for idx in 1...count {
                    let monster = Monster(identifier: MonsterIdentifier(idx))
                    try await clientEndpoint.handleMonster(monster, for: stream)
                }

                try await clientEndpoint.snapshotDone(for: stream)
                logger.info("sent \(count) messages")
            }
        } catch {
            logger.error("Error: \(error)")
        }
    }

    public func getMonster() async -> TestMessages.Monster {
        fatalError("")
    }

    public func doNothing() {
        fatalError("Should never be called")
    }

    public func handleMonsters(_ monsters: [Monster]) async {
        fatalError("Should never be called")
    }

    public func handleMonsters(_ monsters: [String: Monster]) async {
        fatalError("Should never be called")
    }

    public func handleConnectionState(_ state: ConnectionState) async {
        // do nothing
    }

    public func run() async throws {
        try await self.actorSystem.start(at: NetworkAddress(host: "0.0.0.0", port: self.serverPort))
        let moduleID = DistributedSystem.ModuleIdentifier(1)
        try await self.actorSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleID) { actorSystem in
            let serviceEndpoint = try TestServiceEndpoint(self, in: actorSystem)
            self.clientEndpointID = serviceEndpoint.id.makeClientEndpoint()
            return serviceEndpoint
        }

        // wait forever while the test service will not be stopped by signal
        let (stream, _) = AsyncStream.makeStream(of: Void.self)
        for await _ in stream { break }
    }
}
