import ArgumentParser
import Dispatch
import Distributed
import DistributedSystem
import class Foundation.ProcessInfo
@preconcurrency import Lifecycle
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
        await service.run()
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

    public func openStream(byRequest _: OpenRequest) async {
        do {
            logger.info("Server: open stream")
            let clientEndpoint = try TestClientEndpoint.resolve(id: clientEndpointID!, using: actorSystem)
            self.clientEndpoint = clientEndpoint

            Task {
                try await clientEndpoint.streamOpened(StreamOpened(_StreamOpenedStruct(requestIdentifier: 0)))

                for i in 1 ... 1_000_000 {
                    try await clientEndpoint.handleMonster(
                        Monster(_MonsterStruct(identifier: MonsterIdentifier(i))),
                        for: Stream(_StreamStruct(streamIdentifier: 0)))
                }
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

    public func start() async {}

    public func stop() async {}

    public func run() async {
        let signal = [ServiceLifecycle.Signal.INT]
        let lifecycle = ServiceLifecycle(configuration: .init(callbackQueue: .main,
                                                              shutdownSignal: signal, installBacktrace: true))

        lifecycle.register(label: "System",
                           start: .async {
                               try await self.actorSystem.start(at: NetworkAddress(host: "0.0.0.0", port: self.serverPort))
                               let moduleID = DistributedSystem.ModuleIdentifier(1)
                               try await self.actorSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleID) { actorSystem in
                                   let serviceEndpoint = try TestServiceEndpoint(self, in: actorSystem)
                                   self.clientEndpointID = serviceEndpoint.id.makeClientEndpoint()
                                   return serviceEndpoint
                               }
                           },
                           shutdown: .sync(actorSystem.stop))

        lifecycle.register(label: "Client",
                           start: .async(start),
                           shutdown: .async(stop))

        lifecycle.start { error in
            if let error {
                print("Opps...: \(error)")
            }
        }

        await withCheckedContinuation { continuation in
            DispatchQueue.global().async {
                lifecycle.wait()
                continuation.resume()
            }
        }
    }
}
