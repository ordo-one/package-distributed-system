import ArgumentParser
import Dispatch
import Distributed
import DistributedSystem
import class Foundation.ProcessInfo
import Lifecycle
import Logging
import TestMessages

@main
public struct ClientStarter: AsyncParsableCommand {
    @Option(
        help: "Query in text form"
    )
    var query: String = "id 7151723187586227489"

    @Flag(
        help: "Enable debug logging"
    )
    var debug = false

    public init() {}

    public mutating func run() async throws {
        let logLevel: Logger.Level = debug ? .debug : .info
        let client = TestClient(query: query, logLevel)
        await client.run()
    }
}

public class TestClient: TestableClient, @unchecked Sendable {
    private let actorSystem: DistributedSystem
    private let logger: Logger

    private var start: ContinuousClock.Instant?
    private var received = 0
    private var expected = 0

    public init(query: String, _ logLevel: Logger.Level) {
        let processInfo = Foundation.ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-test_system"
        actorSystem = DistributedSystem(systemName: systemName)
        var logger = Logger(label: "client")
        logger.logLevel = logLevel
        self.logger = logger
    }

    public func streamOpened(_: StreamOpened) async {
        logger.info("Stream is opened")
    }

    public func handleMonster(_ monster: Monster, for stream: Stream) async {
        received += 1
        logger.debug("Received monster \(monster) for \(stream.streamIdentifier)")
    }

    public func snapshotDone(for _: Stream) async {
        let finish = ContinuousClock.now
        if received != expected {
            logger.error("MISBEHAVIOR: received \(received) monsters, but expected \(expected)")
        }
        if let start {
            let duration = start.duration(to: finish)
            logger.info("Stream snapshot done in \(duration), received \(received) monsters")
        }
    }

    public func handleConnectionState(_ state: ConnectionState) async {
        // do nothing
    }

    public func start() async {
        logger.info("starting client...done!")

        do {
            let serverEndpoint = try await actorSystem.connectToService(
                TestServiceEndpoint.self,
                withFilter: { _ in true },
                clientFactory: { actorSystem in
                    TestClientEndpoint(self, in: actorSystem)
                }
            )

            let openRequest = _OpenRequestStruct(requestIdentifier: 1)
            start = ContinuousClock.now

            try await serverEndpoint.openStream(byRequest: OpenRequest(openRequest))
        } catch {
            logger.warning("Error: \(error)")
        }
    }

    public func stop() async {
        logger.info("stopping client...done!")
    }

    public func run() async {
        let signal = [ServiceLifecycle.Signal.INT]
        let lifecycle = ServiceLifecycle(configuration: .init(callbackQueue: .main,
                                                              shutdownSignal: signal, installBacktrace: true))

        lifecycle.register(label: "System",
                           start: .sync { try self.actorSystem.start() },
                           shutdown: .sync { self.actorSystem.stop() })

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
