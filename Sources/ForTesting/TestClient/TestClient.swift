import ArgumentParser
import Dispatch
import Distributed
import DistributedSystem
import class Foundation.ProcessInfo
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
        let client = TestClient(logLevel: logLevel, query: query)
        await client.run()
    }
}

public class TestClient: TestableClient, @unchecked Sendable {
    private let logger: Logger

    private var start: ContinuousClock.Instant?
    private var received = 0

    public init(logLevel: Logger.Level, query: String) {
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
        if let start {
            let duration = start.duration(to: finish)
            logger.info("Stream snapshot done in \(duration), received \(received) monsters")
        }
    }

    public func handleConnectionState(_ state: ConnectionState) async {
        // do nothing
    }

    public func run() async {
        let processInfo = Foundation.ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-test_system"
        let distributedSystem = DistributedSystem(systemName: systemName)

        logger.info("starting client...done!")

        do {
            let serverEndpoint = try await distributedSystem.connectToService(
                TestServiceEndpoint.self,
                withFilter: { _ in true },
                clientFactory: { actorSystem in
                    TestClientEndpoint(self, in: actorSystem)
                }
            )

            let openRequest = OpenRequest(requestIdentifier: 1)
            start = ContinuousClock.now

            try await serverEndpoint.openStream(byRequest: openRequest)
        } catch {
            logger.warning("Error: \(error)")
        }

        // wait forever while the test client will not be stopped by signal
        let (stream, _) = AsyncStream.makeStream(of: Void.self)
        for await _ in stream { break }
    }
}
