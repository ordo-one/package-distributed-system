import ArgumentParser
import Dispatch
import Distributed
import DistributedSystem
import class Foundation.ProcessInfo
import Frostflake
import LatencyTimer
import Lifecycle
import Logging
import TestMessages

public var logger = Logger(label: "client")

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
        if debug {
            logger.logLevel = .debug
        } else {
            logger.logLevel = .info
        }

        let client = TestClient(query: query)

        await client.run()
    }
}

public class TestClient: TestableClient, @unchecked Sendable {
    private let actorSystem: DistributedSystem

    private var start: UInt64 = 0
    private var received = 0
    private var expected = 0

    public init(query: String) {
        let processInfo = Foundation.ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-test_system-\(processInfo.processIdentifier)"
        actorSystem = DistributedSystem(systemName: systemName)
    }

    public func streamOpened(_: StreamOpened) async {
        logger.info("Stream is opened")
    }

    public func handleMonster(_ monster: Monster, for stream: Stream) async {
        received += 1
        logger.debug("Received monster \(monster) for \(stream.streamIdentifier)")
    }

    public func snapshotDone(for _: Stream) async {
        let finish = LatencyTimer.getTimestamp()
        if received != expected {
            logger.error("MISBEHAVIOR: received \(received) monsters, but expected \(expected)")
        }
        let timePassed = finish - start
        logger.info("Stream snapshot done in \(timePassed) usec, received \(received) monsters")
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

            let openRequest = _OpenRequestStruct(requestIdentifier: serverEndpoint.id.instanceID.rawValue)
            start = LatencyTimer.getTimestamp()

            try await serverEndpoint.openStream(byRequest: OpenRequest(openRequest))
        } catch {
            logger.warning("Error: \(error)")
        }
    }

    public func stop() async {
        logger.info("stopping client...done!")
    }

    public func run() async {
        Frostflake.setup(sharedGenerator: .init(generatorIdentifier: 1))

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
