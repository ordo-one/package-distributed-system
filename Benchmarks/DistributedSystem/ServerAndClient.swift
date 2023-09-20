import PackageConcurrencyHelpers
import DistributedSystem
import LatencyStatistics
import LatencyTimer
import Logging
import TestMessages

public class Client: TestableClient {
    private let  logger: Logger

    let lock = Lock()

    var statistics = LatencyStatistics()

    var snapshotDoneReceived = false
    var streamOpened = false

    var snapshotDoneContinuation: CheckedContinuation<Void, Never>?
    var streamOpenedContinuation: CheckedContinuation<Void, Never>?

    let label: String

    public init(_ logger: Logger, label: String) {
        self.logger = logger
        self.label = label
    }

    public func whenSnapshotDone() async {
        await withCheckedContinuation { continuation in
            lock.withLock {
                snapshotDoneContinuation = continuation
                if snapshotDoneReceived {
                    continuation.resume()
                }
            }
        }
    }

    public func whenStreamOpened() async {
        await withCheckedContinuation { continuation in
            lock.withLockVoid {
                streamOpenedContinuation = continuation
                if streamOpened {
                    continuation.resume()
                }
            }
        }
    }

    public func reset() {
        snapshotDoneContinuation = nil
        streamOpenedContinuation = nil
        snapshotDoneReceived = false
        streamOpened = false
    }

    public func snapshotDone(for stream: TestMessages.Stream) {
        logger.debug("CLIENT: stream #\(stream.streamIdentifier) snapshot")

        lock.withLockVoid {
            snapshotDoneReceived = true
            if let snapshotDoneContinuation {
                snapshotDoneContinuation.resume()
            }
        }
    }

    public func streamOpened(_ reply: TestMessages.StreamOpened) {
        logger.debug("CLIENT: stream #\(reply.requestIdentifier) opened")

        lock.withLockVoid {
            streamOpened = true

            if let streamOpenedContinuation {
                streamOpenedContinuation.resume()
            }
        }
    }

    public func handleMonster(_ monster: TestMessages.Monster, for _: TestMessages.Stream) {
        /*
        let currentAttoSec = (LatencyTimer.getTimestamp() % 1_000_000) * 1_000_000_000_000
        var latency: UInt64 = 0

        if let monsterTime = monster.timeCreated?.attoseconds {
            if currentAttoSec > monsterTime {
                let timeDiff = currentAttoSec - monsterTime
                latency = timeDiff / 1_000_000_000_000
                // logger.info("CLIENT: got monster \(monster.id) from stream #\(to.streamIdentifier), latency: \(latency)")
                // logger.info("\(monster)")
                // statistics.add(Int(latency))
            }
            // ignore second wrapping
        }
        */
    }
}

public class Service: TestableService {
    private var logger: Logger

    let lock = Lock()

    var streamOpen = false
    var openStreamContinuation: CheckedContinuation<Void, Never>?

    private var doNothingCount = 0
    private var monstersCount = 0

    init(_ logger: Logger) {
        self.logger = logger
    }

    public func whenOpenStream() async {
        await withCheckedContinuation { continuation in
            lock.withLock {
                openStreamContinuation = continuation
                if streamOpen {
                    continuation.resume()
                }
            }
        }
    }

    public func reset() {
        openStreamContinuation = nil
        streamOpen = false
        doNothingCount = 0
        monstersCount = 0
    }

    public func openStream(byRequest request: TestMessages.OpenRequest) async {
        logger.debug("SERVER: open stream #\(request.requestIdentifier) request received")

        lock.withLockVoid {
            streamOpen = true

            if let openStreamContinuation {
                logger.debug("SERVER: continuation resume")
                openStreamContinuation.resume()
            } else {
                logger.debug("SERVER: continuation not here yet")
            }
        }
    }

    public func getMonster() -> Monster {
        fatalError("Should not be called")
    }

    public func doNothing() {
        doNothingCount += 1
    }

    public func handleMonsters(_ monsters: [Monster]) async {
        monstersCount += monsters.count
    }

    public func handleMonsters(_ monsters: [String: Monster]) async {
        fatalError("Should not be called")
    }
}

public actor ClientActor {
    private let client: TestableClient

    public init(_ client: TestableClient) {
        self.client = client
    }

    public func handleMonster(_ monster: TestMessages.Monster, for stream: TestMessages.Stream) async throws {
        await client.handleMonster(monster, for: stream)
    }

    public func snapshotDone(for stream: TestMessages.Stream) async throws {
        await client.snapshotDone(for: stream)
    }

    public func streamOpened(_ reply: TestMessages.StreamOpened) async throws {
        await client.streamOpened(reply)
    }
}

public actor ServiceActor {
    private let service: TestableService

    public init(_ service: TestableService) {
        self.service = service
    }

    public func openStream(byRequest request: TestMessages.OpenRequest) async throws {
        await service.openStream(byRequest: request)
    }
}

public class ClientClass {
    private let client: TestableClient

    public init(_ client: TestableClient) {
        self.client = client
    }

    public func handleMonster(_ monster: TestMessages.Monster, for stream: TestMessages.Stream) async throws {
        await client.handleMonster(monster, for: stream)
    }

    public func snapshotDone(for stream: TestMessages.Stream) async throws {
        await client.snapshotDone(for: stream)
    }

    public func streamOpened(_ reply: TestMessages.StreamOpened) async throws {
        await client.streamOpened(reply)
    }
}

public class ServiceClass {
    private let service: TestableService

    public init(_ service: TestableService) {
        self.service = service
    }

    public func openStream(byRequest request: TestMessages.OpenRequest) async throws {
        await service.openStream(byRequest: request)
    }
}
