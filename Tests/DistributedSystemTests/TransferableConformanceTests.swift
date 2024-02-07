import Distributed
@testable import DistributedSystem
@testable import DistributedSystemConformance
import FrostflakeKit
@testable import TestMessages
import XCTest

fileprivate enum ServiceError: Error {
    case error(String)
}

fileprivate distributed actor TestServiceEndpoint: ServiceEndpoint {
    public typealias ActorSystem = DistributedSystem
    public typealias SerializationRequirement = DistributedSystemConformance.Transferable

    static var serviceName: String { "TestActorService" }

    private let stream: AsyncStream<Result<Void, Error>>
    private let streamContinuation: AsyncStream<Result<Void, Error>>.Continuation

    init(actorSystem: DistributedSystem, _ stream: AsyncStream<Result<Void, Error>>, _ streamContinuation: AsyncStream<Result<Void, Error>>.Continuation) {
        self.actorSystem = actorSystem
        self.stream = stream
        self.streamContinuation = streamContinuation
    }

    // test a distributed call with an array of transferable and trivially copyable objects (integers)
    public distributed func handleIntArray(_ data: [Int]) async {
        var invalidEntries = 0
        for idx in 0..<data.count {
            if data[idx] != idx {
                invalidEntries += 1
            }
        }
        logger.info("SERVICE: got \(data.count) integers")
        if invalidEntries == 0 {
            streamContinuation.yield(.success(()))
        } else {
            streamContinuation.yield(.failure(ServiceError.error("Array has \(invalidEntries) invalid entries")))
        }
    }

    // test a distributed call with an array of transferable and trivially copyable objects (doubles)
    public distributed func handleDoubleArray(_ data: [Double]) async {
        var invalidEntries = 0
        for idx in 0..<data.count {
            if data[idx] != Double(idx) {
                invalidEntries += 1
            }
        }
        logger.info("SERVICE: got \(data.count) doubles")
        if invalidEntries == 0 {
            streamContinuation.yield(.success(()))
        } else {
            streamContinuation.yield(.failure(ServiceError.error("Array has \(invalidEntries) invalid entries")))
        }
    }

    // test a distributed call with an array of transferable objects
    public distributed func handleStringArray(_ data: [String]) async {
        var invalidEntries = 0
        var str = ""
        for s in data {
            if s != str {
                invalidEntries += 1
            }
            str += "A"
        }
        logger.info("SERVICE: got \(data.count) strings")
        if invalidEntries == 0 {
            streamContinuation.yield(.success(()))
        } else {
            streamContinuation.yield(.failure(ServiceError.error("Array has \(invalidEntries) invalid entries")))
        }
    }

    public distributed func handleMonsters(_ monsters: [String: Monster]) async {
        var invalidEntries = 0
        for key in monsters.keys {
            if !key.hasPrefix("monster-") {
                invalidEntries += 1
            }
        }
        logger.info("SERVICE: got \(monsters.count) monsters")
        if invalidEntries == 0 {
            streamContinuation.yield(.success(()))
        } else {
            streamContinuation.yield(.failure(ServiceError.error("Dictionary has \(invalidEntries) invalid entries")))
        }
    }

    public distributed func handleConnectionState(_ state: ConnectionState) async {
        logger.info("SERVICE: connection state: \(state)")
    }
}

final class TransferableConformanceTests: XCTestCase {
    override func setUp() {
        super.setUp()
        FrostflakeInitializer.initialize()
    }

    func testArraySerialization() async throws {
        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"

        var streamContinuation: AsyncStream<Result<Void, Error>>.Continuation?
        let stream = AsyncStream<Result<Void, Error>>() { streamContinuation = $0 }
        guard let streamContinuation else { fatalError("Internal error: streamContinuation unexpectedly nil") }

        let moduleID = DistributedSystem.ModuleIdentifier(1)
        let serverSystem = DistributedSystemServer(name: systemName)
        try await serverSystem.start()
        try await serverSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleID) { actorSystem in
            TestServiceEndpoint(actorSystem: actorSystem, stream, streamContinuation)
        }

        let clientSystem = DistributedSystem(name: systemName)
        try clientSystem.start()

        let serviceEndpoint = try await clientSystem.connectToService(
            TestServiceEndpoint.self,
            withFilter: { _ in true }
        )

        let intArray = (0...100).map { $0 }
        try await serviceEndpoint.handleIntArray(intArray)

        for await result in stream {
            if case let .failure(err) = result {
                XCTFail(String(describing: err))
            }
            break
        }

        let doubleArray = (0...150).map { Double($0) }
        try await serviceEndpoint.handleDoubleArray(doubleArray)

        for await result in stream {
            if case let .failure(err) = result {
                XCTFail(String(describing: err))
            }
            break
        }

        let stringArray = (0...200).map { String(repeating: "A", count: $0) }
        try await serviceEndpoint.handleStringArray(stringArray)

        for await result in stream {
            if case let .failure(err) = result {
                XCTFail(String(describing: err))
            }
            break
        }

        clientSystem.stop()
        serverSystem.stop()
    }

    func testDictionarySerialization() async throws {
        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-ts-\(processInfo.processIdentifier)-\(#line)"

        var streamContinuation: AsyncStream<Result<Void, Error>>.Continuation?
        let stream = AsyncStream<Result<Void, Error>>() { streamContinuation = $0 }
        guard let streamContinuation else { fatalError("Internal error: streamContinuation unexpectedly nil") }

        let moduleID = DistributedSystem.ModuleIdentifier(1)
        let serverSystem = DistributedSystemServer(name: systemName)
        try await serverSystem.start()
        try await serverSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleID) { actorSystem in
            TestServiceEndpoint(actorSystem: actorSystem, stream, streamContinuation)
        }

        let clientSystem = DistributedSystem(name: systemName)
        try clientSystem.start()

        let serviceEndpoint = try await clientSystem.connectToService(
            TestServiceEndpoint.self,
            withFilter: { _ in true }
        )

        var monsters = [String: Monster]()
        for idx in 1...5 {
            let monster = _MonsterStruct(identifier: MonsterIdentifier(idx))
            monsters["monster-\(idx)"] = Monster(monster)
        }

        try await serviceEndpoint.handleMonsters(monsters)

        for await result in stream {
            if case let .failure(err) = result {
                XCTFail(String(describing: err))
            }
            break
        }

        clientSystem.stop()
        serverSystem.stop()
    }
}
