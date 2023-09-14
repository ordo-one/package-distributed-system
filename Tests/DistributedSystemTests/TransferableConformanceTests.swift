import Distributed
@testable import DistributedSystem
@testable import DistributedSystemConformance
import Frostflake
import XCTest

fileprivate distributed actor TestServiceEndpoint: ServiceEndpoint {
    public typealias ActorSystem = DistributedSystem
    public typealias SerializationRequirement = DistributedSystemConformance.Transferable

    static var serviceName: String { "TestActorService" }

    // test a distributed call with an array of transferable and trivially copyable objects (integers)
    public distributed func handleIntArray(_ data: [Int]) async {
        for idx in 0..<data.count {
            XCTAssertEqual(data[idx], idx)
        }
    }

    // test a distributed call with an array of transferable and trivially copyable objects (doubles)
    public distributed func handleDoubleArray(_ data: [Double]) async {
        for idx in 0..<data.count {
            XCTAssertEqual(data[idx], Double(idx))
        }
    }

    // test a distributed call with an array of transferable objects
    public distributed func handleStringArray(_ data: [String]) async {
        var str = ""
        for s in data {
            XCTAssertEqual(s, str)
            str += "A"
        }
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

        let moduleID = DistributedSystem.ModuleIdentifier(1)
        let serverSystem = DistributedSystemServer(name: systemName)
        try await serverSystem.start()
        try await serverSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleID) { actorSystem in
            TestServiceEndpoint(actorSystem: actorSystem)
        }

        let clientSystem = DistributedSystem(name: systemName)
        try clientSystem.start()

        let serviceEndpoint = try await clientSystem.connectToService(
            TestServiceEndpoint.self,
            withFilter: { _ in true }
        )

        let intArray = (0...100).map { $0 }
        try await serviceEndpoint.handleIntArray(intArray)

        let doubleArray = (0...150).map { Double($0) }
        try await serviceEndpoint.handleDoubleArray(doubleArray)

        let stringArray = (0...200).map { String(repeating: "A", count: $0) }
        try await serviceEndpoint.handleStringArray(stringArray)

        clientSystem.stop()
        serverSystem.stop()
    }
}
