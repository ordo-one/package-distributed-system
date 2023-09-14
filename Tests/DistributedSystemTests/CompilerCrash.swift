import Distributed
@testable import DistributedSystem
@testable import DistributedSystemConformance
import XCTest

protocol StorableKey {
    static var isInteger: Bool { get }
}

distributed actor TestActor<Object> where Object: Transferable & Identifiable, Object.ID: StorableKey {
    public typealias ActorSystem = DistributedSystem

    public init(in actorSystem: ActorSystem) {
        self.actorSystem = actorSystem
    }

    public distributed func handleObject(_ object: Object) async throws {
        print("\(object)")
    }
}

final class CompilerCrashTests: XCTestCase {
    func test1() async throws {
    }
}
