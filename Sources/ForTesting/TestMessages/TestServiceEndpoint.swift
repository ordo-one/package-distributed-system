import Distributed
import DistributedSystem

struct TransferableRemoteCallError: Error & Transferable {
    init() {
    }

    init(fromSerializedBuffer buffer: UnsafeRawBufferPointer) throws {
    }

    func withUnsafeBytesSerialization<Result>(_ body: (UnsafeRawBufferPointer) throws -> Result) rethrows -> Result {
        try body(UnsafeRawBufferPointer(start: nil, count: 0))
    }

    func _releaseBuffer() {
    }
}

struct RemoteCallError: Error {}

public distributed actor TestServiceEndpoint: ServiceEndpoint {
    public typealias ActorSystem = DistributedSystem
    public typealias SerializationRequirement = Transferable

    public static var serviceName: String { "test_service" }

    private let service: TestableService

    public init(_ service: TestableService, in actorSystem: ActorSystem) throws {
        self.actorSystem = actorSystem
        self.service = service
    }

    public distributed func openStream(byRequest request: OpenRequest) async throws {
        await service.openStream(byRequest: request)
    }

    public distributed func getMonster() async throws -> Monster {
        await service.getMonster()
    }

    public distributed func getMonsterThrowingTransferable() async throws -> Monster {
        throw TransferableRemoteCallError()
    }

    public distributed func getMonsterThrowingNonTransferable() async throws -> Monster {
        throw RemoteCallError()
    }

    public distributed func doNothing() async throws {
        await service.doNothing()
    }

    public distributed func handleMonsters(array monsters: [Monster]) async throws {
        await service.handleMonsters(monsters)
    }

    public distributed func handleConnectionState(_ state: ConnectionState) async throws {
        await service.handleConnectionState(state)
    }
}
