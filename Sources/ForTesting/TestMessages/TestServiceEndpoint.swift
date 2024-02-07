import Distributed
import DistributedSystem
import DistributedSystemConformance

public distributed actor TestServiceEndpoint: ServiceEndpoint {
    public typealias ActorSystem = DistributedSystem
    public typealias SerializationRequirement = DistributedSystemConformance.Transferable

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
