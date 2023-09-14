import Distributed
import DistributedSystemConformance

protocol PingService {
    func ping(_ clientEndpoint: PingServiceClientEndpoint) async
}

struct PingServiceImpl: PingService {
    func ping(_ clientEndpoint: PingServiceClientEndpoint) async {
        // DistributedSystem.logger.debug("\(id): ping")
        do {
            try await clientEndpoint.pong()
        } catch {
            DistributedSystem.logger.error("\(error)")
        }
    }
}

distributed actor PingServiceEndpoint: ServiceEndpoint {
    public typealias ActorSystem = DistributedSystem

    public static var serviceName: String { "Ping" }

    private let service: PingService
    private let clientEndpoint: PingServiceClientEndpoint

    init(_ service: PingService, in actorSystem: ActorSystem) throws {
        self.actorSystem = actorSystem
        self.service = service
        clientEndpoint = try PingServiceClientEndpoint.resolve(id: id.makeClientEndpoint(), using: actorSystem)
    }

    distributed func ping() async throws {
        await service.ping(clientEndpoint)
    }
}

protocol PingServiceClient {
    func pong() async
}

struct PingServiceClientImpl: PingServiceClient {
    func pong() async {
        // DistributedSystem.logger.debug("\(id): pong")
    }
}

distributed actor PingServiceClientEndpoint: ClientEndpoint {
    public typealias ActorSystem = DistributedSystem

    private let client: PingServiceClient

    init(_ client: PingServiceClient, in actorSystem: ActorSystem) {
        self.actorSystem = actorSystem
        self.client = client
    }

    distributed func pong() async throws {
        await client.pong()
    }
}

