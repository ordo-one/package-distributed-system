import Distributed
import DistributedSystem
import DistributedSystemConformance

public distributed actor TestClientEndpoint: ClientEndpoint {
    public typealias ActorSystem = DistributedSystem
    public typealias SerializationRequirement = DistributedSystemConformance.Transferable

    private let client: TestableClient

    public init(_ client: TestableClient, in actorSystem: ActorSystem) {
        self.actorSystem = actorSystem
        self.client = client
    }

    public distributed func handleMonster(_ monster: Monster, for stream: Stream) async throws {
        await client.handleMonster(monster, for: stream)
    }

    public distributed func snapshotDone(for stream: Stream) async throws {
        await client.snapshotDone(for: stream)
    }

    public distributed func streamOpened(_ reply: StreamOpened) async throws {
        await client.streamOpened(reply)
    }
}
