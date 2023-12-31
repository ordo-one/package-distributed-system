import Distributed
import DistributedSystemConformance

protocol PingEndpoint {
    func ping() async throws
}

distributed actor PingServiceEndpoint: ServiceEndpoint, PingEndpoint {
    public typealias ActorSystem = DistributedSystem

    public static var serviceName: String { "Ping" }

    private let clientEndpoint: PingServiceClientEndpoint

    init(actorSystem: ActorSystem) throws {
        self.actorSystem = actorSystem
        clientEndpoint = try PingServiceClientEndpoint.resolve(id: id.makeClientEndpoint(), using: actorSystem)
        actorSystem.sendPing(to: clientEndpoint, id: clientEndpoint.id)
    }

    distributed func ping() async throws {
        actorSystem.logger.trace("\(id): ping")
        do {
            try await clientEndpoint.pong()
        } catch {
            actorSystem.logger.error("\(error)")
        }
    }

    distributed func pong() async throws {
        actorSystem.logger.trace("\(id): pong")
    }
}

distributed actor PingServiceClientEndpoint: ClientEndpoint, PingEndpoint {
    public typealias ActorSystem = DistributedSystem

    let serviceID: ServiceIdentifier?
    var serviceEndpoint: PingServiceEndpoint?

    init(actorSystem: ActorSystem, _ serviceID: ServiceIdentifier?) {
        self.actorSystem = actorSystem
        self.serviceID = serviceID
    }

    distributed func ping() async throws {
        actorSystem.logger.trace("\(id): ping")

        if serviceEndpoint == nil {
            if let serviceID {
                let endpointID = EndpointIdentifier(id.instanceID, serviceID)
                serviceEndpoint = try PingServiceEndpoint.resolve(id: endpointID, using: actorSystem)
            }
        }

        if let serviceEndpoint {
            do {
                try await serviceEndpoint.pong()
            } catch {
                actorSystem.logger.error("\(error)")
            }
        }
    }

    distributed func pong() async throws {
        actorSystem.logger.trace("\(id): pong")
    }
}
