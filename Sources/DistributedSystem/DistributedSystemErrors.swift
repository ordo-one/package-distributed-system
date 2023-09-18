import Distributed

public enum DistributedSystemErrors: DistributedActorSystemError {
    case decodeError(description: String)
    case duplicatedService(String, DistributedSystem.ModuleIdentifier)
    case duplicatedEndpointIdentifier(EndpointIdentifier)
    case error(String)
    case connectionForActorLost(EndpointIdentifier)
    case serviceDiscoveryTimeout(String)
}

public enum StreamErrors: DistributedActorSystemError {
    case streamOperationFails(reason: String)
}
