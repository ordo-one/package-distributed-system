import Distributed

/// Defines service side endpoint distributed actor
public protocol ServiceEndpoint: DistributedActor {
    static var serviceName: String { get }
}

/// Defines client side endpoint distributed actor
public protocol ClientEndpoint: DistributedActor {}
