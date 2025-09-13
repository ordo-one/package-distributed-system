// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import Distributed

public enum DistributedSystemErrors: DistributedActorSystemError, CustomStringConvertible {
    case decodeError(description: String)
    case duplicatedService(String, DistributedSystem.ModuleIdentifier)
    case error(String)
    case serviceDiscoveryTimeout(String)
    case unexpectedResultType(String)
    case connectionLost
    case unknownActor(EndpointIdentifier)
    case invalidActorState(String)
    case cancelled(String)
    case serviceMetadataUpdateError(String)

    public var description: String {
        switch self {
        case .decodeError(let description):
            return "Decode error: \(description)"
        case .duplicatedService(let serviceName, let moduleId):
            return "Duplicated service '\(serviceName)' in module \(moduleId)"
        case .error(let message):
            return "Error: \(message)"
        case .serviceDiscoveryTimeout(let details):
            return "Service discovery timeout: \(details)"
        case .unexpectedResultType(let details):
            return "Unexpected result type: \(details)"
        case .connectionLost:
            return "Connection lost"
        case .unknownActor(let endpointId):
            return "Unknown actor with endpoint identifier: \(endpointId)"
        case .invalidActorState(let details):
            return "Invalid actor state: \(details)"
        case .cancelled(let reason):
            return "Operation cancelled: \(reason)"
        case .serviceMetadataUpdateError(let details):
            return "Service metadata update error: \(details)"
        }
    }
}

public enum StreamErrors: DistributedActorSystemError {
    case streamOperationFails(reason: String)
}
