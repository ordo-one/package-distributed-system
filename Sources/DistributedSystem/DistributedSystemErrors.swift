// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import Distributed

public enum DistributedSystemErrors: DistributedActorSystemError {
    case decodeError(description: String)
    case duplicatedService(String, DistributedSystem.ModuleIdentifier)
    case error(String)
    case serviceDiscoveryTimeout(String)
    case unexpectedResultType(String)
    case connectionLost
    case unknownActor(EndpointIdentifier)
    case invalidActorState(String)
    case cancelled(String)
}

public enum StreamErrors: DistributedActorSystemError {
    case streamOperationFails(reason: String)
}
