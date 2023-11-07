// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import Distributed
import DistributedSystemConformance

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
