// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import Distributed

/// Defines service side endpoint distributed actor
public protocol ServiceEndpoint: DistributedActor {
    static var serviceName: String { get }
}

/// Defines client side endpoint distributed actor
public protocol ClientEndpoint: DistributedActor {}
