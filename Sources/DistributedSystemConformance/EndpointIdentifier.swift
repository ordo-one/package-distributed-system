// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

public struct EndpointIdentifier: Hashable, Codable, CustomStringConvertible {
    public let instanceID: InstanceIdentifier
    public let serviceID: ServiceIdentifier

    public var description: String {
        "\(instanceID):\(serviceID)"
    }

    public init(_ instanceID: InstanceIdentifier, _ serviceID: ServiceIdentifier) {
        self.instanceID = instanceID
        self.serviceID = serviceID
    }

    public init(_ serviceID: ServiceIdentifier) {
        self.init(InstanceIdentifier(), serviceID)
    }

    public func makeClientEndpoint() -> EndpointIdentifier {
        assert(serviceID.rawValue != 0)
        return Self(instanceID, ServiceIdentifier(0))
    }
}
