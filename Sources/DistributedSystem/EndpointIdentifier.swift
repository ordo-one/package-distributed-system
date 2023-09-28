// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import NIOCore

public struct EndpointIdentifier: Hashable, Codable, CustomStringConvertible {
    public let instanceID: DistributedSystem.InstanceIdentifier
    public let serviceID: DistributedSystem.ServiceIdentifier

    public var description: String {
        "\(instanceID):\(serviceID)"
    }

    public var wireSize: Int {
        instanceID.wireSize + serviceID.wireSize
    }

    init(_ instanceID: DistributedSystem.InstanceIdentifier, _ serviceID: DistributedSystem.ServiceIdentifier) {
        self.instanceID = instanceID
        self.serviceID = serviceID
    }

    public init(_ serviceID: DistributedSystem.ServiceIdentifier) {
        self.init(DistributedSystem.InstanceIdentifier(), serviceID)
    }

    public init(from buffer: inout ByteBuffer) throws {
        instanceID = try DistributedSystem.InstanceIdentifier(from: &buffer)
        serviceID = try DistributedSystem.ServiceIdentifier(from: &buffer)
    }

    public func encode(to buffer: inout ByteBuffer) {
        instanceID.encode(to: &buffer)
        serviceID.encode(to: &buffer)
    }

    public func makeClientEndpoint() -> EndpointIdentifier {
        assert(serviceID.rawValue != 0)
        return Self(instanceID, DistributedSystem.ServiceIdentifier(0))
    }
}
