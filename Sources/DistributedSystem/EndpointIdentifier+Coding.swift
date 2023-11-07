// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import DistributedSystemConformance
internal import NIOCore

extension EndpointIdentifier {
    var wireSize: Int {
        instanceID.wireSize + serviceID.wireSize
    }

    init(from buffer: inout ByteBuffer) throws {
        let instanceID = try InstanceIdentifier(from: &buffer)
        let serviceID = try ServiceIdentifier(from: &buffer)
        self.init(instanceID, serviceID)
    }

    func encode(to buffer: inout ByteBuffer) {
        instanceID.encode(to: &buffer)
        serviceID.encode(to: &buffer)
    }
}
