// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import DistributedSystemConformance
import Frostflake
internal import NIOCore

extension ServiceIdentifier {
    var wireSize: Int {
        MemoryLayout<FrostflakeIdentifier>.size
    }

    init(from buffer: inout ByteBuffer) throws {
        if let rawValue = buffer.readInteger(as: FrostflakeIdentifier.self) {
            self.init(rawValue)
        } else {
            throw DistributedSystemErrors.error("Failed to decode ServiceIdentifier")
        }
    }

    func encode(to buffer: inout ByteBuffer) {
        buffer.writeInteger(rawValue)
    }
}
