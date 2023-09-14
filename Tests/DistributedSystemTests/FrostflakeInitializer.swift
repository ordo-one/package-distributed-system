// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import Foundation
import Frostflake
import Logging

var logger = Logger(label: "ds-test")

struct FrostflakeInitializer {
    private static var initialized = false

    static func initialize() {
        if !Self.initialized {
            // Use generator identifier derived from the process identifier
            // to reduce probability of clashing with other tests, but probably not enough
            let processInfo = ProcessInfo.processInfo
            let generatorIdentifier = UInt16(processInfo.processIdentifier % Int32(Frostflake.validGeneratorIdentifierRange.upperBound))
            Frostflake.setup(sharedGenerator: Frostflake(generatorIdentifier: generatorIdentifier))
            Self.initialized = true
        }
    }
}
