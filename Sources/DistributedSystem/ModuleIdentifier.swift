// Copyright 2024 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

public struct ModuleIdentifier: Hashable, Codable, CustomStringConvertible {
    public let rawValue: UInt64

    public var description: String {
        String(describing: rawValue)
    }

    public init(_ rawValue: UInt64) {
        self.rawValue = rawValue
    }

    public init?(_ rawValue: UInt64?) {
        if let rawValue {
            self.rawValue = rawValue
        } else {
            return nil
        }
    }

    public init?(_ str: String) {
        if let rawValue = UInt64(str) {
            self.init(rawValue)
        } else {
            return nil
        }
    }
}
