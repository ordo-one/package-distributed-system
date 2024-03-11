// Copyright 2024 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

public struct EndpointIdentifier: Hashable, Codable, CustomStringConvertible {
    // keep the side flag in the lower bits for more efficient ULEB128 encoding
    private static let serviceFlag: UInt32 = 0x00000001

    typealias InstanceIdentifier = UInt32

    let channelID: UInt32 // 0 - local
    let instanceID: InstanceIdentifier

    public var description: String {
        let side = ((instanceID & Self.serviceFlag) == 0) ? "C" : "S"
        return "\(side):\(channelID):\(instanceID >> 1)"
    }

    static func instanceIdentifierDescription(_ instanceID: InstanceIdentifier) -> String {
        let side = ((instanceID & Self.serviceFlag) == 0) ? "C" : "S"
        return "\(side):X:\(instanceID >> 1)"
    }

    init(_ channelID: UInt32, _ instanceID: InstanceIdentifier) {
        self.channelID = channelID
        self.instanceID = instanceID
    }

    public func makeClientEndpoint() -> EndpointIdentifier {
        assert((instanceID & Self.serviceFlag) != 0)
        return .init(channelID, instanceID ^ Self.serviceFlag)
    }

    func makeServiceEndpoint() -> EndpointIdentifier {
        assert((instanceID & Self.serviceFlag) == 0)
        return .init(channelID, instanceID | Self.serviceFlag)
    }

    static func makeServiceEndpoint(_ channelID: UInt32, _ instanceID: UInt32) -> EndpointIdentifier {
        return .init(channelID, instanceID | Self.serviceFlag)
    }
}
