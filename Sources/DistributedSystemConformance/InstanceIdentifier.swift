import Frostflake

public struct InstanceIdentifier: Hashable, Codable, CustomStringConvertible {
    public let rawValue: FrostflakeIdentifier

    public var description: String {
        String(describing: rawValue)
    }

    public init(_ rawValue: FrostflakeIdentifier = Frostflake.generate()) {
        self.rawValue = rawValue
    }
}
