public struct NetworkAddress: Codable, Equatable, Hashable, CustomStringConvertible {
    public let host: String
    public let port: Int

    public init(host: String, port: Int) {
        self.host = host
        self.port = port
    }

    public var description: String {
        "\(host):\(port)"
    }

    public static var anyAddress: NetworkAddress { NetworkAddress(host: "0.0.0.0", port: 0) }
}
