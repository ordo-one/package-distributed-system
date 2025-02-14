import DistributedSystem

public protocol TestableService: Sendable {
    func openStream(byRequest request: OpenRequest) async
    func getMonster() async -> Monster
    func doNothing() async
    func handleMonsters(_ monsters: [Monster]) async
    func handleConnectionState(_ state: ConnectionState) async
}

public protocol TestableClient: Sendable {
    /// Acknowledgement for previously sent open request
    func streamOpened(_ reply: StreamOpened) async

    /// Snapshot done for previously opened stream
    func snapshotDone(for: Stream) async

    func handleMonster(_ monster: Monster, for stream: Stream) async

    func handleConnectionState(_ state: ConnectionState) async
}
