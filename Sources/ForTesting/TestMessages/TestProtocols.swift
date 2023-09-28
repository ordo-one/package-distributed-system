public protocol TestableService {
    func openStream(byRequest request: OpenRequest) async
    func getMonster() async -> Monster
    func doNothing() async
    func handleMonsters(_ monsters: [Monster]) async
}

public protocol TestableClient {
    /// Acknowledgement for previously sent open request
    func streamOpened(_ reply: StreamOpened) async

    /// Snapshot done for previously opened stream
    func snapshotDone(for: Stream) async

    func handleMonster(_ monster: Monster, for stream: Stream) async
}
