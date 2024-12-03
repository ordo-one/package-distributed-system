import Benchmark
import Dispatch
import Distributed
import DistributedSystem
import class Foundation.ProcessInfo
import LatencyTimer
import Logging
import TestMessages

fileprivate struct Endpoints {
    let client: TestClientEndpoint
    let service: TestServiceEndpoint

    init(_ client: TestClientEndpoint, _ service: TestServiceEndpoint) {
        self.client = client
        self.service = service
    }
}

fileprivate struct SharedData {
    let serviceSystem: DistributedSystem
    let clientSystem1: DistributedSystem
    let clientSystem2: DistributedSystem
    let service: Service
    let client: Client
    let localEndpoints: Endpoints
    let remoteEndpoints1: Endpoints
    let remoteEndpoints2: Endpoints

    init(_ serviceSystem: DistributedSystem,
         _ clientSystem1: DistributedSystem,
         _ clientSystem2: DistributedSystem,
         _ service: Service,
         _ client: Client,
         _ localEndpoints: Endpoints,
         _ remoteEndpoints1: Endpoints,
         _ remoteEndpoints2: Endpoints) {
        self.serviceSystem = serviceSystem
        self.clientSystem1 = clientSystem1
        self.clientSystem2 = clientSystem2
        self.service = service
        self.client = client
        self.localEndpoints = localEndpoints
        self.remoteEndpoints1 = remoteEndpoints1
        self.remoteEndpoints2 = remoteEndpoints2
    }

    func shutdown() {
        serviceSystem.stop()
        clientSystem1.stop()
        clientSystem2.stop()
    }
}

private var logger = Logger(label: "ds-benchmarks")
private var sharedData: SharedData?

let benchmarks = {
    Benchmark.startupHook = {
        let processInfo = ProcessInfo.processInfo
        let systemName = "\(processInfo.hostName)-benchmark_test_system-\(processInfo.processIdentifier)"

        let serviceSystem = DistributedSystemServer(systemName: systemName)
        try await serviceSystem.start()
        let service = Service(logger)

        let (stream, streamContinuation) = AsyncStream.makeStream(of: TestClientEndpoint.self)
        var streamIterator = stream.makeAsyncIterator()

        let moduleId = DistributedSystem.ModuleIdentifier(1)
        try await serviceSystem.addService(ofType: TestServiceEndpoint.self, toModule: moduleId) { actorSystem in
            let serviceEndoint = try TestServiceEndpoint(service, in: actorSystem)
            let clientEndpoint = try TestClientEndpoint.resolve(id: serviceEndoint.id.makeClientEndpoint(), using: actorSystem)
            streamContinuation.yield(clientEndpoint)
            return serviceEndoint
        }

        let client = Client(logger)

        let localServiceEndpoint = try await serviceSystem.connectToService(
            TestServiceEndpoint.self,
            withFilter: { _ in true },
            clientFactory: { actorSystem in
                TestClientEndpoint(client, in: actorSystem)
            }
        )
        let localClientEndpoint = await streamIterator.next()
        guard let localClientEndpoint else {
            fatalError("localClientEndpoint unexpectedly nil")
        }

        let clientSystem1 = DistributedSystem(systemName: systemName)
        try clientSystem1.start()

        let remoteServiceEndpoint1 = try await clientSystem1.connectToService(
            TestServiceEndpoint.self,
            withFilter: { _ in true },
            clientFactory: { actorSystem in
                TestClientEndpoint(client, in: actorSystem)
            }
        )
        let remoteClientEndpoint1 = await streamIterator.next()
        guard let remoteClientEndpoint1 else {
            fatalError("remoteClientEndpoint unexpectedly nil")
        }

        let clientSystem2 = DistributedSystem(systemName: systemName, compressionMode: .streaming)
        let remoteServiceEndpoint2 = try await clientSystem2.connectToService(
            TestServiceEndpoint.self,
            withFilter: { _ in true },
            clientFactory: { actorSystem in
                TestClientEndpoint(client, in: actorSystem)
            }
        )
        let remoteClientEndpoint2 = await streamIterator.next()
        guard let remoteClientEndpoint2 else {
            fatalError("remoteClientEndpoint unexpectedly nil")
        }

        streamContinuation.finish()

        sharedData = .init(serviceSystem,
                           clientSystem1,
                           clientSystem2,
                           service,
                           client,
                           Endpoints(localClientEndpoint, localServiceEndpoint),
                           Endpoints(remoteClientEndpoint1, remoteServiceEndpoint1),
                           Endpoints(remoteClientEndpoint2, remoteServiceEndpoint2))
    }

    Benchmark.shutdownHook = {
        if let sharedData {
            sharedData.shutdown()
        } else {
            fatalError("Shared data is not initialized")
        }
        sharedData = nil
    }

    func runBenchmark(_ benchmark: Benchmark,
                      _ serviceSystem: DistributedSystem,
                      _ service: Service, _ serviceEndpoint: TestServiceEndpoint,
                      _ client: Client, _ clientEndpoint: TestClientEndpoint) async throws {
        do {
            let openRequest = _OpenRequestStruct(requestIdentifier: 1)

            try await serviceEndpoint.openStream(byRequest: OpenRequest(openRequest))
            await service.whenOpenStream()

            try await clientEndpoint.streamOpened(StreamOpened(_StreamOpenedStruct(requestIdentifier: openRequest.id)))
            await client.whenStreamOpened()

            let stream = Stream(_StreamStruct(streamIdentifier: openRequest.id))
            var monsterID: UInt64 = 1

            let bytesSentBefore = try await serviceSystem.getBytesSent()

            benchmark.startMeasurement()
            for _ in benchmark.scaledIterations {
                let monster = _MonsterStruct(identifier: monsterID)
                try await clientEndpoint.handleMonster(Monster(monster), for: stream)
                monsterID += 1
            }

            try await clientEndpoint.snapshotDone(for: stream)
            await client.whenSnapshotDone()

            benchmark.stopMeasurement()

            let bytesSentAfter = try await serviceSystem.getBytesSent()
            let bytesSent = (bytesSentAfter - bytesSentBefore)
            benchmark.measurement(.custom("Bytes sent"), Int(bytesSent))
        } catch {
            fatalError("Exception handled: \(error), stopping execution for this test")
        }

        client.reset()
        service.reset()
    }

    Benchmark("Send Monster  #1 - facade: class, local calls",
              configuration: Benchmark.Configuration(scalingFactor: .kilo)) { benchmark in
        guard let sharedData else {
            fatalError("Shared data is not initialized")
        }

        let service = sharedData.service
        let serverClass = ServiceClass(service)

        let client = sharedData.client
        let clientClass = ClientClass(client)

        do {
            let openRequest = _OpenRequestStruct(requestIdentifier: 1)

            try await serverClass.openStream(byRequest: OpenRequest(openRequest))
            try await clientClass.streamOpened(StreamOpened(_StreamOpenedStruct(requestIdentifier: openRequest.id)))

            let stream = Stream(_StreamStruct(streamIdentifier: openRequest.id))
            var monsterID: UInt64 = 1

            benchmark.startMeasurement()
            for _ in benchmark.scaledIterations {
                let monster = _MonsterStruct(identifier: monsterID)
                try await clientClass.handleMonster(Monster(monster), for: stream)
                monsterID += 1
            }

            try await clientClass.snapshotDone(for: stream)

            benchmark.stopMeasurement()
        } catch {
            fatalError("Exception handled: \(error), stopping execution for this test")
        }

        service.reset()
        client.reset()
    }

    Benchmark("Send Monster  #2 - facade: actor, local calls",
              configuration: Benchmark.Configuration(scalingFactor: .kilo)) { benchmark in
        guard let sharedData else {
            fatalError("Shared data is not initialized")
        }

        let service = sharedData.service
        let serverActor = ServiceActor(service)

        let client = sharedData.client
        let clientActor = ClientActor(client)

        do {
            let openRequest = _OpenRequestStruct(requestIdentifier: 1)

            try await serverActor.openStream(byRequest: OpenRequest(openRequest))

            try await clientActor.streamOpened(StreamOpened(_StreamOpenedStruct(requestIdentifier: openRequest.id)))

            let stream = Stream(_StreamStruct(streamIdentifier: openRequest.id))
            var monsterID: UInt64 = 1

            benchmark.startMeasurement()
            for _ in benchmark.scaledIterations {
                let monster = _MonsterStruct(identifier: monsterID)
                try await clientActor.handleMonster(Monster(monster), for: stream)
                monsterID += 1
            }

            try await clientActor.snapshotDone(for: stream)

            benchmark.stopMeasurement()
        } catch {
            fatalError("Exception handled: \(error), stopping execution for this test")
        }

        service.reset()
        client.reset()
    }

    Benchmark("Send Monster  #3 - facade: distributed actor, local calls",
              configuration: Benchmark.Configuration(scalingFactor: .kilo)) { benchmark in
        guard let sharedData else {
            fatalError("Shared data is not initialized")
        }

        let endpoints = sharedData.localEndpoints
        try await runBenchmark(benchmark, sharedData.serviceSystem, sharedData.service, endpoints.service, sharedData.client, endpoints.client)
    }

    Benchmark("Send Monster #4.1 - facade: distributed actor, remote (network) calls",
              configuration: Benchmark.Configuration(metrics: BenchmarkMetric.default + [.custom("Bytes sent")], scalingFactor: .kilo)) { benchmark in
        guard let sharedData else {
            fatalError("Shared data is not initialized")
        }
        let endpoints = sharedData.remoteEndpoints1
        try await runBenchmark(benchmark, sharedData.serviceSystem, sharedData.service, endpoints.service, sharedData.client, endpoints.client)
    }

    Benchmark("Send Monster #4.2 - facade: distributed actor, remote (network) calls, traffic compression",
              configuration: Benchmark.Configuration(metrics: BenchmarkMetric.default + [.custom("Bytes sent")], scalingFactor: .kilo)) { benchmark in
        guard let sharedData else {
            fatalError("Shared data is not initialized")
        }
        let endpoints = sharedData.remoteEndpoints2
        try await runBenchmark(benchmark, sharedData.serviceSystem, sharedData.service, endpoints.service, sharedData.client, endpoints.client)
    }

    Benchmark("Send Monster #4.3 - facade: distributed actor, remote (network) calls (slow receiver)",
              configuration: Benchmark.Configuration(scalingFactor: .kilo)) { benchmark in
        guard let sharedData else {
            fatalError("Shared data is not initialized")
        }

        var client = sharedData.client
        client.receiveThroughput = 10_000

        let endpoints = sharedData.remoteEndpoints1
        try await runBenchmark(benchmark, sharedData.serviceSystem, sharedData.service, endpoints.service, client, endpoints.client)
    }

    func arrayOfMonsters(_ benchmark: Benchmark, _ arraySize: Int) async throws {
        guard let sharedData else {
            fatalError("Shared data is not initialized")
        }

        var service = sharedData.service
        let client = sharedData.client
        let endpoints = sharedData.remoteEndpoints1

        do {
            let iterations = benchmark.scaledIterations.count / arraySize
            var monsterID: UInt64 = 1

            benchmark.startMeasurement()
            for _ in 1...iterations {
                var monsters = [Monster]()
                for _ in 1...arraySize {
                    let monster = _MonsterStruct(identifier: monsterID)
                    monsters.append(Monster(monster))
                }
                try await endpoints.service.handleMonsters(array: monsters)
                monsterID += 1
            }

            let openRequest = _OpenRequestStruct(requestIdentifier: 1)
            try await endpoints.service.openStream(byRequest: OpenRequest(openRequest))
            await service.whenOpenStream()

            benchmark.stopMeasurement()
        } catch {
            fatalError("Exception handled: \(error), stopping execution for this test")
        }

        service.reset()
        client.reset()
    }

    for idx in 1...6 {
        let testID = (4 + idx)
        let monsters = (1 << idx)
        Benchmark("Send monster \(testID < 10 ? " " : "")#\(testID) - facade: distributed actor, remote (network) calls, array of \(monsters) monsters",
                  configuration: Benchmark.Configuration(scalingFactor: .kilo)) { benchmark in
            try await arrayOfMonsters(benchmark, monsters)
        }
    }

    Benchmark("Send Monster #11 - remote over network - 900 bytes",
              configuration: Benchmark.Configuration(scalingFactor: .kilo)) { benchmark in
        guard let sharedData else {
            fatalError("Shared data is not initialized")
        }

        var service = sharedData.service
        let client = sharedData.client
        let endpoints = sharedData.remoteEndpoints1

        do {
            let openRequest = _OpenRequestStruct(requestIdentifier: 1)

            try await endpoints.service.openStream(byRequest: OpenRequest(openRequest))
            await service.whenOpenStream()

            try await endpoints.client.streamOpened(StreamOpened(_StreamOpenedStruct(requestIdentifier: openRequest.id)))
            await client.whenStreamOpened()

            let stream = Stream(_StreamStruct(streamIdentifier: openRequest.id))

            let repeatString = Array("inventory".utf8)
            var inventory: [UInt8] = []
            for _ in 1 ... (900 / repeatString.count) {
                inventory.append(contentsOf: repeatString)
            }
            var monsterID: UInt64 = 1

            benchmark.startMeasurement()
            for _ in benchmark.scaledIterations {
                var monster = _MonsterStruct(identifier: monsterID)
                monster.inventory = inventory
                try await endpoints.client.handleMonster(Monster(monster), for: stream)
                monsterID += 1
            }

            try await endpoints.client.snapshotDone(for: stream)
            await client.whenSnapshotDone()

            benchmark.stopMeasurement()
        } catch {
            fatalError("Exception handled: \(error), stopping execution for this test")
        }

        service.reset()
        client.reset()
    }

    Benchmark("Send Monster #12 - facade: distributed actor, local calls, 1 thread",
              configuration: Benchmark.Configuration(scalingFactor: .mega)) { benchmark in
        guard let sharedData else {
            fatalError("Shared data is not initialized")
        }

        let endpoints = sharedData.localEndpoints

        do {
            benchmark.startMeasurement()
            for _ in benchmark.scaledIterations {
                try await endpoints.service.doNothing()
            }
            benchmark.stopMeasurement()
        } catch {
            fatalError("Exception handled: \(error), stopping execution for this test")
        }
    }

    Benchmark("Send Monster #13 - facade: distributed actor, local calls, 2 threads contention",
              configuration: Benchmark.Configuration(scalingFactor: .mega)) { benchmark in
        guard let sharedData else {
            fatalError("Shared data is not initialized")
        }

        let endpoints = sharedData.localEndpoints

        benchmark.startMeasurement()
        _ = await withTaskGroup(of: Void.self, returning: Void.self, body: { taskGroup in
            let tasks = 2
            for _ in 0 ..< tasks {
                taskGroup.addTask {
                    do {
                        for _ in 0 ..< benchmark.scaledIterations.count / tasks {
                            try await endpoints.service.doNothing()
                        }
                    } catch {
                        fatalError("\(error)")
                    }
                }
            }
            await taskGroup.waitForAll()
        })
        benchmark.stopMeasurement()
    }
}
