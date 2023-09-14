import Benchmark
import Dispatch
import Distributed
import DistributedSystem
import DistributedSystemConformance
import class Foundation.ProcessInfo
import Frostflake
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
    private let serviceSystem: DistributedSystem
    private let clientSystem: DistributedSystem
    let service: Service
    let localClient: Client
    let localClientEndpoints: Endpoints
    let remoteClient: Client
    let remoteClientEndpoints: Endpoints

    init(_ serviceSystem: DistributedSystem,
         _ clientSystem: DistributedSystem,
         _ service: Service,
         _ localClient: Client,
         _ localClientEndpoints: Endpoints,
         _ remoteClient: Client,
         _ remoteClientEndpoints: Endpoints) {
        self.serviceSystem = serviceSystem
        self.clientSystem = clientSystem
        self.service = service
        self.localClient = localClient
        self.localClientEndpoints = localClientEndpoints
        self.remoteClient = remoteClient
        self.remoteClientEndpoints = remoteClientEndpoints
    }

    func getClientAndEndpoints(remote: Bool) async throws -> (Client, Endpoints) {
        remote ? (remoteClient, remoteClientEndpoints) : (localClient, localClientEndpoints)
    }

    func shutdown() {
        serviceSystem.stop()
        clientSystem.stop()
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

        var streamContinuation: AsyncStream<TestClientEndpoint>.Continuation?
        let stream = AsyncStream<TestClientEndpoint>() { streamContinuation = $0 }
        var streamIterator = stream.makeAsyncIterator()

        try await serviceSystem.addService(ofType: TestServiceEndpoint.self, toModule: DistributedSystem.ModuleIdentifier(1)) { actorSystem in
            let serviceEndoint = try TestServiceEndpoint(service, in: actorSystem)
            let clientEndpoint = try TestClientEndpoint.resolve(id: serviceEndoint.id.makeClientEndpoint() , using: actorSystem)
            streamContinuation?.yield(clientEndpoint)
            return serviceEndoint
        }

        let clientSystem = DistributedSystem(systemName: systemName)
        try clientSystem.start()

        let localClient = Client(logger, label: "local")
        let localServiceEndpoint = try await serviceSystem.connectToService(
            TestServiceEndpoint.self,
            withFilter: { _ in true },
            clientFactory: { actorSystem in
                TestClientEndpoint(localClient, in: actorSystem)
            }
        )
        let localClientEndpoint = await streamIterator.next()
        guard let localClientEndpoint else {
            fatalError("localClientEndpoint unexpectedly nil")
        }

        let remoteClient = Client(logger, label: "remote")
        let remoteServiceEndpoint = try await clientSystem.connectToService(
            TestServiceEndpoint.self,
            withFilter: { _ in true },
            clientFactory: { actorSystem in
                TestClientEndpoint(remoteClient, in: actorSystem)
            }
        )
        let remoteClientEndpoint = await streamIterator.next()
        guard let remoteClientEndpoint else {
            fatalError("remoteClientEndpoint unexpectedly nil")
        }

        streamContinuation?.finish()

        sharedData = .init(serviceSystem,
                           clientSystem,
                           service,
                           localClient,
                           Endpoints(localClientEndpoint, localServiceEndpoint),
                           remoteClient,
                           Endpoints(remoteClientEndpoint, remoteServiceEndpoint))
    }

    Benchmark.shutdownHook = {
        if let sharedData {
            sharedData.shutdown()
        } else {
            fatalError("Shared data is not initialized")
        }
        sharedData = nil
    }

    Frostflake.setup(sharedGenerator: Frostflake(generatorIdentifier: 1_000))

    func getFrostflake(_ forIteration: Int = 0) -> Frostflake {
        Frostflake(generatorIdentifier: UInt16(forIteration % (Int(1 << Frostflake.generatorIdentifierBits) - 1)))
    }

    Benchmark("Send Monster  #1 - facade: class, local calls",
              configuration: Benchmark.Configuration(scalingFactor: .kilo)) { benchmark in
        guard let sharedData else {
            fatalError("Shared data is not initialized")
        }

        let serverClass = ServiceClass(sharedData.service)
        let clientClass = ClientClass(sharedData.localClient)

        let frostflake = getFrostflake(benchmark.currentIteration)

        do {
            let openRequest = _OpenRequestStruct(requestIdentifier: 1)

            try await serverClass.openStream(byRequest: OpenRequest(openRequest))

            try await clientClass.streamOpened(StreamOpened(_StreamOpenedStruct(requestIdentifier: openRequest.id)))

            let stream = Stream(_StreamStruct(streamIdentifier: openRequest.id))

            benchmark.startMeasurement()
            for _ in benchmark.scaledIterations {
                let monster = _MonsterStruct(identifier: frostflake.generate())
                try await clientClass.handleMonster(Monster(monster), for: stream)
            }

            try await clientClass.snapshotDone(for: stream)

            benchmark.stopMeasurement()
        } catch {
            fatalError("Exception handled: \(error), stopping execution for this test")
        }
    }

    Benchmark("Send Monster  #2 - facade: actor, local calls",
              configuration: Benchmark.Configuration(scalingFactor: .kilo)) { benchmark in
        guard let sharedData else {
            fatalError("Shared data is not initialized")
        }

        let serverActor = ServiceActor(sharedData.service)
        let clientActor = ClientActor(sharedData.localClient)

        let frostflake = getFrostflake(benchmark.currentIteration)

        do {
            let openRequest = _OpenRequestStruct(requestIdentifier: 1)

            try await serverActor.openStream(byRequest: OpenRequest(openRequest))

            try await clientActor.streamOpened(StreamOpened(_StreamOpenedStruct(requestIdentifier: openRequest.id)))

            let stream = Stream(_StreamStruct(streamIdentifier: openRequest.id))

            benchmark.startMeasurement()
            for _ in benchmark.scaledIterations {
                let monster = _MonsterStruct(identifier: frostflake.generate())
                try await clientActor.handleMonster(Monster(monster), for: stream)
            }

            try await clientActor.snapshotDone(for: stream)

            benchmark.stopMeasurement()
        } catch {
            fatalError("Exception handled: \(error), stopping execution for this test")
        }
    }

    Benchmark("Send Monster  #3 - facade: distributed actor, local calls",
              configuration: Benchmark.Configuration(scalingFactor: .kilo)) { benchmark in
        guard let sharedData else {
            fatalError("Shared data is not initialized")
        }

        let (client, endpoints) = try await sharedData.getClientAndEndpoints(remote: false)
        var service = sharedData.service

        let frostflake = getFrostflake(benchmark.currentIteration)

        do {
            let openRequest = _OpenRequestStruct(requestIdentifier: 1)

            try await endpoints.service.openStream(byRequest: OpenRequest(openRequest))
            await service.whenOpenStream()

            try await endpoints.client.streamOpened(StreamOpened(_StreamOpenedStruct(requestIdentifier: openRequest.id)))
            await client.whenStreamOpened()

            let stream = Stream(_StreamStruct(streamIdentifier: openRequest.id))

            benchmark.startMeasurement()
            for _ in benchmark.scaledIterations {
                let monster = _MonsterStruct(identifier: frostflake.generate())
                try await endpoints.client.handleMonster(Monster(monster), for: stream)
            }

            try await endpoints.client.snapshotDone(for: stream)
            await client.whenSnapshotDone()

            benchmark.stopMeasurement()

            client.reset()
            service.reset()
        } catch {
            fatalError("Exception handled: \(error), stopping execution for this test")
        }
    }

    Benchmark("Send Monster  #4 - facade: distributed actor, remote (network) calls",
              configuration: Benchmark.Configuration(scalingFactor: .kilo)) { benchmark in
        guard let sharedData else {
            fatalError("Shared data is not initialized")
        }

        let (client, endpoints) = try await sharedData.getClientAndEndpoints(remote: true)
        var service = sharedData.service

        let frostflake = getFrostflake(benchmark.currentIteration)

        do {
            let openRequest = _OpenRequestStruct(requestIdentifier: 1)

            try await endpoints.service.openStream(byRequest: OpenRequest(openRequest))
            await service.whenOpenStream()

            try await endpoints.client.streamOpened(StreamOpened(_StreamOpenedStruct(requestIdentifier: openRequest.id)))
            await client.whenStreamOpened()

            let stream = Stream(_StreamStruct(streamIdentifier: openRequest.id))

            benchmark.startMeasurement()
            for _ in benchmark.scaledIterations {
                let monster = _MonsterStruct(identifier: frostflake.generate())
                try await endpoints.client.handleMonster(Monster(monster), for: stream)
            }

            try await endpoints.client.snapshotDone(for: stream)
            await client.whenSnapshotDone()

            benchmark.stopMeasurement()

            client.reset()
            service.reset()
        } catch {
            fatalError("Exception handled: \(error), stopping execution for this test")
        }
    }

    func arrayOfMonsters(_ benchmark: Benchmark, _ arraySize: Int) async throws {
        guard let sharedData else {
            fatalError("Shared data is not initialized")
        }

        let (client, endpoints) = try await sharedData.getClientAndEndpoints(remote: true)
        var service = sharedData.service

        let frostflake = getFrostflake(benchmark.currentIteration)

        do {
            let openRequest = _OpenRequestStruct(requestIdentifier: 1)

            try await endpoints.service.openStream(byRequest: OpenRequest(openRequest))
            await service.whenOpenStream()

            try await endpoints.client.streamOpened(StreamOpened(_StreamOpenedStruct(requestIdentifier: openRequest.id)))
            await client.whenStreamOpened()

            let stream = Stream(_StreamStruct(streamIdentifier: openRequest.id))

            let iterations = benchmark.scaledIterations.count / arraySize

            benchmark.startMeasurement()
            for _ in 1...iterations {
                var monsters = [Monster]()
                for _ in 1...arraySize {
                    let monster = _MonsterStruct(identifier: frostflake.generate())
                    monsters.append(Monster(monster))
                }
                try await endpoints.client.handleMonsters(monsters, for: stream)
            }

            try await endpoints.client.snapshotDone(for: stream)
            await client.whenSnapshotDone()

            benchmark.stopMeasurement()

            client.reset()
            service.reset()
        } catch {
            fatalError("Exception handled: \(error), stopping execution for this test")
        }
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

        let (client, endpoints) = try await sharedData.getClientAndEndpoints(remote: true)
        var service = sharedData.service

        let frostflake = getFrostflake(benchmark.currentIteration)

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

            benchmark.startMeasurement()
            for _ in benchmark.scaledIterations {
                var monster = _MonsterStruct(identifier: frostflake.generate())
                monster.inventory = inventory
                try await endpoints.client.handleMonster(Monster(monster), for: stream)
            }

            try await endpoints.client.snapshotDone(for: stream)
            await client.whenSnapshotDone()

            benchmark.stopMeasurement()

            client.reset()
            service.reset()
        } catch {
            fatalError("Exception handled: \(error), stopping execution for this test")
        }
    }

    Benchmark("Send Monster #12 - facade: distributed actor, local calls, 1 thread",
              configuration: Benchmark.Configuration(scalingFactor: .mega)) { benchmark in
        guard let sharedData else {
            fatalError("Shared data is not initialized")
        }

        let (_, endpoints) = try await sharedData.getClientAndEndpoints(remote: false)

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

        let (_, endpoints) = try await sharedData.getClientAndEndpoints(remote: false)

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
