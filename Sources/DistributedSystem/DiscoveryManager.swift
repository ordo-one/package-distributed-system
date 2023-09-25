// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import PackageConcurrencyHelpers
import ConsulServiceDiscovery
import Helpers
import Logging
import NIOCore

final class DiscoveryManager {
    private final class ProcessInfo {
        let generation: Int
        var channel: Channel?
        var services = Set<String>()
        var connectionLossHandlers = [DistributedSystem.ConnectionLossHandler]()

        init(_ generation: Int) {
            self.generation = generation
        }
    }

    private enum ServiceAddress: Equatable {
        case local(DistributedSystem.ServiceFactory)
        case remote(SocketAddress)

        static func == (lhs: DiscoveryManager.ServiceAddress, rhs: DiscoveryManager.ServiceAddress) -> Bool {
            switch lhs {
            case let .remote(lhsAddress):
                if case let .remote(rhsAddress) = rhs {
                    return lhsAddress == rhsAddress
                } else {
                    return false
                }
            case .local:
                return false
            }
        }
    }

    private final class ServiceInfo {
        var service: NodeService
        var address: ServiceAddress

        private init(_ service: NodeService, _ address: ServiceAddress) {
            self.service = service
            self.address = address
        }

        convenience init(_ service: NodeService, _ address: SocketAddress) {
            self.init(service, .remote(address))
        }

        convenience init(_ service: NodeService, _ factory: @escaping DistributedSystem.ServiceFactory) {
            self.init(service, .local(factory))
        }
    }

    private struct FilterInfo {
        let filter: DistributedSystem.ServiceFilter
        let connectionHandler: DistributedSystem.ConnectionHandler

        init(_ filter: @escaping DistributedSystem.ServiceFilter, _ connectionHandler: @escaping DistributedSystem.ConnectionHandler) {
            self.filter = filter
            self.connectionHandler = connectionHandler
        }
    }

    private final class DiscoveryInfo {
        var discover = true
        var filters: [DistributedSystem.CancellationToken: FilterInfo] = [:]
        var services: [DistributedSystem.ServiceIdentifier: ServiceInfo] = [:]
    }

    private var logger: Logger { DistributedSystem.logger }

    private var lock = Lock()
    private var generation: Int = 0
    private var processes: [SocketAddress: ProcessInfo] = [:]
    private var discoveries: [String: DiscoveryInfo] = [:]

    init() {}

    enum DiscoverServiceResult {
        case cancelled
        case started(Bool, [SocketAddress])
    }

    func discoverService(_ serviceName: String,
                         _ serviceFilter: @escaping DistributedSystem.ServiceFilter,
                         _ connectionHandler: @escaping DistributedSystem.ConnectionHandler,
                         _ cancellationToken: DistributedSystem.CancellationToken) -> DiscoverServiceResult {
        let (cancelled, discover, addresses, services) = lock.withLock {
            if cancellationToken.serviceName != nil {
                fatalError("Internal error: cancellation token already used.")
            }
            var discover: Bool
            var addresses: [SocketAddress] = []
            var services = [(DistributedSystem.ServiceIdentifier, ConsulServiceDiscovery.Instance, (address: SocketAddress, generation: Int, channel: Channel)?)]()

            if cancellationToken.cancelled {
                return (true, false, addresses, services)
            }

            cancellationToken.serviceName = serviceName

            var discoveryInfo = self.discoveries[serviceName]
            if let discoveryInfo {
                for (serviceID, serviceInfo) in discoveryInfo.services {
                    if serviceFilter(serviceInfo.service) {
                        switch serviceInfo.address {
                        case .local:
                            services.append((serviceID, serviceInfo.service, nil))
                        case let .remote(address):
                            if let processInfo = self.processes[address] {
                                if let channel = processInfo.channel {
                                    processInfo.services.insert(serviceName)
                                    services.append((serviceID, serviceInfo.service, (address, processInfo.generation, channel)))
                                }
                            } else {
                                generation += 1
                                self.processes[address] = ProcessInfo(generation)
                                addresses.append(address)
                            }
                        }
                    }
                }
                discover = discoveryInfo.discover
                discoveryInfo.discover = false
            } else {
                discover = true
                discoveryInfo = DiscoveryInfo()
                self.discoveries[serviceName] = discoveryInfo
            }
            guard let discoveryInfo else { fatalError("Internal error: discoveryInfo unexpectedly nil") }
            discoveryInfo.filters[cancellationToken] = FilterInfo(serviceFilter, connectionHandler)
            return (false, discover, addresses, services)
        }

        if cancelled {
            logger.debug("discoverService[\(serviceName)]: cancelled \(Unmanaged.passUnretained(cancellationToken).toOpaque())")
            return .cancelled
        } else {
            logger.debug("discoverService[\(serviceName)]: \(discover) \(addresses) \(services), cancellation token \(Unmanaged.passUnretained(cancellationToken).toOpaque())")
            for (serviceID, service, process) in services {
                let connectionLossHandler = connectionHandler(serviceID, service, process?.channel)
                if let connectionLossHandler, let process {
                    addConnectionLossHandler(process.address, process.generation, connectionLossHandler)
                }
            }
            return .started(discover, addresses)
        }
    }

    private func addConnectionLossHandler(_ address: SocketAddress, _ generation: Int, _ connectionLossHandler: @escaping DistributedSystem.ConnectionLossHandler) {
        logger.debug("addConnectionLossHandler for process @ \(address):\(generation)")
        let added = lock.withLock {
            if let processInfo = self.processes[address], processInfo.generation == generation {
                processInfo.connectionLossHandlers.append(connectionLossHandler)
                return true
            }
            return false
        }
        if !added {
            connectionLossHandler()
        }
    }

    func cancel(_ token: DistributedSystem.CancellationToken) -> Bool {
        lock.withLock {
            if token.cancelled {
                logger.debug("Token \(Unmanaged.passUnretained(token).toOpaque()) already cancelled")
                return false
            } else {
                token.cancelled = true
                if let serviceName = token.serviceName {
                    logger.debug("Cancel token \(Unmanaged.passUnretained(token).toOpaque())/\(serviceName)")
                    guard let discoveryInfo = self.discoveries[serviceName] else {
                        fatalError("Internal error: no discovery registered for '\(serviceName)'")
                    }
                    return discoveryInfo.filters.removeValue(forKey: token) != nil
                } else {
                    logger.debug("Cancel token \(Unmanaged.passUnretained(token).toOpaque())")
                    return true
                }
            }
        }
    }

    func factoryFor(_ serviceName: String, _ serviceID: DistributedSystem.ServiceIdentifier) -> DistributedSystem.ServiceFactory? {
        lock.withLock {
            guard let discoveryInfo = self.discoveries[serviceName] else {
                return nil
            }

            guard let serviceInfo = discoveryInfo.services[serviceID] else {
                return nil
            }

            if case let .local(factory) = serviceInfo.address {
                return factory
            } else {
                return nil
            }
        }
    }

    func addService(_ serviceName: String,
                    _ serviceID: DistributedSystem.ServiceIdentifier,
                    _ service: NodeService,
                    _ factory: @escaping DistributedSystem.ServiceFactory) -> Bool {
        let (updateHealthStatus, services) = lock.withLock {
            let updateHealthStatus = (self.discoveries.count == 1)

            var discoveryInfo = self.discoveries[serviceName]
            if discoveryInfo == nil {
                discoveryInfo = DiscoveryInfo()
                self.discoveries[serviceName] = discoveryInfo
            }

            guard let discoveryInfo else {
                fatalError("Internal error: service \(serviceName) not discovered")
            }

            if discoveryInfo.services[serviceID] != nil {
                fatalError("Internal error: duplicated service \(serviceName)/\(serviceID)")
            }

            logger.debug("addService: \(serviceName)/\(serviceID)")
            discoveryInfo.services[serviceID] = ServiceInfo(service, factory)

            var services = [(DistributedSystem.ServiceIdentifier, ConsulServiceDiscovery.Instance, DistributedSystem.ConnectionHandler)]()
            for (_, filterInfo) in discoveryInfo.filters {
                if filterInfo.filter(service) {
                    services.append((serviceID, service, filterInfo.connectionHandler))
                }
            }

            return (updateHealthStatus, services)
        }

        for (serviceID, service, connectionHandler) in services {
             _ = connectionHandler(serviceID, service, nil)
             return updateHealthStatus
        }

        return updateHealthStatus
    }

    func setAddress(_ address: SocketAddress, for serviceName: String, _ serviceID: DistributedSystem.ServiceIdentifier, _ service: NodeService) -> Bool {
        let (connect, process) = lock.withLock { () -> (Bool, (channel: Channel, generation: Int, connectionHandlers: [DistributedSystem.ConnectionHandler])?) in
            guard let discoveryInfo = self.discoveries[serviceName] else {
                fatalError("Internal error: service \(serviceName) not discovered")
            }
            if let serviceInfo = discoveryInfo.services[serviceID] {
                // update only remote services
                if case .remote = serviceInfo.address {
                    serviceInfo.service = service
                    serviceInfo.address = .remote(address)
                }
                return (false, nil)
            } else {
                discoveryInfo.services[serviceID] = ServiceInfo(service, address)
                if let processInfo = self.processes[address] {
                    if let channel = processInfo.channel {
                        var connectionHandlers: [DistributedSystem.ConnectionHandler] = []
                        for (_, filterInfo) in discoveryInfo.filters {
                            if filterInfo.filter(service) {
                                processInfo.services.insert(serviceName)
                                connectionHandlers.append(filterInfo.connectionHandler)
                            }
                        }
                        return (false, (channel, processInfo.generation, connectionHandlers))
                    } else {
                        return (false, nil)
                    }
                } else if serviceName != PingServiceEndpoint.serviceName {
                    generation += 1
                    let processInfo = ProcessInfo(generation)
                    processInfo.services.insert(serviceName)
                    self.processes[address] = processInfo
                    return (true, nil)
                } else {
                    return (false, nil)
                }
            }
        }

        if let process {
            for connectionHandler in process.connectionHandlers {
                let connectionLossHandler = connectionHandler(serviceID, service, process.channel)
                if let connectionLossHandler {
                    addConnectionLossHandler(address, generation, connectionLossHandler)
                }
            }
        }

        return connect
    }

    func setChannel(_ channel: Channel, forProcessAt address: SocketAddress) {
        let services = lock.withLock {
            guard let processInfo = processes[address] else {
                fatalError("Internal error: process for \(address) not found")
            }
            processInfo.channel = channel

            if let discoveryInfo = self.discoveries[PingServiceEndpoint.serviceName] {
                for (_, serviceInfo) in discoveryInfo.services {
                    if case let .remote(serviceAddress) = serviceInfo.address, serviceAddress == address {
                        processInfo.services.insert(PingServiceEndpoint.serviceName)
                        break
                    }
                }
            } else {
                fatalError("Internal error: \(PingServiceEndpoint.serviceName) not discovered")
            }

            var services = [(DistributedSystem.ServiceIdentifier, ConsulServiceDiscovery.Instance, Int, DistributedSystem.ConnectionHandler)]()
            for serviceName in processInfo.services {
                guard let discoveryInfo = self.discoveries[serviceName] else {
                    fatalError("Internal error: service \(serviceName) not found")
                }
                for (serviceID, serviceInfo) in discoveryInfo.services {
                    if case let .remote(serviceAddress) = serviceInfo.address, serviceAddress == address {
                        for (_, filterInfo) in discoveryInfo.filters {
                            if filterInfo.filter(serviceInfo.service) {
                                services.append((serviceID, serviceInfo.service, processInfo.generation, filterInfo.connectionHandler))
                            }
                        }
                    }
                }
            }
            return services
        }

        for (serviceID, service, generation, connectionHandler) in services {
            let connectionLossHandler = connectionHandler(serviceID, service, channel)
            if let connectionLossHandler {
                addConnectionLossHandler(address, generation, connectionLossHandler)
            }
        }
    }

    func getLocalServices() -> [DistributedSystem.ServiceIdentifier] {
        lock.withLock {
            var services: [DistributedSystem.ServiceIdentifier] = []
            for (_, discoveryInfo) in self.discoveries {
                for (serviceID, serviceInfo) in discoveryInfo.services {
                    if case .local = serviceInfo.address {
                        services.append(serviceID)
                    }
                }
            }
            return services
        }
    }

    func connectionEstablishmentFailed(_ address: SocketAddress) {
        lock.withLockVoid {
            let processInfo = self.processes.removeValue(forKey: address)
            if let processInfo {
                assert(processInfo.channel == nil)
            }
        }
    }

    func channelInactive(_ channel: Channel) {
        guard let address = channel.remoteAddress else {
            fatalError("Internal error: channel not connected")
        }

        let connectionLossHandlers = lock.withLock {
            self.processes.removeValue(forKey: address)?.connectionLossHandlers ?? []
        }

        for connectionLossHandler in connectionLossHandlers {
            connectionLossHandler()
        }
    }
}
