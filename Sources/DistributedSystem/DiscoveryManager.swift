// Copyright 2024 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import ConsulServiceDiscovery
import Helpers
import Logging
internal import NIOCore
import struct Foundation.UUID
import PackageConcurrencyHelpers

final class DiscoveryManager {
    private final class ProcessInfo {
        var channel: (UInt32, Channel)?
        var pendingHandlers = [String: [DistributedSystem.ConnectionHandler]]()
        var connectedServices = Set<String>()

        func addPendingHandlers(_ serviceName: String, _ handlers: [DistributedSystem.ConnectionHandler]) {
            self.pendingHandlers[serviceName, default: []].append(contentsOf: handlers)
        }

        func setChannel(_ channelID: UInt32, _ channel: Channel) -> [String: [DistributedSystem.ConnectionHandler]] {
            self.channel = (channelID, channel)
            connectedServices.formUnion(pendingHandlers.keys)
            var ret = [String: [DistributedSystem.ConnectionHandler]]()
            swap(&ret, &pendingHandlers)
            return ret
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
        var services: [UUID: ServiceInfo] = [:]
    }

    private var loggerBox: Box<Logger>
    private var logger: Logger { loggerBox.value }

    private var lock = Lock()
    private var processes: [SocketAddress: ProcessInfo] = [:]
    private var discoveries: [String: DiscoveryInfo] = [:]
    private var stopped = false

    init(_ loggerBox: Box<Logger>) {
        self.loggerBox = loggerBox
    }

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
            var services = [(ConsulServiceDiscovery.Instance, DistributedSystem.ChannelOrFactory)]()

            if cancellationToken.cancelled {
                return (true, false, addresses, services)
            }

            cancellationToken.serviceName = serviceName

            var discoveryInfo = self.discoveries[serviceName]
            if let discoveryInfo {
                for serviceInfo in discoveryInfo.services.values {
                    if serviceFilter(serviceInfo.service) {
                        switch serviceInfo.address {
                        case let .local(factory):
                            services.append((serviceInfo.service, .factory(factory)))
                        case let .remote(address):
                            if let processInfo = self.processes[address] {
                                if let (channelID, channel) = processInfo.channel {
                                    processInfo.connectedServices.insert(serviceName)
                                    services.append((serviceInfo.service, .channel(channelID, channel)))
                                } else {
                                    processInfo.addPendingHandlers(serviceName, [connectionHandler])
                                }
                            } else {
                                let processInfo = ProcessInfo()
                                processInfo.addPendingHandlers(serviceName, [connectionHandler])
                                self.processes[address] = processInfo
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
            guard let discoveryInfo else { fatalError("internal error: discoveryInfo unexpectedly nil") }
            discoveryInfo.filters[cancellationToken] = FilterInfo(serviceFilter, connectionHandler)
            return (false, discover, addresses, services)
        }

        if cancelled {
            logger.debug("discoverService[\(serviceName)]: cancelled before start \(cancellationToken.ptr)")
            return .cancelled
        } else {
            logger.debug("discoverService[\(serviceName)]: \(discover) \(addresses) \(services), cancellation token \(cancellationToken.ptr)")
            for (service, addr) in services {
                connectionHandler(service, addr)
            }
            return .started(discover, addresses)
        }
    }

    func cancel(_ token: DistributedSystem.CancellationToken) -> Bool {
        lock.withLock {
            if token.cancelled {
                logger.debug("token \(token.ptr) already cancelled")
                return false
            } else {
                token.cancelled = true
                if let serviceName = token.serviceName {
                    logger.debug("cancel token \(token.ptr)/\(serviceName)")
                    guard let discoveryInfo = self.discoveries[serviceName] else {
                        fatalError("Internal error: no discovery registered for '\(serviceName)'")
                    }
                    let removed = (discoveryInfo.filters.removeValue(forKey: token) != nil)
                    assert(removed)
                    return true
                } else {
                    logger.debug("cancel token \(token.ptr)")
                    return true
                }
            }
        }
    }

    func factoryFor(_ serviceName: String) -> DistributedSystem.ServiceFactory? {
        lock.withLock {
            guard let discoveryInfo = self.discoveries[serviceName] else {
                return nil
            }

            for entry in discoveryInfo.services {
                if case let .local(factory) = entry.value.address {
                    return factory
                }
            }

            return nil
        }
    }

    func addService(_ serviceName: String,
                    _ serviceID: UUID,
                    _ service: NodeService,
                    _ factory: @escaping DistributedSystem.ServiceFactory) -> Bool {
        let (updateHealthStatus, services) = lock.withLock {
            let updateHealthStatus = self.discoveries.isEmpty

            var discoveryInfo = self.discoveries[serviceName]
            if discoveryInfo == nil {
                discoveryInfo = DiscoveryInfo()
                self.discoveries[serviceName] = discoveryInfo
            }

            guard let discoveryInfo else { fatalError("discoveryInfo unexpectedly nil") }

            if discoveryInfo.services[serviceID] != nil {
                fatalError("Internal error: duplicated service \(serviceName)/\(serviceID)")
            }

            logger.debug("addService: \(serviceName)/\(serviceID)")
            discoveryInfo.services[serviceID] = ServiceInfo(service, factory)

            var services = [(ConsulServiceDiscovery.Instance, DistributedSystem.ConnectionHandler)]()
            for (_, filterInfo) in discoveryInfo.filters {
                if filterInfo.filter(service) {
                    services.append((service, filterInfo.connectionHandler))
                }
            }

            return (updateHealthStatus, services)
        }

        for (service, connectionHandler) in services {
            _ = connectionHandler(service, .factory(factory))
        }

        return updateHealthStatus
    }

    func setAddress(_ address: SocketAddress, for serviceName: String, _ serviceID: UUID, _ service: NodeService) -> Bool {
        let (connect, process) = lock.withLock { () -> (Bool, (channel: (UInt32, Channel), connectionHandlers: [DistributedSystem.ConnectionHandler])?) in
            guard let discoveryInfo = self.discoveries[serviceName] else {
                fatalError("Internal error: service \(serviceName) not discovered")
            }
            if let serviceInfo = discoveryInfo.services[serviceID] {
                if case let .remote(serviceAddress) = serviceInfo.address {
                    if serviceAddress != address {
                        logger.error("internal error: service \(serviceID) unexpectedly changed address from \(serviceAddress) to \(address)")
                    }
                    serviceInfo.service = service
                    serviceInfo.address = .remote(address)
                    discoveryInfo.services[serviceID] = serviceInfo
                }
                return (false, nil)
            } else {
                discoveryInfo.services[serviceID] = ServiceInfo(service, address)

                let connectionHandlers = discoveryInfo.filters.values.compactMap {
                    $0.filter(service) ? $0.connectionHandler : nil
                }

                if let processInfo = self.processes[address] {
                    if let channel = processInfo.channel {
                        if connectionHandlers.isEmpty {
                            return (false, nil)
                        } else {
                            processInfo.connectedServices.insert(serviceName)
                            return (false, (channel, connectionHandlers))
                        }
                    } else {
                        if !connectionHandlers.isEmpty {
                            processInfo.addPendingHandlers(serviceName, connectionHandlers)
                        }
                        return (false, nil)
                    }
                } else {
                    let processInfo = ProcessInfo()
                    if connectionHandlers.isEmpty {
                        self.processes[address] = processInfo
                        return (false, nil)
                    } else {
                        processInfo.addPendingHandlers(serviceName, connectionHandlers)
                        self.processes[address] = processInfo
                        return (true, nil)
                    }
                }
            }
        }

        if let process {
            for connectionHandler in process.connectionHandlers {
                connectionHandler(service, .channel(process.channel.0, process.channel.1))
            }
        }

        return connect
    }

    func removeService(_ serviceName: String, _ serviceID: UUID) {
        lock.withLockVoid {
            logger.trace("remove service \(serviceName)/\(serviceID)")
            guard let discoveryInfo = self.discoveries[serviceName] else {
                logger.error("internal error: no services \(serviceName) registered")
                return
            }
            if discoveryInfo.services.removeValue(forKey: serviceID) != nil {
                if discoveryInfo.services.isEmpty {
                    self.discoveries.removeValue(forKey: serviceName)
                }
            }
        }
    }

    func setChannel(_ channelID: UInt32,_ channel: Channel, forProcessAt address: SocketAddress) {
        let services = lock.withLock {
            var ret = [(NodeService, DistributedSystem.ConnectionHandler)]()
            guard let processInfo = self.processes[address] else {
                logger.error("Internal error: process for \(address) not found")
                return ret
            }

            let pendingHandlers = processInfo.setChannel(channelID, channel)
            for (serviceName, handlers) in pendingHandlers {
                if let discoveryInfo = self.discoveries[serviceName] {
                    let serviceInfo = discoveryInfo.services.values.first(where: { $0.address == .remote(address) })
                    if let serviceInfo {
                        ret.append(contentsOf: handlers.map { (serviceInfo.service, $0) })
                    } // else service disappeared from Consul, most probably connection will be closed very soon as well
                }
            }

            return ret
        }

        for (service, connectionHandler) in services {
            connectionHandler(service, .channel(channelID, channel))
        }
    }

    func stop() -> [NodeService] {
        lock.withLock {
            self.stopped = true
            return self.getLocalServicesLocked()
        }
    }

    func getLocalServices() -> [NodeService] {
        lock.withLock {
            self.getLocalServicesLocked()
        }
    }

    private func getLocalServicesLocked() -> [NodeService] {
        var services: [NodeService] = []
        for discoveryInfo in discoveries.values {
            for serviceInfo in discoveryInfo.services.values {
                if case .local = serviceInfo.address {
                    services.append(serviceInfo.service)
                }
            }
        }
        return services
    }

    func connectionEstablishmentFailed(_ address: SocketAddress) {
        lock.withLock {
            // If the service could change the address, we would
            // check it here, and if the address was changed while we tried
            // to connect, we would try to reconnect to the new address.
            // However, we register a service each time with a new service identifier (UUID),
            // so we assume each particular service instance will never change its address,
            // so logic described above is not required here.
            _ = self.processes.removeValue(forKey: address)
        }
    }

    // Returns 'true' if service still registered in the Consul,
    // so probably make sense for distributed system to try to reconnect.
    func channelInactive(_ address: SocketAddress) -> Bool {
        lock.withLock {
            if self.stopped {
                return false
            }

            guard let processInfo = self.processes[address] else { return false }

            var pendingHandlers = [String: [DistributedSystem.ConnectionHandler]]()

            for serviceName in processInfo.connectedServices {
                guard let discoveryInfo = self.discoveries[serviceName] else {
                    logger.error("internal error: service \(serviceName) not found")
                    continue
                }

                var servicePendingHandlers = [DistributedSystem.ConnectionHandler]()

                for serviceInfo in discoveryInfo.services.values {
                    if serviceInfo.address == .remote(address) {
                        let handlers = discoveryInfo.filters.values.compactMap {
                            $0.filter(serviceInfo.service) ? $0.connectionHandler : nil
                        }
                        servicePendingHandlers.append(contentsOf:  handlers)
                    }
                }

                if !servicePendingHandlers.isEmpty {
                    pendingHandlers[serviceName] = servicePendingHandlers
                }
            }

            if pendingHandlers.isEmpty {
                self.processes.removeValue(forKey: address)
                return false
            } else {
                processInfo.channel = nil
                processInfo.pendingHandlers = pendingHandlers
                processInfo.connectedServices.removeAll()
                return true
            }
        }
    }
}
