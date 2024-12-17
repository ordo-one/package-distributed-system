// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import ConsulServiceDiscovery
import Distributed

internal import enum NIOHTTP1.HTTPResponseStatus
import class Foundation.ProcessInfo
import struct Foundation.UUID
import Logging
import NIOCore
internal import NIOPosix
import class ServiceDiscovery.CancellationToken

public class DistributedSystemServer: DistributedSystem {
    private var localAddress: String?
    private var localPort: Int?
    private var serviceDiscoveryCancellationToken: ServiceDiscovery.CancellationToken?

    private static let nanosecondsInSecond: Int64 = 1_000_000_000
    let criticalServiceDeregisterTimeout = TimeAmount.seconds(60) // minimum in Consul is 60 seconds
    var healthStatusUpdateInterval = TimeAmount.seconds(10)
    var healthStatusTTL = TimeAmount.seconds(15)

    private static func localAddress(_ consulAddress: String) throws -> String? {
        var hints = addrinfo()
        hints.ai_family = AF_INET
        hints.ai_socktype = SOCK_DGRAM
        var addrinfo = UnsafeMutablePointer<addrinfo>(nil)
        var rc = getaddrinfo(consulAddress, "domain", &hints, &addrinfo)
        if rc != 0 {
            throw DistributedSystemErrors.error("getaddrinfo('\(consulAddress)') failed: \(rc)")
        }

        defer { freeaddrinfo(addrinfo) }

        guard let addrinfo else {
            throw DistributedSystemErrors.error("getaddrinfo('\(consulAddress)') returned empty list")
        }

        let socket = socket(AF_INET, SOCK_DGRAM, 0)
        if socket < 0 {
            throw DistributedSystemErrors.error("socket() failed: \(errno)")
        }

        defer { close(socket) }

        rc = connect(socket, addrinfo.pointee.ai_addr, addrinfo.pointee.ai_addrlen)
        if rc != 0 {
            throw DistributedSystemErrors.error("connect() failed: \(errno)")
        }

        var localAddr = sockaddr_in()
        rc = withUnsafeMutablePointer(to: &localAddr) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                var len = socklen_t(MemoryLayout<sockaddr_in>.size)
                return getsockname(socket, $0, &len)
            }
        }

        if rc != 0 {
            throw DistributedSystemErrors.error("getsockname() failed: \(errno)")
        }

        if localAddr.sin_len == addrinfo.pointee.ai_addrlen {
            let sameHost = addrinfo.pointee.ai_addr.withMemoryRebound(to: sockaddr_in.self, capacity: 1) {
                localAddr.sin_addr.s_addr == $0.pointee.sin_addr.s_addr
            }

            if sameHost {
                return nil
            }
        }

        let bufferSize = Int((localAddr.sin_family == AF_INET) ? INET_ADDRSTRLEN : INET6_ADDRSTRLEN)
        let buffer = UnsafeMutablePointer<CChar>.allocate(capacity: bufferSize)
        defer { buffer.deallocate() }
        let ptr = inet_ntop(Int32(localAddr.sin_family), &localAddr.sin_addr, buffer, socklen_t(bufferSize))
        guard let ptr else {
            throw DistributedSystemErrors.error("inet_ntop() failed: \(errno)")
        }
        return String(cString: ptr)
    }

    public func start(at address: NetworkAddress = NetworkAddress.anyAddress) async throws {
        try super.start()

        let localAddress = try Self.localAddress(consul.serverHost)
        if let localAddress {
            logger.info("registering in the Consul agent @ \(consul.serverHost) with address \(localAddress)")
        } else {
            logger.info("registering in the local Consul agent")
        }
        self.localAddress = localAddress

        let serverChannel = try await ServerBootstrap(group: eventLoopGroup)
            .serverChannelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                let writeBufferWaterMark = ChannelOptions.Types.WriteBufferWaterMark(
                    low: Int(self.endpointQueueWarningSize/2),
                    high: Int(self.endpointQueueWarningSize))
                _ = channel.setOption(ChannelOptions.writeBufferWaterMark, value: writeBufferWaterMark)
                let pipeline = channel.pipeline
                let channelHandler = ChannelHandler(self.nextChannelID, self, nil, self.endpointQueueWarningSize)
                return pipeline.addHandler(ChannelCounters(self), name: ChannelCounters.name).flatMap { _ in
                    pipeline.addHandler(ChannelHandshakeServer(self, channelHandler), name: ChannelHandshakeServer.name)
                }
            }
            .bind(host: address.host, port: address.port)
            .get()

        guard let channelLocalAddress = serverChannel.localAddress else {
            throw DistributedSystemErrors.error("Can't evaluate local address")
        }

        guard let localPort = channelLocalAddress.port else {
            throw DistributedSystemErrors.error("Invalid local address \(channelLocalAddress)")
        }

        self.localPort = localPort

        loggerBox.value[metadataKey: "port"] = Logger.MetadataValue(stringLiteral: "\(localPort)")
        logger.info("starting server '\(systemName)' @ \(localPort) (compression mode = \(compressionMode))")
    }

    private func registerService(_ serviceName: String, _ serviceID: UUID, metadata: [String: String]) -> EventLoopFuture<Void> {
        // Use TTL type service health check
        // One could think we could use TCP,
        // but it is not a good idea when register services with dynamically allocated ports.
        // Service can crash, then OS will allocate the same port for another service,
        // consul will be able to connect to that port and report that service as 'passing'
        // while in the reality the service will be down
        let check = Check(checkID: "service:\(serviceID)",
                          deregisterCriticalServiceAfter: "\(criticalServiceDeregisterTimeout.nanoseconds / Self.nanosecondsInSecond)s",
                          name: "service:\(serviceID)",
                          status: .passing,
                          ttl: "\(healthStatusTTL.nanoseconds / Self.nanosecondsInSecond)s")
        let service = Service(address: localAddress, checks: [check], id: "\(serviceID)", meta: metadata, name: serviceName, port: localPort)
        return consul.agent.registerService(service)
    }

    override public func stop() {
        if let serviceDiscoveryCancellationToken {
            serviceDiscoveryCancellationToken.cancel()
            self.serviceDiscoveryCancellationToken = nil
        }
        super.stop()
    }

    public func addService(
        ofType type: any ServiceEndpoint.Type,
        toModule moduleID: ModuleIdentifier,
        metadata: [String: String]? = nil,
        _ factory: @escaping ServiceFactory
    ) async throws {
        try await addService(name: type.serviceName, toModule: moduleID, metadata: metadata, factory)
    }

    private func updateHealthStatus(with eventLoop: EventLoop) {
        let services = getLocalServices()
        logger.trace("update health status for \(services.count) services")

        for service in services {
            let checkID = "service:\(service.serviceID)"
            let future = consul.agent.check(checkID, status: .passing)
            future.whenFailure { error in
                if let error = error as? ConsulError,
                   case let .httpResponseError(status) = error,
                   case status = HTTPResponseStatus.notFound {
                    if let serviceName = service.serviceName,
                       let serviceID = UUID(uuidString: service.serviceID) {
                        self.logger.error("check '\(checkID)' failed: \(error), register service again")
                        let serviceMeta = service.serviceMeta ?? [:]
                        let future = self.registerService(serviceName, serviceID, metadata: serviceMeta)
                        future.whenFailure { error in
                            self.logger.error("failed to register service \(serviceName)/\(serviceID): \(error)")
                        }
                    } else {
                        self.logger.error("check '\(checkID)' failed: \(error), can't register service \(service)")
                    }
                } else {
                    self.logger.error("check '\(checkID)' failed: \(error)")
                }
            }
        }

        eventLoop.scheduleTask(in: healthStatusUpdateInterval) {
            self.updateHealthStatus(with: eventLoop)
        }
    }

    public func addService(
        name: String,
        toModule moduleID: ModuleIdentifier,
        metadata: [String: String]? = nil,
        _ factory: @escaping ServiceFactory
    ) async throws {
        var metadata = metadata ?? [:]
        metadata[ServiceMetadata.systemName.rawValue] = systemName
        metadata[ServiceMetadata.processIdentifier.rawValue] = String(ProcessInfo.processInfo.processIdentifier)
        metadata[ServiceMetadata.moduleIdentifier.rawValue] = String(moduleID.rawValue)

        let (serviceID, updateHealthStatus) = super.addService(name, metadata, factory)
        let future = registerService(name, serviceID, metadata: metadata)

        // if super.addService() requested health status update
        // it is still better to schedule it even if service registration failed,
        // because super.addService() will never request health status update again

        if updateHealthStatus {
            let eventLoop = eventLoopGroup.next()
            eventLoop.scheduleTask(in: healthStatusUpdateInterval) {
                self.updateHealthStatus(with: eventLoop)
            }
        }

        try await future.get()
    }
}
