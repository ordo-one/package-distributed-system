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
internal import class Foundation.ProcessInfo
internal import struct Foundation.UUID
import Logging
internal import NIOCore
internal import NIOPosix
import class ServiceDiscovery.CancellationToken

public class DistributedSystemServer: DistributedSystem {
    private var localAddress: SocketAddress!
    private var serviceDiscoveryCancellationToken: ServiceDiscovery.CancellationToken?

    private static let nanosecondsInSecond: Int64 = 1_000_000_000
    let criticalServiceDeregisterTimeout = TimeAmount.seconds(60) // minimum in Consul is 60 seconds
    var healthStatusUpdateInterval = TimeAmount.seconds(10)
    var healthStatusTTL = TimeAmount.seconds(15)

    public func start(at address: NetworkAddress = NetworkAddress.anyAddress) async throws {
        try super.start()

        let serverChannel = try await ServerBootstrap(group: eventLoopGroup)
            .serverChannelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                let pipeline = channel.pipeline
                let streamHandler = ByteToMessageHandler(StreamDecoder(self.loggerBox))
                return pipeline.addHandler(ChannelCompressionHandshakeServer(self.loggerBox, streamHandler)).flatMap { _ in
                    pipeline.addHandler(streamHandler).flatMap { _ in
                        pipeline.addHandler(ChannelHandler(self.nextChannelID, self, nil, self.endpointQueueWarningSize)).flatMap { _ in
                            pipeline.addHandler(ChannelOutboundCounter(self), position: .first)
                        }
                    }
                }
            }
            .bind(host: address.host, port: address.port)
            .get()

        guard let localAddress = serverChannel.localAddress else {
            throw DistributedSystemErrors.error("Can't evaluate local address")
        }

        self.localAddress = localAddress

        guard let portNumber = localAddress.port else {
            throw DistributedSystemErrors.error("Invalid local address (\(#file):\(#line))")
        }

        loggerBox.value[metadataKey: "port"] = Logger.MetadataValue(stringLiteral: "\(portNumber)")
        logger.debug("starting server '\(systemName)' @ \(portNumber)")
    }

    private func registerService(_ serviceName: String, _ serviceID: UUID, metadata: [String: String]) -> EventLoopFuture<Void> {
        // Use TTL type service health check
        // One could think we could use TCP,
        // but it is not a good idea when register services with dynamically allocated ports.
        // Service can crash, then OS will allocate the same port for another service,
        // consul will be able to connect to that port and report that service as 'passing'
        // while in the reality the service will be down
        guard let port = localAddress.port else {
            fatalError("Unsupported socket address type")
        }
        let check = Check(checkID: "service:\(serviceID)",
                          deregisterCriticalServiceAfter: "\(criticalServiceDeregisterTimeout.nanoseconds / Self.nanosecondsInSecond)s",
                          name: "service:\(serviceID)",
                          status: .passing,
                          ttl: "\(healthStatusTTL.nanoseconds / Self.nanosecondsInSecond)s")
        let service = Service(checks: [check], id: "\(serviceID)", meta: metadata, name: serviceName, port: port)
        return consul.agent.registerService(service)
    }

    override public func stop() {
        if let serviceDiscoveryCancellationToken {
            serviceDiscoveryCancellationToken.cancel()
            self.serviceDiscoveryCancellationToken = nil
        }
        super.stop()
    }

    public func addService(ofType type: any ServiceEndpoint.Type,
                           toModule moduleID: ModuleIdentifier,
                           metadata: [String: String]? = nil,
                           _ factory: @escaping ServiceFactory) async throws {
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

    public func addService(name: String,
                           toModule moduleID: ModuleIdentifier,
                           metadata: [String: String]? = nil,
                           _ factory: @escaping ServiceFactory) async throws {
        var metadata = metadata ?? [:]
        metadata[ServiceMetadata.systemName.rawValue] = systemName
        metadata[ServiceMetadata.processIdentifier.rawValue] = String(ProcessInfo.processInfo.processIdentifier)
        metadata[ServiceMetadata.moduleIdentifier.rawValue] = String(moduleID.rawValue)
        let (serviceID, updateHealthStatus) = super.addService(name, metadata, factory)
        let future = registerService(name, serviceID, metadata: metadata)
        try await future.get()
        if updateHealthStatus {
            let eventLoop = eventLoopGroup.next()
            eventLoop.scheduleTask(in: healthStatusUpdateInterval) {
                self.updateHealthStatus(with: eventLoop)
            }
        }
    }
}
