// Copyright 2023 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

import ConsulServiceDiscovery
import Distributed
import DistributedSystemConformance
import class Foundation.ProcessInfo
import Frostflake
import Logging
import NIOCore
import NIOPosix
import class ServiceDiscovery.CancellationToken

public class DistributedSystemServer: DistributedSystem {
    static let healthStatusUpdateInterval = TimeAmount.seconds(10)
    static let healthStatusTTL = "15s"

    private var localAddress: SocketAddress!
    private var serviceDiscoveryCancellationToken: ServiceDiscovery.CancellationToken?

    public func start(at address: NetworkAddress = NetworkAddress.anyAddress) async throws {
        try super.start()

        let serverChannel = try await ServerBootstrap(group: eventLoopGroup)
            .serverChannelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                channel.pipeline.addHandler(ByteToMessageHandler(StreamDecoder(self.loggerBox))).flatMap { _ in
                    channel.pipeline.addHandler(ChannelHandler(self, nil))
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

        // Ping service
        let metadata = [
            ServiceMetadata.systemName.rawValue: systemName,
            ServiceMetadata.processIdentifier.rawValue: String(ProcessInfo.processInfo.processIdentifier)
        ]

        let serviceID = super.addService(PingServiceEndpoint.serviceName, metadata) { actorSystem in
            try PingServiceEndpoint(actorSystem: actorSystem)
        }

        try await registerService(PingServiceEndpoint.serviceName, serviceID, metadata: metadata)
    }

    private func registerService(_ serviceName: String, _ serviceID: ServiceIdentifier, metadata: [String: String]) async throws {
        // Use TTL type service health check
        // One could think we could use TCP,
        // but it is not a good idea when register services with dynamically allocated ports.
        // Service can crash, then OS will allocate the same port for another service,
        // consul will be able to connect to that port and report that service as 'passing'
        // while in the reality the service will be down
        guard let port = localAddress.port else {
            fatalError("Unsupported socket address type")
        }
        let check = Check(checkID: "service:\(serviceID)", deregisterCriticalServiceAfter: "1m", status: .passing, ttl: Self.healthStatusTTL)
        let service = Service(checks: [check], id: "\(serviceID)", meta: metadata, name: serviceName, port: port)
        let registerFuture = consul.agent.registerService(service)
        try await registerFuture.get()
    }

    override public func stop() {
        if let serviceDiscoveryCancellationToken {
            serviceDiscoveryCancellationToken.cancel()
            self.serviceDiscoveryCancellationToken = nil
        }
        super.stop()
    }

    public func addService(ofType type: any ServiceEndpoint.Type,
                           toModule moduleID: DistributedSystem.ModuleIdentifier,
                           metadata: [String: String]? = nil,
                           _ factory: @escaping (DistributedSystem) throws -> any DistributedActor) async throws {
        try await addService(name: type.serviceName, toModule: moduleID, metadata: metadata) { actorSystem in
            try (factory(actorSystem), nil)
        }
    }

    public func addService(ofType type: any ServiceEndpoint.Type,
                           toModule moduleID: DistributedSystem.ModuleIdentifier,
                           metadata: [String: String]? = nil,
                           _ factory: @escaping ServiceFactory) async throws {
        try await addService(name: type.serviceName, toModule: moduleID, metadata: metadata, factory)
    }

    public func addService(name: String,
                           toModule moduleID: DistributedSystem.ModuleIdentifier,
                           _ factory: @escaping (DistributedSystem) throws -> any DistributedActor) async throws {
        try await addService(name: name, toModule: moduleID, metadata: nil) { actorSystem in
            try (factory(actorSystem), nil)
        }
    }

    public func addService(name: String,
                           toModule moduleID: DistributedSystem.ModuleIdentifier,
                           metadata: [String: String]? = nil,
                           _ factory: @escaping ServiceFactory) async throws {
        var metadata = metadata ?? [:]
        metadata[ServiceMetadata.systemName.rawValue] = systemName
        metadata[ServiceMetadata.processIdentifier.rawValue] = String(ProcessInfo.processInfo.processIdentifier)
        metadata[ServiceMetadata.moduleIdentifier.rawValue] = String(moduleID.rawValue)
        let serviceID = super.addService(name, metadata, factory)
        try await registerService(name, serviceID, metadata: metadata)
    }
}
