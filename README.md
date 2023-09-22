# package-distributed-system

The main purpose of the library is to simplify the creation of distributed computing systems. During the development of the system, a set of services and the semantics of the interaction of services with each other are determined. The structure of the system (the number of services and their distribution across processes and physical machines) is determined by the configuration, which makes it easy to scale the computing system by adding or removing services, processes and servers.

Consul is used as a service discovery mechanism.
TCP/IP and swift-nio are used to transfer data between processes.

The basic element of the system is a module that combines several different services. Each service of the system is assigned the distributed actor type. Services in the system are registered programmatically, during registration it is determined to which module the service belongs.

## Register service
In order to register a service in the distributed system the function `addService` supposed to be used:
```
   func addService(_ serviceName: String,
                    _ metadata: [String: String],
                    _ factory: @escaping ServiceFactory) -> ServiceIdentifier
```
where
- serviceName: name of the service
- metadata: additional key/value pairs to be propagated to the consul service metadata
- factory: factory to be used by distributed system when some request a new service instance
After calling the function, the service will be registered in the distributed system and other components of the system will be able to connect to this service and work with it.

## Use service
In order to use the service, you need to get it through the distributed system. The basic mechanism of receiving the service:

```
func connectToServices<S: ServiceEndpoint, C>(
        _ serviceEndpointType: S.Type,
        withFilter serviceFilter: @escaping ServiceFilter,
        clientFactory: ((DistributedSystem) -> C)? = nil,
        serviceHandler: @escaping (S, ConsulServiceDiscovery.Instance) -> ConnectionLossHandler?,
        cancellationToken: CancellationToken? = nil)
```
where
- `serviceFilter`: filter closure, giving a possibility to filter out services which are not interesting
- `clientFactory`: factory for creating a response handler from the service
- ` serviceHandler`: factory for creating a handler for loss of connection with the service
- `cancellationToken`: a token through which you can stop the connection

Calling the function starts the procedure for searching for services in the same system. For each service found, filter closure will be called, and if it returns true, then the distributed system will establish a connection with the process in which the service is running. After the connection is established, the factory will be called to create a response handler from the service, and then a serviceHandler in which you can start working with the service.
