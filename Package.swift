// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import class Foundation.ProcessInfo
import PackageDescription

let externalDependencies: [String: Range<Version>] = [
    "https://github.com/ordo-one/flatbuffers": .upToNextMajor(from: "22.0.0"),
    "https://github.com/apple/swift-argument-parser": .upToNextMajor(from: "1.1.0"),
    "https://github.com/apple/swift-nio": .upToNextMajor(from: "2.42.0"),
    "https://github.com/apple/swift-log": .upToNextMajor(from: "1.4.4"),
    "https://github.com/apple/swift-service-discovery.git" : .upToNextMajor(from: "1.0.0"),
]

let internalDependencies: [String: Range<Version>] = [
    "package-consul": .upToNextMajor(from: "9.0.0"),
    "package-lz4": .upToNextMinor(from: "1.10.0"),
]

#if swift(>=6.0)
@MainActor
#endif
func makeDependencies() -> [Package.Dependency] {
    var dependencies: [Package.Dependency] = []
    dependencies.reserveCapacity(externalDependencies.count + internalDependencies.count)

    for extDep in externalDependencies {
        dependencies.append(.package(url: extDep.key, extDep.value))
    }

    let localPath = ProcessInfo.processInfo.environment["LOCAL_PACKAGES_DIR"]
    for intDep in internalDependencies {
        if let localPath {
            dependencies.append(.package(name: "\(intDep.key)", path: "\(localPath)/\(intDep.key)"))
        } else {
            dependencies.append(.package(url: "https://github.com/ordo-one/\(intDep.key)", intDep.value))
        }
    }

    return dependencies
}

let package = Package(
    name: "package-distributed-system",
    platforms: [
        .macOS(.v15),
        .iOS(.v17),
    ],
    products: [
        .library(
            name: "DistributedSystem",
            targets: ["DistributedSystem"]
        ),
        .executable(
            name: "TestClient",
            targets: ["TestClient"]
        ),
        .executable(
            name: "TestService",
            targets: ["TestService"]
        ),
        .library(
            name: "TestMessages",
            targets: ["TestMessages"]
        ),
    ],
    dependencies: makeDependencies(),
    targets: [
        .target(
            name: "DistributedSystem",
            dependencies: [
                .product(name: "ConsulServiceDiscovery", package: "package-consul"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "lz4", package: "package-lz4"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "ServiceDiscovery", package: "swift-service-discovery"),
            ],
            swiftSettings:[
                .enableExperimentalFeature("AccessLevelOnImport")
            ]
        ),
        .target(
            name: "TestMessages",
            dependencies: [
                "DistributedSystem",
                .product(name: "FlatBuffers", package: "flatbuffers"),
            ],
            path: "Sources/ForTesting/TestMessages/",
            exclude: ["TestMessages.fbs"]
        ),
        .executableTarget(
            name: "TestService",
            dependencies: [
                "DistributedSystem",
                "TestMessages",
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
            ],
            path: "Sources/ForTesting/TestService",
            swiftSettings: [
                .unsafeFlags(["-Xfrontend", "-enable-experimental-distributed"]),
            ]
        ),
        .executableTarget(
            name: "TestClient",
            dependencies: [
                "DistributedSystem",
                "TestMessages",
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
            ],
            path: "Sources/ForTesting/TestClient",
            swiftSettings: [
                .unsafeFlags(["-Xfrontend", "-enable-experimental-distributed"]),
            ]
        ),
        .testTarget(
            name: "DistributedSystemTests",
            dependencies: [
                "DistributedSystem",
                "TestMessages",
            ],
            resources: [
                .process("Resources")
            ]
        ),
    ]
)
