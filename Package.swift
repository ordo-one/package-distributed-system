// swift-tools-version: 5.9
// The swift-tools-version declares the minimum version of Swift required to build this package.

import class Foundation.ProcessInfo
import PackageDescription

let externalDependencies: [String: Range<Version>] = [
    "https://github.com/ordo-one/flatbuffers": .upToNextMajor(from: "22.0.0"),
    "https://github.com/apple/swift-argument-parser": .upToNextMajor(from: "1.1.0"),
    "https://github.com/apple/swift-nio": .upToNextMajor(from: "2.42.0"),
    "https://github.com/apple/swift-log": .upToNextMajor(from: "1.4.4"),
    "https://github.com/ordo-one/swift-service-lifecycle_1.0": .upToNextMajor(from: "1.0.0-alpha.13"), // to remove in future
    "https://github.com/apple/swift-service-discovery.git" : .upToNextMajor(from: "1.0.0"),
    "https://github.com/ordo-one/package-system-libs": .upToNextMajor(from: "0.0.3"),
]

let internalDependencies: [String: Range<Version>] = [
    "package-benchmark": .upToNextMajor(from: "1.0.0"),
    "package-concurrency-helpers": .upToNextMajor(from: "4.0.0"),
    "package-consul": .upToNextMajor(from: "6.0.0"),
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

func enableUpcomingFeatures() -> [SwiftSetting] {
    [
        .enableUpcomingFeature("BareSlashRegexLiterals"),
        .enableUpcomingFeature("ConciseMagicFile"),
        .enableUpcomingFeature("DeprecateApplicationMain"),
        .enableUpcomingFeature("DisableOutwardActorInference"),
        .enableUpcomingFeature("ForwardTrailingClosures"),
        .enableUpcomingFeature("StrictConcurrency"),
    ]
}

let package = Package(
    name: "package-distributed-system",
    platforms: [
        .macOS(.v14),
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
    ],
    dependencies: makeDependencies(),
    targets: [
        .target(
            name: "DistributedSystem",
            dependencies: [
                .product(name: "ConsulServiceDiscovery", package: "package-consul"),
                .product(name: "Helpers", package: "package-concurrency-helpers"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "lz4", package: "package-system-libs"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "PackageConcurrencyHelpers", package: "package-concurrency-helpers"),
                .product(name: "ServiceDiscovery", package: "swift-service-discovery"),
            ],
            swiftSettings:[
                .enableExperimentalFeature("AccessLevelOnImport")
            ] + enableUpcomingFeatures()
        ),
        .target(
            name: "TestMessages",
            dependencies: [
                "DistributedSystem",
                .product(name: "FlatBuffers", package: "flatbuffers"),
                .product(name: "Helpers", package: "package-concurrency-helpers"),
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
                .product(name: "Lifecycle", package: "swift-service-lifecycle_1.0"),
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
                .product(name: "Lifecycle", package: "swift-service-lifecycle_1.0"),
            ],
            path: "Sources/ForTesting/TestClient",
            swiftSettings: [
                .unsafeFlags(["-Xfrontend", "-enable-experimental-distributed"]),
            ]
        ),
        .executableTarget(
            name: "DistributedSystemBenchmark",
            dependencies: [
                "DistributedSystem",
                "TestMessages",
                .product(name: "PackageConcurrencyHelpers", package: "package-concurrency-helpers"),
                .product(name: "Benchmark", package: "package-benchmark"),
                .product(name: "BenchmarkPlugin", package: "package-benchmark"),
            ],
            path: "Benchmarks/DistributedSystem",
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
