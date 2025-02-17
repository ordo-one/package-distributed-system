// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "package-distributed-system-benchmarks",
    platforms: [
        .macOS(.v15),
    ],
    dependencies: [
        .package(path: "../"),
        .package(url: "https://github.com/ordo-one/package-benchmark", from: "1.29.0"),
    ],
    targets: [
        .executableTarget(
            name: "DistributedSystemBenchmark",
            dependencies: [
                .product(name: "DistributedSystem", package: "package-distributed-system"),
                .product(name: "TestMessages", package: "package-distributed-system"),
                .product(name: "Benchmark", package: "package-benchmark"),
            ],
            path: "Benchmarks/DistributedSystem",
            swiftSettings: [
                .enableExperimentalFeature("AccessLevelOnImport"),
                .unsafeFlags(["-Xfrontend", "-enable-experimental-distributed"])
            ],
            plugins: [
                .plugin(name: "BenchmarkPlugin", package: "package-benchmark")
            ]
        ),
    ],
    swiftLanguageModes: [.v5]
)
