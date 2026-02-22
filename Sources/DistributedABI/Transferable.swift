// Copyright 2026 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

/// Combine Serializable & Deserializable protocols
/// TODO: possibly no need for standalone protocol node, could be a typealias
public protocol Transferable: Serializable & Deserializable & Sendable {}
