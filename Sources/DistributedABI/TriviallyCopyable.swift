// Copyright 2026 Ordo One AB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

/// If a type implements TriviallyCopyable protocol then collection types (like an Array or Dictionary)
/// just serialize a binary data representation of the collection instead of serializing each item.
public protocol TriviallyCopyable {}
