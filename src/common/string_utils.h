// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/string/string_escape.h>

#include <string>

namespace bcache2 {

inline std::string DebugRawString(const std::string& src) {
    return byte::Escape(byte::StringPiece(src));
}

inline std::string DebugRawString(const void* data, size_t size) {
    return byte::Escape(byte::StringPiece(reinterpret_cast<const char*>(data), size));
}

inline std::string HexString(const std::string& src) {
    return byte::AlwaysEscape(byte::StringPiece(src));
}

inline std::string HexString(const void* data, size_t size) {
    return byte::AlwaysEscape(byte::StringPiece(reinterpret_cast<const char*>(data), size));
}

}  // namespace bcache2
