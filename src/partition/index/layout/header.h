// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <cstdint>
#include <type_traits>

namespace bcache2 {
namespace partition {

// slot layout
enum LayoutType : uint8_t {
    kSinglePage = 0x1f,        // single page layout
    kSingleObject = 0x2f,      // single object layout (hotspot type)
    kSinglePageObject = 0x3f,  // single page and object layout (hotspot type)
    kMultiPageObject = 0x4f,   // multi pages or objects layout

    kMax = 0xff,
};
constexpr int kLayoutCount = LayoutType::kMax + 1;

// ┌─────────────┬─────────────────┐
// │  magic (1B) │  last_used (3B) │
// └─────────────┴─────────────────┘
struct Header {
    LayoutType magic : 8;     // TODO(zkwu): this is NOT a magic number, and it should be renamed to
                              // layout_type
    uint32_t last_used : 24;  // from redis: LRU time (relative to global lru_clock) or LFU
                              // data (least significant 8 bits frequency and most
                              // significant 16 bits access time).

    Header() : magic(LayoutType::kMax), last_used(0) {}
} __attribute__((__packed__));
static_assert(std::is_standard_layout<Header>::value, "for reinterpret cast");
static_assert(sizeof(Header) == 4);

}  // namespace partition
}  // namespace bcache2
