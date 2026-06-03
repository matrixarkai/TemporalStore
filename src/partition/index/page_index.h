// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <inttypes.h>
#include <stddef.h>

#include <type_traits>
#include <utility>

namespace bcache2 {
namespace partition {

// ┌────────────┬───────────┬──────────────┐
// │ flags (1B) │ size (3B) │ address (8B) │
// └─────┬──────┴───────────┴──────────────┘
//       │     ┌─────────────┬──────────────┬──────────────────┬───────────────┐
//       └────>│  dirty (1b) │ deleted (1b) │ page in log (1b) │ reserved (6b) │
//             └─────────────┴──────────────┴──────────────────┴───────────────┘
// TODO(wangtai.10 & qianwenpin): compact PageIndex
struct PageIndex {
    uint8_t object_id = 0;
    uint8_t model_id = 0;
    uint16_t page_id = 0;
    bool dirty : 1;
    bool page_in_log : 1;
    uint8_t reserved : 6;
    uint32_t page_size = 0;  // page_size == 0 indicates page is deleted
    uint64_t address = 0;

    PageIndex() : dirty(0), page_in_log(0), reserved(0) {}
} __attribute__((__packed__));
static_assert(std::is_standard_layout<PageIndex>::value);
static_assert(sizeof(PageIndex) == 17);

// * for page_store: page_address indicates an unique page
// * for oplogger(page in log): page_address indicates an unique log,
//                              page_address + slot_id + object_id + page_id
//                              indicates an unique page
struct SlotPageIndex {
    uint64_t slot_id = 0;
    PageIndex page_index;

    SlotPageIndex(uint64_t _slot_id, PageIndex _page_index)
        : slot_id(_slot_id), page_index(std::move(_page_index)) {}
};

}  // namespace partition
}  // namespace bcache2
