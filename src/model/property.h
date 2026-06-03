// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <stdint.h>

namespace bcache2 {
namespace model {

struct Property {
    uint16_t page_id = 0;
    uint8_t cluster_id = 0;
    bool deleted = 0;
    uint64_t timestamp = 0;
};

}  // namespace model
}  // namespace bcache2
