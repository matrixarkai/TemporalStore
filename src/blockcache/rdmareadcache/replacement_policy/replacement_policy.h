// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once
#include <iostream>

namespace bcache2 {
class ReplacementPolicy {
 public:
    ReplacementPolicy() = default;
    virtual ~ReplacementPolicy() {}
};
}  // namespace bcache2
