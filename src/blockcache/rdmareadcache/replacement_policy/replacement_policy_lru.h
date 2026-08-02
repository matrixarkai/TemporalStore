// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once
#include "blockcache/rdmareadcache/replacement_policy/replacement_policy.h"

namespace bcache2 {
class ReplacementPolicyLRU : public ReplacementPolicy {
 public:
    ReplacementPolicyLRU();
    virtual ~ReplacementPolicyLRU();
};
}  // namespace bcache2
