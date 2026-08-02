// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once
#include <string>

#include "blockcache/rdmareadcache/replacement_policy/replacement_policy.h"
namespace bcache2 {
class ReplacementPolicyFIFO : public ReplacementPolicy {
 public:
    ReplacementPolicyFIFO();
    virtual ~ReplacementPolicyFIFO();
};
}  // namespace bcache2
