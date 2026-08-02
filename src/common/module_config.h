// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once
#include "common/status.h"
#include "protocol/config.pb.h"

namespace bcache2 {

class ModuleCustomConfig {
 public:
    // dummy struct for polymorphism only
    ModuleCustomConfig() {}
    virtual Status LoadCustomConfig(const CustomConfig&) = 0;
    virtual Status UpdateCustomConfig(const CustomConfig&) = 0;
    virtual ~ModuleCustomConfig() {}
};

}  // namespace bcache2
