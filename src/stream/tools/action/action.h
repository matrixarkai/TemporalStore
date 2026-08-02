// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include "common/status.h"

namespace bcache2 {
namespace stream {
namespace tool {

class Action {
 public:
    Action() {}
    virtual ~Action() {}

    virtual Status Run() { return Status::NotFound("Action not found"); }
};

}  // namespace tool
}  // namespace stream
}  // namespace bcache2
