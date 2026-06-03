// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/base/closure.h>

#include "common/controller.h"
#include "protocol/bench.pb.h"

namespace bcache2 {
namespace bench {

class Client {
 public:
    virtual ~Client() {}
    virtual void Execute(Controller* ctrl, Operation* op, Closure<void>* callback) = 0;
};

}  // namespace bench
}  // namespace bcache2
