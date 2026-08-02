// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/container/flat_hash_map.h>
#include <byte/base/closure.h>
#include <byte/include/macros.h>

#include "common/controller.h"
#include "protocol/server.pb.h"

#define CMD_EXECUTOR_SET_PREPARE_CTX_DEFAULT(cmd_type, cmd_name) \
    CMD_EXECUTOR_PREPARE_CTX_DEFAULT(Set, set, cmd_type, cmd_name)
#define CMD_EXECUTOR_SET_EXECUTE(cmd_type, cmd_name) \
    CMD_EXECUTOR_EXECUTE(Set, set, cmd_type, cmd_name)

namespace bcache2 {
namespace partition {

class ObjectManager;

}  // namespace partition
}  // namespace bcache2
