// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include "common/logging.h"
namespace bcache2 {
namespace control_state_tool {

#define CONTROL_STATE_LOG(LEVEL, request) LOG(LEVEL) << "[control_state]key = " << request.key() << " "

}  // namespace control_state_tool
}  // namespace bcache2
