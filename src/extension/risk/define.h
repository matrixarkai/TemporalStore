// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include "common/logging.h"
namespace bcache2 {
namespace risk_tool {

#define RISK_LOG(LEVEL, request) LOG(LEVEL) << "[risk]key = " << request.key() << " "

}  // namespace risk_tool
}  // namespace bcache2
