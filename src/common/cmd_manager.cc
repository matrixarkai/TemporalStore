// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "common/cmd_manager.h"

namespace bcache2 {

CmdManager& CmdManager::Instance() {
    static CmdManager module_manager_;
    return module_manager_;
}

}  // namespace bcache2
