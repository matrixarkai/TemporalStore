// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/strings/string_view.h>
#include <stdint.h>

namespace bcache2 {

class TimeTracer;

namespace partition {
class OpLogger;
class Object;
class ObjectManager;
struct CmdContext;
}  // namespace partition

namespace model {

struct ModelContext {
    uint64_t partition_id = 0;
    uint64_t slot_id = 0;
    partition::Object* object = nullptr;
    partition::OpLogger* op_logger;
    TimeTracer* time_tracer = nullptr;
    partition::CmdContext* cmd_context = nullptr;
};

void SetModelContext(ModelContext* model_context);
ModelContext* GetModelContext();

}  // namespace model
}  // namespace bcache2
