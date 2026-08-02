// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "model/model_context.h"

namespace bcache2 {
namespace model {

__thread ModelContext* thread_model_context = nullptr;

void SetModelContext(ModelContext* model_context) { thread_model_context = model_context; }

ModelContext* GetModelContext() { return thread_model_context; }

}  // namespace model
}  // namespace bcache2
