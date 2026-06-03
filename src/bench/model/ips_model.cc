// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "bench/model/ips_model.h"

#include <utility>

#include "bench/model/nil_model.h"
#include "extension/modules.pb.h"

namespace bcache2 {
namespace bench {

Status IpsModel::Apply(const ApplyOptions& opts, const Operation& op,
                        std::vector<std::unique_ptr<Model>>* next_states) const {
    return Status::OK();
}

}  // namespace bench
}  // namespace bcache2
