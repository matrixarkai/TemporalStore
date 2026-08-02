// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "bench/model/model.h"

namespace bcache2 {
namespace bench {

class NilModel : public Model {
 public:
    Status Apply(const ApplyOptions& opts, const Operation& op,
                 std::vector<std::unique_ptr<Model>>* next_states) const override;
    std::string ToString() const override { return "NilModel{}"; }
};

}  // namespace bench
}  // namespace bcache2
