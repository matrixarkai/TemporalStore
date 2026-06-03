// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "bench/model/model.h"
#include "common/macros.h"
#include "common/status.h"
#include "extension/ips/interface.pb.h"
#include "protocol/bench.pb.h"

namespace bcache2 {
namespace bench {

class IpsModel : public Model {
 public:
    Status Apply(const ApplyOptions& opts, const Operation& op,
                 std::vector<std::unique_ptr<Model>>* next_states) const override;
    std::string ToString() const override;

 private:
    ModelProperty property_;
    std::unordered_map<std::string, std::string> value_;
};

inline std::string IpsModel::ToString() const {
    return "";
}

}  // namespace bench
}  // namespace bcache2
