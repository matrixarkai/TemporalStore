// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "bench/model/model.h"
#include "common/macros.h"
#include "common/status.h"
#include "extension/string/interface.pb.h"
#include "protocol/bench.pb.h"

namespace bcache2 {
namespace bench {

class StringModel : public Model {
 public:
    Status Apply(const ApplyOptions& opts, const Operation& op,
                 std::vector<std::unique_ptr<Model>>* next_states) const override;
    std::string ToString() const override;

 private:
    Status ApplyInternal(const ApplyOptions& opts, const str2::SetRequest& request,
                         const str2::SetResponse& response, const Operation& op,
                         std::vector<std::unique_ptr<Model>>* next_states) const;

    Status ApplyInternal(const ApplyOptions& opts, const str2::SetexRequest& request,
                         const str2::SetexResponse& response, const Operation& op,
                         std::vector<std::unique_ptr<Model>>* next_states) const;

    Status ApplyInternal(const ApplyOptions& opts, const str2::GetRequest& request,
                         const str2::GetResponse& response, const Operation& op,
                         std::vector<std::unique_ptr<Model>>* next_states) const;
    void DoApplyValue(const ApplyOptions& opts, uint64_t start_ts_us, uint64_t end_ts_us,
                      const std::string& value, uint64_t ttl_ms,
                      std::vector<std::unique_ptr<Model>>* next_states) const;

    ModelProperty property_;
    std::string value_;
};

inline std::string StringModel::ToString() const {
    std::stringstream ss;
    ss << "StringModel{";
    ss << "Property=" << property_.ToString() << ", Value=" << value_;
    ss << "}";
    return ss.str();
}

}  // namespace bench
}  // namespace bcache2
