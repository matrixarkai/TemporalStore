// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "bench/model/model.h"
#include "common/macros.h"
#include "common/status.h"
#include "extension/hash/interface.pb.h"
#include "protocol/bench.pb.h"

namespace bcache2 {
namespace bench {

class HashModel : public Model {
 public:
    Status Apply(const ApplyOptions& opts, const Operation& op,
                 std::vector<std::unique_ptr<Model>>* next_states) const override;
    std::string ToString() const override;

 private:
    Status ApplyInternal(const ApplyOptions& opts, const hash2::SetRequest& request,
                         const hash2::SetResponse& response, const Operation& op,
                         std::vector<std::unique_ptr<Model>>* next_states) const;

    Status ApplyInternal(const ApplyOptions& opts, const hash2::GetRequest& request,
                         const hash2::GetResponse& response, const Operation& op,
                         std::vector<std::unique_ptr<Model>>* next_states) const;

    Status ApplyInternal(const ApplyOptions& opts, const hash2::DelRequest& request,
                         const hash2::DelResponse& response, const Operation& op,
                         std::vector<std::unique_ptr<Model>>* next_states) const;

    Status ApplyInternal(const ApplyOptions& opts, const hash2::GetAllRequest& request,
                         const hash2::GetAllResponse& response, const Operation& op,
                         std::vector<std::unique_ptr<Model>>* next_states) const;

    void DoApplyValue(const ApplyOptions& opts, uint64_t start_ts_us, uint64_t end_ts_us,
                      const std::string& field, const std::string& value, bool del,
                      std::vector<std::unique_ptr<Model>>* next_states) const;

    ModelProperty property_;
    std::unordered_map<std::string, std::string> value_;
};

inline std::string HashModel::ToString() const {
    std::stringstream ss;
    ss << "HashModel{";
    ss << "Property=" << property_.ToString() << ", Value={";
    bool first = true;
    for (auto iter : value_) {
        if (!first) {
            ss << ", ";
        }
        ss << iter.first << ":" << iter.second;
        first = false;
    }
    ss << "}}";
    return ss.str();
}

}  // namespace bench
}  // namespace bcache2
