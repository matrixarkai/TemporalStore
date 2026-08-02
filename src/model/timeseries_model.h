// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "model/orset_model.h"

namespace bcache2 {
namespace model {

class TimeSeriesOrSet {
 public:
    explicit TimeSeriesOrSet(PersistentMap<uint64_t, std::string>* data) : data_(data) {}

    Status OnLoaded() { return Status::OK(); }

    Status OnChange(uint64_t key, std::string value) { return Status::OK(); }
    Status Execute(partition::CmdContext* ctx, std::string cmd, std::string* result) {
        absl::string_view view(cmd);
        std::vector<std::string> v = absl::StrSplit(view, ' ');
        if (v.size() != 3) {
            return Status::InvalidArgument("Error format");
        }
        if (v[0] == "ADD") {
            uint64_t timestamp = 0;
            if (!absl::SimpleAtoi(v[1], &timestamp)) {
                return Status::InvalidArgument("");
            }
            return Add(ctx, timestamp, v[2]);
        } else if (v[0] == "QUERY") {
            uint64_t start = 0;
            uint64_t end = 0;
            if (!absl::SimpleAtoi(v[1], &start) || !absl::SimpleAtoi(v[2], &end)) {
                return Status::InvalidArgument("");
            }
            std::vector<std::pair<uint64_t, std::string>> result_vector;
            Status status = Query(start, end, &result_vector);
            for (const auto& item : result_vector) {
                result->append(std::to_string(item.first));
                result->append(":");
                result->append(item.second);
                result->append(",");
            }
            return status;
        } else {
            return Status::Unimplemented("");
        }
    }

 public:
    Status Add(partition::CmdContext* ctx, uint64_t timestamp, std::string point) {
        data_->Put(ctx, timestamp, point);
        return Status::OK();
    }
    Status Query(uint64_t start, uint64_t end,
                 std::vector<std::pair<uint64_t, std::string>>* result) {
        auto it = data_->LowerBound(start);
        result->clear();
        while (it != data_->End() && it.First() < end) {
            result->push_back(std::make_pair(it.First(), it.Second()));
            ++it;
        }
        return Status::OK();
    }

 private:
    PersistentMap<uint64_t, std::string>* data_;
};

using TimeSeriesModel = OrSetModel<uint64_t, std::string, TimeSeriesOrSet>;

}  // namespace model
}  // namespace bcache2
