// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "common/logging.h"
#include "model/orset_model.h"
#include "protocol/feature_module.pb.h"

namespace bcache2 {
namespace model {

class FeatureOrSet {
 public:
    explicit FeatureOrSet(PersistentMap<uint64_t, std::string>* data) : data_(data) {}

    Status OnLoaded() { return Status::OK(); }

    Status OnChange(uint64_t key, std::string) { return Status::OK(); }

    Status Add(partition::CmdContext* ctx, uint64_t timestamp, std::string long_seq_point) {
        data_->Put(ctx, timestamp, long_seq_point);
        return Status::OK();
    }

    bool Get(uint64_t timestamp) {
        auto it = data_->Find(timestamp);
        if (it != data_->End()) {
            return true;
        }
        return false;
    }

    Status Query(uint64_t start, uint64_t end, uint64_t max_num,
                 std::function<void(const uint64_t, const std::string)> filter) {
        uint64_t cur_num = 0;
        auto it = data_->LowerBound(start);
        while (it != data_->End() && it.First() < end && cur_num++ < max_num) {
            filter(it.First(), std::move(it.Second()));
            ++it;
        }
        return Status::OK();
    }

    void DelBegins(partition::CmdContext* ctx, uint64_t cnt) {
        while (Size() > cnt) {
            data_->DelBegin(ctx);
        }
    }

    Status GetProperty(const uint64_t& key, Property* pt) {
        auto it = data_->Find(key);
        if (it == data_->End()) {
            return Status::NotFound("");
        }
        *pt = it.GetProperty();
        return Status::OK();
    }

    uint64_t Size() const { return data_->Size(); }

 private:
    PersistentMap<uint64_t, std::string>* data_;
};

using FeatureModel = OrSetModel<uint64_t, std::string, FeatureOrSet>;

}  // namespace model
}  // namespace bcache2
