// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>
#include <absl/strings/string_view.h>
#include <byte/include/macros.h>

#include <string>
#include <utility>

#include "model/orset_model.h"

namespace bcache2 {
namespace model {

template <typename Key, typename Value>
class HashOrSet {
 public:
    explicit HashOrSet(PersistentMap<Key, Value>* data) : data_(data) {}

    Status OnLoaded() { return Status::OK(); }

    Status OnChange(Key key, Value value) { return Status::OK(); }

    Status Set(partition::CmdContext* ctx, Key key, Value value) {
        data_->Put(ctx, std::move(key), std::move(value));
        return Status::OK();
    }

    Status Del(partition::CmdContext* ctx, const Key& key) {
        data_->Del(ctx, key);
        return Status::OK();
    }

    Status Get(const Key& key, Value* value) {
        auto it = data_->Find(key);
        if (it == data_->End()) {
            return Status::NotFound("field not found");
        }
        *value = it.Second();
        return Status::OK();
    }

    Status GetProperty(const Key& key, Property* pt) {
        auto it = data_->Find(key);
        if (it == data_->End()) {
            return Status::NotFound("field not found");
        }
        *pt = it.GetProperty();
        return Status::OK();
    }

    void Scan(const typename PersistentMap<Key, Value>::IterateFunc& iter_func) {
        data_->Scan(iter_func);
    }

    void ScanRange(const Key& start, const Key& end,
                   const typename PersistentMap<Key, Value>::IterateFunc& iter_func) {
        auto iter = data_->LowerBound(start);
        auto end_iter = data_->LowerBound(end);
        for (; iter != end_iter && iter != data_->End(); ++iter) {
            if (!iter_func(iter.First(), iter.Second())) {
                break;
            }
        }
    }

    size_t Size() const { return data_->Size(); }

 private:
    PersistentMap<Key, Value>* data_ = nullptr;
};

using HashModel = OrSetModel<std::string, std::string, HashOrSet<std::string, std::string>>;

}  // namespace model
}  // namespace bcache2
