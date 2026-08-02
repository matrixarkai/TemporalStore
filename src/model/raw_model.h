// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <string>

#include "model/orset_model.h"

namespace bcache2 {
namespace model {

template <typename Key, typename Value>
class RawOrSet {
 public:
    explicit RawOrSet(PersistentMap<Key, Value>* data) : data_(data) {}

    Status OnLoaded() { return Status::OK(); }

    Status OnChange(Key key, Value value) { return Status::OK(); }

    PersistentMap<Key, Value>* operator->() { return data_; }

 private:
    PersistentMap<Key, Value>* data_ = nullptr;
};

using RawModel = OrSetModel<std::string, std::string, RawOrSet<std::string, std::string>>;

}  // namespace model
}  // namespace bcache2
