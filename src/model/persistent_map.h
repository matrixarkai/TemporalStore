// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <algorithm>
#include <functional>
#include <string>
#include <utility>

#include "absl/container/btree_map.h"
#include "absl/container/internal/btree_container.h"
#include "common/allocator.h"
#include "model/common.h"
#include "model/model_context.h"
#include "model/property.h"
#include "partition/cmd_context.h"

namespace bcache2 {
namespace model {

template <>
inline size_t DataSize<std::pair<Property, std::string>>(
    const std::pair<Property, std::string>& data) {
    return data.second.length();
}

template <typename Key, typename Value,
          typename Alloc = Allocator::StlWrapper<std::pair<const Key, Value>>>
using BtreeMap = absl::btree_map<Key, Value, std::less<Key>, Alloc>;

// absl::btree_map + oplog
template <typename Key, typename Value>
class PersistentMap {
 public:
    using IterateFunc = std::function<bool(const Key& key, const Value& value)>;
    class Iterator {
     public:
        using iterator = typename BtreeMap<Key, std::pair<Property, Value>>::iterator;

        explicit Iterator(iterator it) : it_(it) {}

        const Key& First() { return it_->first; }
        const Value& Second() { return it_->second.second; }
        const Property& GetProperty() { return it_->second.first; }

        bool operator==(const Iterator& x) const { return it_ == x.it_; }
        bool operator!=(const Iterator& x) const { return it_ != x.it_; }
        Iterator& operator++() {
            ++it_;
            return *this;
        }
        Iterator& operator--() {
            --it_;
            return *this;
        }

     private:
        iterator it_;
    };

    PersistentMap(BtreeMap<Key, std::pair<Property, Value>>* data, uint64_t* max_timestamp)
        : data_(data), max_timestamp_(max_timestamp) {}

    void Put(partition::CmdContext* ctx, Key key, Value value) {
        Property property;
        property.page_id = 0;
        property.cluster_id = 0;
        property.deleted = 0;
        property.timestamp = std::max(GetCurrentTimeInNs(), *max_timestamp_ + 1);
        *max_timestamp_ = property.timestamp;
        // For compatibility
        if (ctx == nullptr) {
            ModelContext* ctx = GetModelContext();
            ctx->op_logger->WriteKvLog(ctx->cmd_context, ctx->slot_id, ctx->object->ObjectId(),
                                       ctx->object->Key(), ctx->object->ModelId(),
                                       SerializeToString(key), SerializeToString(value), property);
        } else {
            ctx->op_logger->WriteKvLog(ctx, ctx->slot_id, ctx->object.ObjectId(), ctx->object.Key(),
                                       ctx->object.ModelId(), SerializeToString(key),
                                       SerializeToString(value), property);
        }
        (*data_)[std::move(key)] = std::make_pair(property, std::move(value));
    }

    void Del(partition::CmdContext* ctx, const Key& key) {
        auto it = data_->find(key);
        if (it != data_->end()) {
            it->second.first.deleted = 1;
            it->second.second = Value();  // an empty value
            // For compatibility
            if (ctx == nullptr) {
                ModelContext* ctx = GetModelContext();
                ctx->op_logger->WriteKvLog(ctx->cmd_context, ctx->slot_id, ctx->object->ObjectId(),
                                           ctx->object->Key(), ctx->object->ModelId(),
                                           SerializeToString(key),
                                           SerializeToString(it->second.second), it->second.first);
            } else {
                ctx->op_logger->WriteKvLog(ctx, ctx->slot_id, ctx->object.ObjectId(),
                                           ctx->object.Key(), ctx->object.ModelId(),
                                           SerializeToString(key),
                                           SerializeToString(it->second.second), it->second.first);
            }
            data_->erase(key);
        }
    }

    void DelBegin(partition::CmdContext* ctx) {
        auto it = data_->begin();
        if (it != data_->end()) {
            auto key = it->first;
            it->second.first.deleted = 1;
            it->second.second = Value();
            if (ctx == nullptr) {
                ModelContext* ctx = GetModelContext();
                ctx->op_logger->WriteKvLog(ctx->cmd_context, ctx->slot_id, ctx->object->ObjectId(),
                                           ctx->object->Key(), ctx->object->ModelId(),
                                           SerializeToString(key),
                                           SerializeToString(it->second.second), it->second.first);
            } else {
                ctx->op_logger->WriteKvLog(ctx, ctx->slot_id, ctx->object.ObjectId(),
                                           ctx->object.Key(), ctx->object.ModelId(),
                                           SerializeToString(key),
                                           SerializeToString(it->second.second), it->second.first);
            }
            data_->erase(key);
        }
    }

    Iterator Find(const Key& key) { return Iterator(data_->find(key)); }
    Iterator LowerBound(const Key& key) { return Iterator(data_->lower_bound(key)); }
    Iterator UpperBound(const Key& key) { return Iterator(data_->upper_bound(key)); }
    Iterator Begin() { return Iterator(data_->begin()); }
    Iterator End() { return Iterator(data_->end()); }

    const Status Scan(const IterateFunc& iter_func) {
        Iterator iter = Begin();
        while (iter != End()) {
            if (!iter_func(iter.First(), iter.Second())) {
                break;
            }
            ++iter;
        }
        return Status::OK();
    }

    const Status Scan(const Key& start_key, const IterateFunc& iter_func) {
        Iterator iter = LowerBound(start_key);
        while (iter != End()) {
            if (!iter_func(iter.First(), iter.Second())) {
                break;
            }
            ++iter;
        }
        return Status::OK();
    }

    // TODO(wy): is lowerbound accurate?
    const Status ScanBackward(const Key& start_key, const IterateFunc& iter_func) {
        if (Size() == 0) {
            return Status::OK();
        }

        Iterator iter = LowerBound(start_key);
        if (iter == End()) --iter;

        while (true) {
            if (!iter_func(iter.First(), iter.Second())) {
                break;
            }
            if (iter == Begin()) break;
            --iter;
        }
        return Status::OK();
    }

    uint64_t Size() const { return data_->size(); }

 private:
    BtreeMap<Key, std::pair<Property, Value>>* data_ = nullptr;
    uint64_t* max_timestamp_ = nullptr;
};

}  // namespace model
}  // namespace bcache2
