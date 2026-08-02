// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <string>

#include "common/allocator.h"
#include "partition/metrics.h"

namespace bcache2 {
namespace partition {

// TODO(wangtai.10): more type
enum AllocatorType {
    // slot node
    kSlotNode,

    // dirty slots, ...
    kIndexOverhead,

    // basically user data
    kModel,

    // slot context
    kSlotContext,

    // placeholder
    kAllocatorTypeMax,
};

inline std::string AllocatorTypeName(int type) {
    switch (type) {
    case kSlotNode:
        return "SlotNode";
    case kIndexOverhead:
        return "IndexOverhead";
    case kModel:
        return "Model";
    case kSlotContext:
        return "SlotContext";
    default:
        BYTE_ASSERT(false) << "invalid type " << type;
    }
    return "";
}

class AllocatorManager {
 public:
    explicit AllocatorManager(MetricsManager* metrics_manager) {
        for (int i = 0; i < AllocatorType::kAllocatorTypeMax; ++i) {
            metrics_[i].Init(metrics_manager, AllocatorTypeName(i));
        }
    }
    ~AllocatorManager() = default;

    Allocator* GetAllocator(AllocatorType type) { return &allocators_[type]; }

    size_t GetTotalAllocedSize() const {
        size_t ret = 0;
        for (auto& allocator : allocators_) {
            ret += allocator.GetStats().alloced_size();
        }
        return ret;
    }

    void ReapMetrics() {
        for (int i = 0; i < AllocatorType::kAllocatorTypeMax; ++i) {
            const AllocatorStats& stats = allocators_[i].GetStats();
            metrics_[i].alloced_size->get()->Set(stats.alloced_size());
            metrics_[i].alloc_cnt->get()->Set(stats.alloc_cnt());
            metrics_[i].dealloc_cnt->get()->Set(stats.dealloc_cnt());
        }
    }

 private:
    // one allocator and metrics for each type
    Allocator allocators_[AllocatorType::kAllocatorTypeMax];
    AllocatorMetrics metrics_[AllocatorType::kAllocatorTypeMax];

    DISALLOW_COPY_AND_ASSIGN(AllocatorManager);
};

}  // namespace partition
}  // namespace bcache2
