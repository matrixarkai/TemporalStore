// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <gflags/gflags.h>

#include <memory>

#include "common/metrics.h"
#include "common/status.h"
#include "partition/index/index.h"

namespace bcache2 {

class MetricsManager;

namespace partition {

class ObjectManager;
class OpLogger;

class Expirer {
 public:
    Expirer(Index* index, ObjectManager* object_manager, OpLogger* op_logger,
            MetricsManager* metrics_manager);
    ~Expirer() = default;

    // scan objects in memory and in storage, check their TTL, delete them if expired
    Status TryExpire();

 private:
    Index* index_ = nullptr;
    ObjectManager* object_manager_ = nullptr;
    OpLogger* op_logger_ = nullptr;
    std::unique_ptr<Index::SlotIterator> in_memory_iter_;
    std::unique_ptr<Index::SlotIterator> in_store_iter_;
    std::unique_ptr<MetricsEnv::HistogramHolder> expire_latency_;
    std::unique_ptr<MetricsEnv::CounterHolder> expire_counter_;

    DISALLOW_COPY_AND_ASSIGN(Expirer);
};

}  // namespace partition
}  // namespace bcache2
