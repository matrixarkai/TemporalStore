// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <map>
#include <memory>
#include <set>
#include <vector>

#include "common/metrics.h"
#include "partition/index/index.h"
#include "partition/storage/page_store.h"
#include "partition/storage/slot_store.h"
#include "partition/storage/zone.h"

namespace bcache2 {
namespace partition {

class Partition;

// TODO(wangtai.10): merge PageGc and PageStore
class PageGc {
 public:
    PageGc(Partition* partition, stream::Env* env, Index* index, PageStore* page_store,
           SlotStore* slot_store, OpLogger* op_logger, MetricsManager* metrics_manager);

    virtual ~PageGc();

    Status Gc();

 private:
    struct GcZoneContext {
        uint32_t zone_id = kInvalidZoneId;
        bool page_log = false;
        std::unique_ptr<Index::SlotIterator> iterator;
        SlotMap::key_type cursor = SlotMap::key_type();
        bool completed = false;
        TimeCost gc_cost;
        uint64_t used_bytes = 0;
        uint64_t total_bytes = 0;
        double utility = 0.0;
    };

    Status PurgeCompactedZones();
    bool PickNextZone();
    bool GcCurrentZone();  // return finished

    Partition* partition_ = nullptr;
    stream::Env* env_ = nullptr;
    Index* index_ = nullptr;
    PageStore* page_store_ = nullptr;
    SlotStore* slot_store_ = nullptr;
    OpLogger* op_logger_ = nullptr;
    MetricsManager* metrics_manager_ = nullptr;
    GcZoneContext current_gc_zone_;

    uint64_t recycled_oplog_length_ = UINT64_MAX;
    PageGcMetrics metrics_;
    std::vector<std::unique_ptr<ZoneGcMetrics>> zone_metrics_;

    DISALLOW_COPY_AND_ASSIGN(PageGc);
};

}  // namespace partition
}  // namespace bcache2
