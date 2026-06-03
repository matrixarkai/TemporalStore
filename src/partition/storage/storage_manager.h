// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/macros.h>
#include <gflags/gflags.h>

#include <memory>
#include <vector>

#include "common/metrics.h"
#include "common/ring_array.h"
#include "partition/allocator_manager.h"
#include "partition/index/index.h"
#include "partition/storage/page_gc.h"
#include "partition/storage/page_store.h"
#include "partition/storage/zone.h"

DECLARE_int32(stream_zone_size);

namespace bcache2 {

class CoSyncClosure;

namespace partition {

class Index;
class PageStore;
class OpLogger;
class ObjectManager;
class SlotStore;
class Evicter;
class PageGc;
class GcStat;
class Expirer;
class SlotContextManager;
class AllocatorManager;
class PageCompactor;
class Partition;
class CmdExecutor;

class StorageManager {
 public:
    struct Options {
        bool enable_prepare = true;
        bool enable_oplog_rolling = true;
        bool enable_evict = true;
        bool enable_expire = true;
        bool enable_page_gc = true;
        bool enable_page_compaction = true;
        bool enable_index_gc = true;
    };

    StorageManager(Partition* partition, CmdExecutor* cmd_executor, Index* index,
                   PageStore* page_store, OpLogger* op_logger, SlotStore* slot_store,
                   Evicter* evicter, Expirer* expirer, ObjectManager* object_manager,
                   PageGc* page_gc, PageCompactor* page_compactor,
                   SlotContextManager* slot_context_manager, MetricsManager* metrics_manager,
                   AllocatorManager* allocator_manager);
    virtual ~StorageManager();

    void Init(Options options);
    void Start();
    void Stop();

 private:
    void LoopWorker();
    Status Prepare();
    void ReclaimOpLog();
    bool ShouldDelayDumpOplog();
    void ReclaimOpLogWithLimit(int limit);
    void ReclaimMemory();
    void ReclaimPage();
    void CompactPages();
    void ReclaimIndex();
    void ReapMetrics();

    Status PrepareNewZone(bool force_new_zone) {
        return page_store_->PrepareNewZone(force_new_zone);
    }

    void PrepareUpdateIndexMeta(bool manual);
    void RefreshMetrics();

    Options opts_;

    CmdExecutor* cmd_executor_ = nullptr;
    Index* index_ = nullptr;
    PageStore* page_store_ = nullptr;
    OpLogger* op_logger_ = nullptr;
    SlotStore* slot_store_ = nullptr;
    Evicter* evicter_ = nullptr;
    Expirer* expirer_ = nullptr;
    ObjectManager* object_manager_ = nullptr;
    AllocatorManager* allocator_manager_ = nullptr;
    Partition* partition_ = nullptr;
    SlotContextManager* slot_context_manager_ = nullptr;
    PageGc* page_gc_ = nullptr;
    PageCompactor* page_compactor_ = nullptr;

    std::unique_ptr<CoSyncClosure> stop_sync_;
    std::unique_ptr<Index::SlotIterator> slot_iterator_;
    bool stopped_ = true;
    StorageManagerMetrics metrics_;

    uint64_t last_update_meta_ms_ = 0;

    DISALLOW_COPY_AND_ASSIGN(StorageManager);
};

}  // namespace partition
}  // namespace bcache2
