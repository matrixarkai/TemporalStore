// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/storage/storage_manager.h"

#include <byte/container/intrusive_list.h>
#include <byte/thread/async_thread.h>
#include <gflags/gflags.h>

#include <string>
#include <utility>
#include <vector>

#include "brpc/details/tcmalloc_extension.h"
#include "common/coclosure.h"
#include "common/logging.h"
#include "common/time_tracer.h"
#include "partition/allocator_manager.h"
#include "partition/index/index.h"
#include "partition/metrics.h"
#include "partition/partition.h"
#include "partition/partition_fiu.h"
#include "partition/storage/evicter.h"
#include "partition/storage/expirer.h"
#include "partition/storage/object_manager.h"
#include "partition/storage/page_compactor.h"
#include "partition/storage/page_store.h"
#include "partition/storage/slot_context_manager.h"
#include "partition/storage/slot_store.h"

DECLARE_uint64(reap_metrics_max_slots_per_round);
DECLARE_uint64(storage_loop_interval_us);
DECLARE_uint64(storage_dump_slots_per_round);
DECLARE_uint64(storage_oplog_delay_dump_length);
DECLARE_uint64(storage_dump_index_meta_oplog_gap);
DECLARE_uint64(trigger_update_index_meta_time_ms);
DECLARE_bool(storage_enable_oplog_rolling);
DECLARE_bool(storage_enable_evict);
DECLARE_bool(storage_enable_expire);
DECLARE_bool(storage_enable_page_gc);
DECLARE_bool(storage_enable_page_compaction);
DECLARE_bool(storage_enable_index_gc);
DECLARE_bool(start_storage_manager_when_loading);

namespace bcache2 {
namespace partition {

StorageManager::StorageManager(Partition* partition, CmdExecutor* cmd_executor, Index* index,
                               PageStore* page_store, OpLogger* op_logger, SlotStore* slot_store,
                               Evicter* evicter, Expirer* expirer, ObjectManager* object_manager,
                               PageGc* page_gc, PageCompactor* page_compactor,
                               SlotContextManager* slot_context_manager,
                               MetricsManager* metrics_manager, AllocatorManager* allocator_manager)
    : cmd_executor_(cmd_executor),
      index_(index),
      page_store_(page_store),
      op_logger_(op_logger),
      slot_store_(slot_store),
      evicter_(evicter),
      expirer_(expirer),
      object_manager_(object_manager),
      allocator_manager_(allocator_manager),
      partition_(partition),
      slot_context_manager_(slot_context_manager),
      page_gc_(page_gc),
      page_compactor_(page_compactor) {
    metrics_.Init(metrics_manager);
    slot_iterator_ = index->NewSlotIterator();
}

StorageManager::~StorageManager() {}

void StorageManager::Init(Options options) { opts_ = std::move(options); }

void StorageManager::Start() {
    LOG_CALL_INFO().put("PartitionId", partition_->GetPartitionID()).put("Stopped", stopped_);
    if (!stopped_) {
        return;
    }
    stopped_ = false;
    byte::InvokeInCurrentThread(NewCoClosure(this, &StorageManager::LoopWorker));
}

void StorageManager::Stop() {
    LOG_CALL_INFO().put("PartitionId", partition_->GetPartitionID()).put("Stopped", stopped_);
    if (stopped_) {
        return;
    }
    stopped_ = true;
    stop_sync_.reset(new CoSyncClosure());
    stop_sync_->Wait();
}

void StorageManager::LoopWorker() {
    auto run_once = [this] {
        TimeTracer tracer;

        Status status = Prepare();
        if (!status.ok()) {
            LOG_WARNING("Prepare Failed")
                .put("PartitionId", partition_->GetPartitionID())
                .put("Status", status);
            return;
        }
        tracer.AddEvent("Prepare");

        if (!stopped_) {
            ReclaimOpLog();
            tracer.AddEvent("ReclaimOpLog");
        }

        if (!stopped_) {
            ReclaimMemory();
            tracer.AddEvent("ReclaimMemory");
        }

        if (!stopped_) {
            ReclaimPage();
            tracer.AddEvent("ReclaimPage");
        }

        if (!stopped_) {
            ReclaimIndex();
            tracer.AddEvent("ReclaimIndex");
        }

        if (!stopped_) {
            CompactPages();
            tracer.AddEvent("CompactPages");
        }

        if (!stopped_) {
            ReapMetrics();
            tracer.AddEvent("ReapMetrics");
        }

        LOG_DEBUG("StorageManager Loop")
            .put("PartitionId", partition_->GetPartitionID())
            .put("Tracer", tracer.ToString());
        LOG_INFO_SAMPLE("StorageManager Loop")
            .put("PartitionId", partition_->GetPartitionID())
            .put("Tracer", tracer.ToString());
        metrics_.loop_qps->get()->Increment();
        metrics_.loop_latency->get()->Set(tracer.TotalSpentUs());
    };

    if (UNLIKELY(!IsCoContext())) {
        if (stop_sync_) {
            stop_sync_->Run();
            return;
        }
        if (!stopped_) {
            run_once();
        }
        if (stop_sync_) {
            stop_sync_->Run();
        } else if (!stopped_) {
            byte::InvokeLaterInCurrentThread(FLAGS_storage_loop_interval_us,
                                             NewCoClosure(this, &StorageManager::LoopWorker));
        }
        return;
    }

    while (stop_sync_.get() == nullptr && !stopped_) {
        CoSleep(FLAGS_storage_loop_interval_us);
        run_once();
    }

    if (stop_sync_) {
        stop_sync_->Run();
    }
}

Status StorageManager::Prepare() {
    LOG_CALL_DEBUG().put("PartitionId", partition_->GetPartitionID());

    if (!opts_.enable_prepare) {
        return Status::OK();
    }

    Status status = PrepareNewZone(/* force_new_zone = */ false);
    if (!status.ok()) {
        LOG_ERROR("prepare new zone failed")
            .put("PartitionId", partition_->GetPartitionID())
            .put("errorcode", status.errorcode());
        return status;
    }

    uint64_t now = GetCurrentTimeInMs();
    if (now - last_update_meta_ms_ > FLAGS_trigger_update_index_meta_time_ms) {
        index_->OnMetaUpdate();
        last_update_meta_ms_ = now;
    }
    return Status::OK();
}

void StorageManager::ReclaimOpLog() {
    LOG_CALL_DEBUG()
        .put("PartitionId", partition_->GetPartitionID())
        .put("enable_oplog_rolling", opts_.enable_oplog_rolling)
        .put("ShouldDelayDumpOplog", ShouldDelayDumpOplog());
    if (!opts_.enable_oplog_rolling || !FLAGS_storage_enable_oplog_rolling) {  // log rotation
        return;
    }
    if (!ShouldDelayDumpOplog()) {
        ScopedLatency latency(metrics_.reclaim_oplog_latency->get());
        ReclaimOpLogWithLimit(FLAGS_storage_dump_slots_per_round);
    }
}

bool StorageManager::ShouldDelayDumpOplog() {
    // Dump slot after its subsequent logs have accumulated. So that multiple updates
    // could have chance to merge.
    uint64_t wait_dump_oplog_length = op_logger_->UnDumpLength();
    if (wait_dump_oplog_length < FLAGS_storage_oplog_delay_dump_length) {
        LOG_DEBUG("Delay oplog dump")
            .put("PartitionId", partition_->GetPartitionID())
            .put("WaitDumpLength", wait_dump_oplog_length)
            .put("DirtySlotsSize", slot_context_manager_->DirtySlotsNum());
        return true;
    }
    return false;
}

// at most limit dirty slots to reclaim
void StorageManager::ReclaimOpLogWithLimit(int limit) {
    LOG_CALL_DEBUG().put("PartitionId", partition_->GetPartitionID());
    if (slot_context_manager_->DirtySlotsEmpty()) {
        // no dirty slots, no write operations
        return;
    }

    uint64_t reclaim_log_id = 0;
    std::vector<uint64_t> slots;
    while (limit-- > 0 && !slot_context_manager_->DirtySlotsEmpty()) {
        uint64_t dirty_slot = slot_context_manager_->GetLastDirtySlot();
        slot_context_manager_->PopLastDirtySlot();
        slots.push_back(dirty_slot);  // dump it later
        BYTE_ASSERT(slot_context_manager_->GetSlotFirstDirtyLogId(dirty_slot) >= reclaim_log_id)
            << slot_context_manager_->GetSlotFirstDirtyLogId(dirty_slot) << " " << reclaim_log_id;
        reclaim_log_id = slot_context_manager_->GetSlotFirstDirtyLogId(dirty_slot);
        LOG_DEBUG("Ready dump slot")
            .put("PartitionId", partition_->GetPartitionID())
            .put("SlotId", dirty_slot)
            .put("DirtyLogPos", reclaim_log_id);
    }
    if (slots.empty()) {
        return;
    }

    // dump dirty slots, then related oplog can
    // be safely deleted, and the index will be
    // updated in this function call
    // FIXME(wangtai.10): use SYNC_CALL
    Controller ctrl;
    CoSyncClosure sync;
    byte::InvokeInCurrentThread(NewClosure(slot_store_, &SlotStore::DumpSlots, &ctrl, slots,
                                           static_cast<Closure<void>*>(&sync)));
    sync.Wait();
    if (ctrl.status().IsStreamAbort()) {
        LOG_WARNING("Stream aborted, stop storage manager")
            .put("PartitionId", partition_->GetPartitionID());
        stopped_ = true;
        return;
    }
    BYTE_ASSERT(ctrl.status().ok()) << ctrl.status();
    PARTITION_FAULT_INJECT("oplog_gc/reclaim_oplog/after_slot_store_dump");

    // delay truncate to gc
    if (reclaim_log_id > 0) {
        index_->SetDumpedLogId(reclaim_log_id);  // all log entries before have been dumped
    }
    PARTITION_FAULT_INJECT("oplog_gc/reclaim_oplog/after_index_set_dumped_log_id");
}

void StorageManager::ReclaimMemory() {
    LOG_CALL_DEBUG().put("PartitionId", partition_->GetPartitionID());

    if (!opts_.enable_evict && !opts_.enable_expire) {
        return;
    }

    ScopedLatency latency(metrics_.reclaim_memory_latency->get());

    if (opts_.enable_expire && FLAGS_storage_enable_expire) {
        expirer_->TryExpire();
    }

    if (opts_.enable_evict && FLAGS_storage_enable_evict) {
        evicter_->TryEvict();
    }
}

// both page store and oplog
void StorageManager::ReclaimPage() {
    LOG_CALL_DEBUG().put("PartitionId", partition_->GetPartitionID());
    if (!opts_.enable_page_gc || !FLAGS_storage_enable_page_gc) {
        return;
    }
    ScopedLatency latency(metrics_.reclaim_page_latency->get());
    Status status = page_gc_->Gc();
    if (status.IsStreamAbort()) {
        LOG_WARNING("Stream aborted, stop storage manager")
            .put("PartitionId", partition_->GetPartitionID());
        stopped_ = true;
        return;
    }
    BYTE_ASSERT(status.ok()) << status;
}

void StorageManager::ReclaimIndex() {
    LOG_CALL_DEBUG().put("PartitionId", partition_->GetPartitionID());
    if (!opts_.enable_index_gc || !FLAGS_storage_enable_index_gc) {
        return;
    }

    ScopedLatency latency(metrics_.reclaim_index_latency->get());
    bool dirty_slot = false;
    uint64_t truncate_id = 0;
    index_->TryGc(&dirty_slot, &truncate_id);
    PARTITION_FAULT_INJECT("index_gc/reclaim_index/after_try_gc");
    // slots that need dumping
    if (dirty_slot) {
        Controller ctrl;
        SYNC_CALL(op_logger_->Commit, &ctrl);
        if (ctrl.status().IsStreamAbort()) {
            LOG_WARNING("Stream aborted, stop storage manager")
                .put("PartitionId", partition_->GetPartitionID());
            stopped_ = true;
            return;
        }
        BYTE_ASSERT(ctrl.status().ok()) << ctrl.status();
    }
    PARTITION_FAULT_INJECT("index_gc/reclaim_index/after_commit_dirty_slots");
    // index log entries before this ID can be safely discarded
    if (truncate_id != 0) {
        index_->Truncate(truncate_id);
    }
    PARTITION_FAULT_INJECT("index_gc/reclaim_index/after_index_truncate");
}

void StorageManager::ReapMetrics() {
    index_->MutableMetrics()->index_oplog_gap->get()->Set(op_logger_->Length() -
                                                          index_->GetDumpedLogId());
    metrics_.max_memory->get()->Set(partition_->GetConfig().evicter_config().maxmemory().value());
    metrics_.used_memory->get()->Set(allocator_manager_->GetTotalAllocedSize());
    index_->ReapMetrics();
    allocator_manager_->ReapMetrics();
    page_store_->ReapMetrics();
    op_logger_->ReapMetrics();
    cmd_executor_->ReapMetrics();

    auto func = [this](uint64_t slot_id, SlotNode* slot) -> bool {
        index_->MutableMetrics()->slot_object_num->get()->Set(slot->GetObjectNum());
        index_->MutableMetrics()->slot_page_num->get()->Set(slot->GetPageNum());

        // aggregate pages in object
        std::vector<uint64_t> object_page_size;
        for (auto& page : slot->GetPages()) {
            if (page.page_size == 0) {
                // ignore deleted page
                continue;
            }
            if (page.object_id >= object_page_size.size()) {
                object_page_size.resize(page.object_id + 1, 0);
            }
            object_page_size[page.object_id] += page.page_size;
        }

        for (size_t object_id = 0; object_id < object_page_size.size(); ++object_id) {
            if (object_page_size[object_id] > 0) {
                index_->MutableMetrics()->slot_object_page_size->get()->Set(
                    object_page_size[object_id]);
            }
        }

        return true;
    };

    if (!slot_iterator_->Scan(FLAGS_reap_metrics_max_slots_per_round, func)) {
        slot_iterator_ = index_->NewSlotIterator();
    }
}

void StorageManager::CompactPages() {
    LOG_CALL_DEBUG()
        .put("PartitionId", partition_->GetPartitionID())
        .put("enable_page_compaction", opts_.enable_page_compaction);
    if (!opts_.enable_page_compaction || !FLAGS_storage_enable_page_compaction) {
        return;
    }

    ScopedLatency latency(metrics_.page_compaction_latency->get());

    // FIXME(wangtai.10): use SYNC_CALL
    Controller ctrl;
    CoSyncClosure sync;
    byte::InvokeInCurrentThread(NewClosure(page_compactor_, &PageCompactor::CompactPages, &ctrl,
                                           static_cast<Closure<void>*>(&sync)));
    sync.Wait();
    if (ctrl.status().IsStreamAbort()) {
        LOG_WARNING("Stream aborted, stop storage manager")
            .put("PartitionId", partition_->GetPartitionID());
        stopped_ = true;
        return;
    }
    BYTE_ASSERT(ctrl.status().ok()) << ctrl.status();
}

}  // namespace partition
}  // namespace bcache2
