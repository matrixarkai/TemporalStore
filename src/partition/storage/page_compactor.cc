// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/storage/page_compactor.h"

#include <algorithm>
#include <utility>

#include "common/scoped_invoker.h"
#include "model/model_manager.h"
#include "partition/partition.h"
#include "partition/storage/slot_store.h"

DECLARE_uint64(page_compaction_max_slots_per_round);

namespace bcache2 {
namespace partition {

PageCompactor::PageCompactor(Partition* partition, Index* index, SlotStore* slot_store,
                             MetricsManager* metrics_manager)
    : partition_(partition), index_(index), slot_store_(slot_store) {
    metrics_.Init(metrics_manager);
    slot_iterator_ = index->NewSlotIterator();
}

void PageCompactor::CompactPages(Controller* ctrl, Closure<void>* callback) {
    metrics_.loop_qps->get()->Increment();

    ScopedInvoker done(callback);
    std::unique_ptr<CompactContext> ctx(new CompactContext());
    auto func = [this, &ctx](uint64_t slot_id, SlotNode* slot) -> bool {
        LOG_DEBUG("Page compaction scan slot")
            .put("PartitionId", partition_->GetPartitionID())
            .put("SlotId", slot_id);

        // aggregate pages in object
        std::vector<PageIndex> pages = slot->GetPages();
        std::vector<std::vector<PageIndex>> object_pages;
        std::vector<bool> object_need_skip;
        for (auto& page : pages) {
            if (page.page_size == 0) {
                // ignore deleted page
                continue;
            }
            if (page.object_id >= object_pages.size()) {
                object_pages.resize(page.object_id + 1);
                object_need_skip.resize(page.object_id + 1, false);
            }
            object_pages[page.object_id].emplace_back(std::move(page));
            if (UNLIKELY(page.model_id == 0)) {
                // for compatibilitye,
                // this page was generated before the multi-page version,
                // so we can't compact this object
                object_need_skip[page.object_id] = true;
            }
        }

        for (size_t object_id = 0; object_id < object_pages.size(); ++object_id) {
            if (object_pages[object_id].empty()) {
                continue;
            }
            if (object_need_skip[object_id]) {
                continue;
            }

            uint8_t model_id = object_pages[object_id].front().model_id;
            LOG_DEBUG("Get compaction hint")
                .put("PartitionId", partition_->GetPartitionID())
                .put("SlotId", slot_id)
                .put("ObjectId", object_id)
                .put("ModelId", static_cast<int>(model_id))
                .put("PageNum", object_pages[object_id].size());
            auto res =
                model::ModelManager::CompactPagesHint(model_id, std::move(object_pages[object_id]));
            std::vector<PageIndex> page_indexes = std::move(res.first);
            bool remove_tombstone = res.second;
            if (page_indexes.empty()) {
                // no need compaction
                continue;
            }

            LOG_DEBUG("Add load page task")
                .put("PartitionId", partition_->GetPartitionID())
                .put("SlotId", slot_id)
                .put("ObjectId", static_cast<int>(object_id))
                .put("ModelId", static_cast<int>(model_id))
                .put("PageIndexNum", page_indexes.size());
            CompactTask task;
            task.slot_id = slot_id;
            task.model_id = model_id;
            task.object_id = object_id;
            task.remove_tombstone = remove_tombstone;
            for (auto& page_index : page_indexes) {
                task.load_page_indexes.emplace_back(slot_id, page_index);
            }
            ctx->compact_tasks.emplace_back(std::move(task));
        }
        return true;
    };

    if (!slot_iterator_->Scan(FLAGS_page_compaction_max_slots_per_round, func)) {
        slot_iterator_ = index_->NewSlotIterator();
    }
    metrics_.scan_slot_qps->get()->Add(FLAGS_page_compaction_max_slots_per_round);

    if (ctx->compact_tasks.empty()) {
        // no need load
        return;
    }

    done.Release();
    ctx->callback = callback;
    ctx->ctrl = ctrl;
    for (auto& task : ctx->compact_tasks) {
        LOG_DEBUG("Load pages")
            .put("PartitionId", partition_->GetPartitionID())
            .put("Context", ctx.get())
            .put("Task", &task)
            .put("SlotId", task.slot_id)
            .put("ObjectId", static_cast<int>(task.object_id))
            .put("ModelId", static_cast<int>(task.model_id))
            .put("PageIndexNum", task.load_page_indexes.size());
        slot_store_->LoadPages(&task.ctrl, task.load_page_indexes, &task.load_pages,
                               NewClosure(this, &PageCompactor::OnLoadPagesDone, ctx.get(), &task));
    }
    ctx.release();
}

void PageCompactor::OnLoadPagesDone(CompactContext* ctx, CompactTask* task) {
    byte::LogLevel log_level = byte::LOG_LEVEL_DEBUG;
    if (UNLIKELY(!task->ctrl.status().ok())) {
        log_level = byte::LOG_LEVEL_WARNING;
    }
    LOG_MESSAGE(log_level, "Load pages done")
        .put("PartitionId", partition_->GetPartitionID())
        .put("Context", ctx)
        .put("Task", task)
        .put("SlotId", task->slot_id)
        .put("ModelId", static_cast<int>(task->model_id))
        .put("ObjectId", static_cast<int>(task->object_id))
        .put("PageIndexNum", task->load_page_indexes.size())
        .put("Status", task->ctrl.status());

    if (!task->ctrl.status().ok()) {
        task->ctrl.Reset();
        task->update_index = false;  // load pages failed, so we do not update index
        OnDumpPagesDone(ctx, task);
        return;
    }
    task->update_index = true;  // update index after dump

    uint64_t total_load_size = 0;
    for (auto& index : task->load_page_indexes) {
        total_load_size += index.page_index.page_size;
    }
    metrics_.load_page_throughput->get()->Add(total_load_size);
    metrics_.load_page_num->get()->Set(task->load_page_indexes.size());
    metrics_.load_page_qps->get()->Add(task->load_page_indexes.size());

    task->dump_pages =
        model::ModelManager::CompactPages(task->model_id, task->load_pages, task->remove_tombstone);
    BYTE_ASSERT(!task->dump_pages.empty());

    uint64_t total_dump_size = 0;
    for (size_t i = 0; i < task->dump_pages.size();) {
        total_dump_size += task->dump_pages[i].data.size();
        if (task->dump_pages[i].data.empty()) {
            // get empty page after compaction, just ignore it
            std::swap(task->dump_pages[i], task->dump_pages.back());
            task->dump_pages.pop_back();
        } else {
            ++i;
        }
    }

    if (task->dump_pages.empty()) {
        OnDumpPagesDone(ctx, task);
        return;
    }

    LOG_DEBUG("Dump pages")
        .put("PartitionId", partition_->GetPartitionID())
        .put("Context", ctx)
        .put("Task", task)
        .put("SlotId", task->slot_id)
        .put("ObjectId", static_cast<int>(task->object_id))
        .put("ModelId", static_cast<int>(task->model_id))
        .put("LoadPages", task->load_pages.size())
        .put("DumpPages", task->dump_pages.size());
    slot_store_->DumpPages(&task->ctrl, &task->dump_pages,
                           NewClosure(this, &PageCompactor::OnDumpPagesDone, ctx, task));
    metrics_.dump_page_throughput->get()->Add(total_dump_size);
    metrics_.dump_page_num->get()->Set(task->dump_pages.size());
    metrics_.dump_page_qps->get()->Add(task->dump_pages.size());
    metrics_.compaction_ratio->get()->Set(
        total_load_size == 0 ? 0.0 : 100UL * total_dump_size / total_load_size);
}

void PageCompactor::OnDumpPagesDone(CompactContext* ctx, CompactTask* task) {
    LOG_DEBUG("Dump pages done, update index")
        .put("PartitionId", partition_->GetPartitionID())
        .put("Context", ctx)
        .put("Task", task)
        .put("SlotId", task->slot_id)
        .put("ObjectId", static_cast<int>(task->object_id))
        .put("ModelId", static_cast<int>(task->model_id))
        .put("LoadPages", task->load_pages.size())
        .put("DumpPages", task->dump_pages.size());

    if (UNLIKELY(task->ctrl.status().IsStreamAbort())) {
        task->update_index = false;
    } else {
        BYTE_ASSERT(task->ctrl.status().ok()) << task->ctrl.status();
    }

    if (LIKELY(task->update_index)) {
        std::vector<PageInfo> update_pages(task->load_pages.size() + task->dump_pages.size());
        size_t idx = 0;
        size_t max_log_sequence = 0;
        for (auto& page : task->load_pages) {
            // delete origin page
            LOG_DEBUG("Delete origin page")
                .put("PartitionId", partition_->GetPartitionID())
                .put("Context", ctx)
                .put("Task", task)
                .put("SlotId", task->slot_id)
                .put("Page", page.header.ShortDebugString());
            page.size = 0;
            max_log_sequence = std::max(max_log_sequence, page.header.oplog_sequence());
            update_pages[idx++] = std::move(page);
        }
        for (auto& page : task->dump_pages) {
            LOG_DEBUG("Insert new page")
                .put("PartitionId", partition_->GetPartitionID())
                .put("Context", ctx)
                .put("Task", task)
                .put("SlotId", task->slot_id)
                .put("Page", page.header.ShortDebugString());
            max_log_sequence = std::max(max_log_sequence, page.header.oplog_sequence());
            update_pages[idx++] = std::move(page);
        }
        index_->UpdateSlotPages(task->slot_id, max_log_sequence, update_pages);
    }

    metrics_.compact_object_qps->get()->Increment();
    ctx->finish_count += 1;
    if (ctx->finish_count == ctx->compact_tasks.size()) {
        index_->Commit(ctx->ctrl, ctx->callback);
        delete ctx;
        return;
    }
}

}  // namespace partition
}  // namespace bcache2
