// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "partition/storage/slot_store.h"

#include <algorithm>
#include <memory>
#include <set>
#include <unordered_map>
#include <utility>

#include "common/coclosure.h"
#include "common/logging.h"
#include "common/time_tracer.h"
#include "partition/index/index.h"
#include "partition/metrics.h"
#include "partition/partition.h"
#include "partition/partition_fiu.h"
#include "partition/storage/object_manager.h"
#include "partition/storage/op_logger.h"
#include "partition/storage/page_store.h"
#include "partition/storage/slot_context_manager.h"

DECLARE_uint64(object_size_alarm_threshold);

namespace bcache2 {
namespace partition {

SlotStore::SlotStore(Partition* partition, Index* index, PageStore* page_store, OpLogger* op_logger,
                     SlotContextManager* slot_context_manager, MetricsManager* metrics_manager)
    : partition_(partition),
      index_(index),
      page_store_(page_store),
      op_logger_(op_logger),
      slot_context_manager_(slot_context_manager) {
    metrics_.Init(metrics_manager);
}

void SlotStore::DumpSlots(Controller* ctrl, std::vector<uint64_t> slots, Closure<void>* callback) {
    LOG_CALL_DEBUG()
        .put("PartitionId", partition_->GetPartitionID())
        .put("SlotCount", slots.size());

    ScopedInvoker done(callback);
    if (slots.empty()) {
        return;
    }

    done.Release();
    DumpSlotsContext* ctx = new DumpSlotsContext();
    ctx->ctrl = ctrl;
    ctx->callback = callback;
    ctx->total_slot = slots.size();
    for (uint64_t slot_id : slots) {
        SlotNode* slot = index_->GetSlot(slot_id);
        BYTE_ASSERT(slot != nullptr) << slot_id;  // slot is dirty, we expect SlotNode is exist
        BYTE_ASSERT(slot->Dirty()) << slot_id;

        DumpSlotTask* task = new DumpSlotTask();
        task->slot = slot;
        task->slot_id = slot_id;
        ctx->dump_tasks.emplace_back(task);
        if (slot->InMemory()) {
            DumpSlotInMemory(ctx, task);
        } else {
            DumpSlotNotInMemory(ctx, task);
        }
    }
}

void SlotStore::OnLoadObjectDone(Controller* ctrl, uint64_t slot_id, Closure<void>* callback) {
    std::unique_ptr<Controller> _ctrl(ctrl);
    if (!ctrl->status().ok()) {
        LOG_WARNING("Load object failed")
            .put("PartitionId", partition_->GetPartitionID())
            .put("Status", ctrl->status())
            .put("SlotId", slot_id);
        // TODO(wangtai.10): retry backoff
        Controller* new_ctrl = new Controller();
        object_manager_->LoadObject(
            ctrl, slot_id, true,
            NewClosure(this, &SlotStore::OnLoadObjectDone, new_ctrl, slot_id, callback));
        return;
    }
    callback->Run();
}

void SlotStore::DumpSlotInMemory(DumpSlotsContext* context, DumpSlotTask* task) {
    LOG_CALL_DEBUG()
        .put("PartitionId", partition_->GetPartitionID())
        .put("SlotId", task->slot_id)
        .put("Context", context)
        .put("Task", task);
    SlotNode* slot = task->slot;
    BYTE_ASSERT(slot->InMemory());

    // MUST commit oplogger first to commit all page logs
    op_logger_->Commit(nullptr, nullptr);
    task->last_oplog_sequence = slot_context_manager_->GetSlotLastLogSequence(task->slot_id);

    // we only care about the dirty pages & meta
    task->pages.clear();
    task->creation.clear();
    task->metas.clear();
    std::vector<PageIndex> marked_pages;
    index_->GetSlotMarkedPagesAndMetas(task->slot_id, &marked_pages, &task->metas);
    for (const auto& index : marked_pages) {
        task->pages.emplace_back(PageInfo(index));
        task->creation.emplace_back(false);
    }

    // dump pages
    uint64_t dump_size = 0;
    std::vector<Object> object_list = slot->GetObjects();
    for (auto& object : object_list) {
        std::vector<PageIndex> object_pages;
        for (auto& page : slot->GetPages()) {
            if (page.object_id == object.ObjectId()) {
                object_pages.emplace_back(page);
            }
        }

        std::vector<partition::LogItem> object_logs;
        for (auto& log : slot_context_manager_->GetSlotLogs(task->slot_id)) {
            if (log.log.object_id() == object.ObjectId()) {
                object_logs.emplace_back(log);
            }
        }

        std::vector<std::pair<uint16_t, std::string>> pages = model::ModelManager::DumpNewPages(
            object.ModelId(), object.RawModelBuf(), object_pages, object_logs);
        for (auto& page : pages) {
            PageInfo page_info;
            page_info.header.set_slot_id(task->slot_id);
            page_info.header.set_object_id(object.ObjectId());
            page_info.header.set_page_id(page.first);
            if (page.second.empty()) {
                // indicates this page will be deleted
                page_info.size = 0;
                task->pages.push_back(page_info);
                task->creation.emplace_back(false);
                continue;
            }

            page_info.header.set_timestamp_ms(GetCurrentTimeInMs());
            page_info.header.set_model_id(object.ModelId());
            page_info.header.set_key(object.Key().data(), object.Key().size());
            page_info.header.set_page_in_log(false);
            page_info.header.set_version(task->last_oplog_sequence);
            page_info.header.set_oplog_sequence(task->last_oplog_sequence);
            page_info.data = std::move(page.second);

            // fill page_address and data_size inside WritePage
            page_store_->WritePage(&page_info);
            BYTE_ASSERT(page_info.size != 0);
            BYTE_ASSERT(page_info.address != 0);

            dump_size += page_info.size;
            metrics_.dump_page_size->get()->Set(page_info.size);
            metrics_.dump_throughput->get()->Add(page_info.size);
            task->pages.push_back(std::move(page_info));
            task->creation.emplace_back(true);
        }
    }

    LOG_DEBUG("Dump slot success")
        .put("PartitionId", partition_->GetPartitionID())
        .put("SlotId", task->slot_id)
        .put("Context", context)
        .put("Task", task)
        .put("PageNum", task->pages.size())
        .put("DumpSize", dump_size);
    metrics_.dump_qps->get()->Add(1);
    index_->ClearSlotDirty(task->slot_id);
    OnDumpSlotDone(context, task);
}

void SlotStore::DumpSlotNotInMemory(DumpSlotsContext* context, DumpSlotTask* task) {
    LOG_CALL_DEBUG()
        .put("PartitionId", partition_->GetPartitionID())
        .put("SlotId", task->slot_id)
        .put("Context", context)
        .put("Task", task);
    SlotNode* slot = task->slot;
    BYTE_ASSERT(!slot->InMemory());

    if (UNLIKELY(slot->Loading())) {
        // slot is loading, we cannot change the snapshot consistency of pages and logs
        Controller* ctrl = new Controller();
        object_manager_->LoadObject(
            ctrl, task->slot_id, true,
            NewClosure(this, &SlotStore::OnLoadObjectDone, ctrl, task->slot_id,
                       NewClosure(this, &SlotStore::DumpSlotInMemory, context, task)));
        return;
    }

    // MUST commit oplogger first to commit all page logs
    op_logger_->Commit(nullptr, nullptr);
    task->last_oplog_sequence = slot_context_manager_->GetSlotLastLogSequence(task->slot_id);

    // we only care about the dirty pages & meta
    task->pages.clear();
    task->creation.clear();
    task->metas.clear();
    std::vector<PageIndex> marked_pages;
    index_->GetSlotMarkedPagesAndMetas(task->slot_id, &marked_pages, &task->metas);
    for (const auto& index : marked_pages) {
        task->pages.emplace_back(PageInfo(index));
        task->creation.emplace_back(false);
    }

    // dump pages blindly
    std::vector<std::vector<partition::LogItem>> object_logs;
    for (auto& log : slot_context_manager_->GetSlotLogs(task->slot_id)) {
        uint8_t object_id = log.log.object_id();
        if (object_id >= object_logs.size()) {
            object_logs.resize(object_id + 1);
        }
        object_logs[object_id].emplace_back(log);
    }

    std::vector<std::vector<PageIndex>> object_pages;
    for (auto& page : slot->GetPages()) {
        if (page.object_id >= object_pages.size()) {
            object_pages.resize(page.object_id + 1);
        }
        object_pages[page.object_id].emplace_back(page);
    }

    size_t dump_size = 0;
    for (size_t object_id = 0; object_id < object_logs.size(); ++object_id) {
        if (object_logs[object_id].empty()) {
            continue;
        }

        std::string key = object_logs[object_id].back().log.object_key();
        uint8_t model_id = object_logs[object_id].back().log.model_id();
        std::vector<std::pair<uint16_t, std::string>> pages;
        Status status;
        if (UNLIKELY(model_id) == 0) {
            // logs was generated before the blind-dump version
            status =
                Status::FailedPrecondition("this page was generated before the blind-dump version");
        } else {
            const std::vector<PageIndex>& page_indexes =
                object_id >= object_pages.size()
                    ? std::vector<PageIndex>{}  // object only has logs
                    : object_pages[object_id];  // object has pages and logs
            status = model::ModelManager::BlindDumpNewPages(model_id, page_indexes,
                                                            object_logs[object_id], &pages);
        }
        if (!status.ok()) {
            LOG_DEBUG("Blind dump failed, try dump based on memory")
                .put("PartitionId", partition_->GetPartitionID())
                .put("SlotId", task->slot_id)
                .put("Context", context)
                .put("Task", task)
                .put("Status", status);
            Controller* ctrl = new Controller();
            object_manager_->LoadObject(
                ctrl, task->slot_id, true,
                NewClosure(this, &SlotStore::OnLoadObjectDone, ctrl, task->slot_id,
                           NewClosure(this, &SlotStore::DumpSlotInMemory, context, task)));
            return;
        }

        for (auto& page : pages) {
            PageInfo page_info;
            page_info.header.set_slot_id(task->slot_id);
            page_info.header.set_object_id(object_id);
            page_info.header.set_page_id(page.first);
            if (page.second.empty()) {
                // indicates this page will be deleted
                page_info.size = 0;
                task->pages.push_back(page_info);
                task->creation.emplace_back(false);
                continue;
            }

            page_info.header.set_timestamp_ms(GetCurrentTimeInMs());
            page_info.header.set_model_id(model_id);
            page_info.header.set_key(std::move(key));
            page_info.header.set_page_in_log(false);
            page_info.header.set_version(task->last_oplog_sequence);
            page_info.header.set_oplog_sequence(task->last_oplog_sequence);
            page_info.data = std::move(page.second);

            // fill page_address and data_size inside WritePage
            page_store_->WritePage(&page_info);
            BYTE_ASSERT(page_info.size != 0);
            BYTE_ASSERT(page_info.address != 0);

            dump_size += page_info.size;
            metrics_.dump_page_size->get()->Set(page_info.size);
            metrics_.dump_throughput->get()->Add(page_info.size);
            task->pages.push_back(std::move(page_info));
            task->creation.emplace_back(true);
        }
    }

    LOG_DEBUG("Dump slot success")
        .put("PartitionId", partition_->GetPartitionID())
        .put("SlotId", task->slot_id)
        .put("Context", context)
        .put("Task", task)
        .put("PageNum", task->pages.size())
        .put("DumpSize", dump_size);
    metrics_.blind_dump_qps->get()->Add(1);
    index_->ClearSlotDirty(task->slot_id);
    OnDumpSlotDone(context, task);
}

void SlotStore::OnDumpSlotDone(DumpSlotsContext* context, DumpSlotTask* task) {
    context->finish_slot += 1;

    LOG_CALL_DEBUG()
        .put("PartitionId", partition_->GetPartitionID())
        .put("SlotId", task->slot_id)
        .put("Context", context)
        .put("Task", task)
        .put("FinishSlot", context->finish_slot)
        .put("TotalSlot", context->total_slot);
    if (context->finish_slot < context->total_slot) {
        return;
    }
    BYTE_ASSERT(context->finish_slot == context->total_slot);

    {
        Controller* ctrl = new Controller();
        op_logger_->Commit(ctrl, NewClosure(this, &SlotStore::OnCommitDone, ctrl, context, true));
    }

    {
        Controller* ctrl = new Controller();
        page_store_->Commit(ctrl, NewClosure(this, &SlotStore::OnCommitDone, ctrl, context, false));
    }
}

void SlotStore::OnCommitDone(Controller* ctrl, DumpSlotsContext* context, bool oplog) {
    LOG_CALL_DEBUG()
        .put("PartitionId", partition_->GetPartitionID())
        .put("Context", context)
        .put("Oplog", oplog);

    std::unique_ptr<Controller> _ctrl(ctrl);
    if (!ctrl->status().ok()) {
        context->ctrl->set_status(ctrl->status());
    }

    if (oplog) {
        context->oplog_committed = true;
    } else {
        context->page_committed = true;
    }

    if (!context->oplog_committed || !context->page_committed) {
        // waiting for oplog and page all committed
        return;
    }

    ScopedInvoker done(context->callback);
    if (!context->ctrl->status().ok()) {
        // commit failed, do not touch index
        return;
    }

    done.Release();
    context->finish_slot = 0;
    metrics_.dump_latency->get()->Set(context->tracer.TotalSpentUs());
    for (auto& task : context->dump_tasks) {
        UpdateIndex(context, task.get());
    }
}

void SlotStore::UpdateIndex(DumpSlotsContext* context, DumpSlotTask* task) {
    LOG_CALL_DEBUG()
        .put("PartitionId", partition_->GetPartitionID())
        .put("Context", context)
        .put("Task", task)
        .put("SlotId", task->slot_id);

    if (UNLIKELY(task->slot->Loading())) {
        // slot is loading, we cannot change the snapshot consistency of pages and logs
        Controller* ctrl = new Controller();
        object_manager_->LoadObject(
            ctrl, task->slot_id, true,
            NewClosure(this, &SlotStore::OnLoadObjectDone, ctrl, task->slot_id,
                       NewClosure(this, &SlotStore::UpdateIndex, context, task)));
        return;
    }

    // we do a lot of context-switch, the whole world may have changed,
    // so do some filter here to avoid insert staled page
    slot_context_manager_->TrimSlotLogs(task->slot_id, task->last_oplog_sequence);
    std::vector<PageInfo> pages;
    for (size_t i = 0; i < task->pages.size(); ++i) {
        if (!task->creation[i]) {
            pages.emplace_back(std::move(task->pages[i]));
            continue;
        }

        uint64_t slot_id = task->pages[i].header.slot_id();
        uint64_t object_id = task->pages[i].header.object_id();
        if (slot_context_manager_->GetSlotObjectDeleteNum(slot_id, object_id) == 0) {
            pages.emplace_back(std::move(task->pages[i]));
            continue;
        }

        // other situation: page is staled
    }
    index_->UpdateSlotPagesAndMetas(task->slot_id, task->last_oplog_sequence, pages, task->metas);

    context->finish_slot++;
    if (context->finish_slot == context->total_slot) {
        // all finished
        context->callback->Run();
        delete context;
    }
}

// write all these pages to Page Store
void SlotStore::DumpPages(Controller* ctrl, std::vector<PageInfo>* pages, Closure<void>* callback) {
    LOG_CALL_DEBUG().put("PartitionId", partition_->GetPartitionID());
    for (size_t i = 0; i < pages->size(); ++i) {
        // these pages are written to the underlying storage, update their addresses
        // Dumped page may be loaded from oplog
        pages->at(i).header.set_page_in_log(false);
        page_store_->WritePage(&pages->at(i));
    }
    page_store_->Commit(ctrl, callback);
}

void SlotStore::LoadSlot(Controller* ctrl, uint64_t slot_id,
                         std::map<std::string, ObjectPages>* object_pages,
                         Closure<void>* callback) {
    LOG_CALL_DEBUG().put("PartitionId", partition_->GetPartitionID()).put("SlotId", slot_id);
    metrics_.load_qps->get()->Increment();
    LoadSlotContext* context = new LoadSlotContext;
    context->slot_id = slot_id;
    context->ctrl = ctrl;
    context->object_pages = object_pages;
    context->callback = callback;
    for (auto& index : index_->GetSlotPages(slot_id, false)) {
        context->indexes.emplace_back(slot_id, index);
    }
    if (context->indexes.empty()) {
        ctrl->set_status(Status::OK());
        byte::InvokeInCurrentThread(NewClosure(this, &SlotStore::OnLoadSlotDone, context));
        return;
    }
    LoadPages(ctrl, context->indexes, &context->pages,
              NewClosure(this, &SlotStore::OnLoadSlotDone, context));
}

void SlotStore::OnLoadSlotDone(LoadSlotContext* context) {
    LOG_CALL_DEBUG().put("SlotId", context->slot_id).put("PageSize", context->pages.size());
    std::unique_ptr<LoadSlotContext> _context(context);
    ScopedInvoker done(context->callback);
    metrics_.load_latency->get()->Set(context->cost.GetElapsedInUs());
    if (!context->ctrl->status().ok()) {
        metrics_.load_failed_qps->get()->Increment();
        LOG_ERROR("Load pages failed")
            .put("SlotId", context->slot_id)
            .put("Error", context->ctrl->status());
        std::vector<PageIndex> indexes = index_->GetSlotPages(context->slot_id, false);
        bool page_change = false;
        for (size_t i = 0; i < context->indexes.size() && i < indexes.size(); ++i) {
            page_change =
                page_change || context->indexes[i].page_index.address != indexes[i].address;
        }
        BYTE_ASSERT_DEBUG(
            context->indexes.size() != indexes.size() || page_change ||
            (!context->ctrl->status().IsOutOfRange() && !context->ctrl->status().IsNotFound()));
        return;
    }
    // group pages by key
    for (size_t i = 0; i < context->pages.size(); ++i) {
        auto& object_pages = (*context->object_pages)[context->pages[i].header.key()];
        if (!object_pages.empty()) {
            // TODO(wangluping.502): version strictly increase after invalid index log trimed
            BYTE_ASSERT(context->pages[i].header.version() >= object_pages.back().header.version());
        }
        object_pages.emplace_back(context->pages[i]);
    }
    context->ctrl->set_status(Status::OK());
}

void SlotStore::LoadPages(Controller* ctrl, const std::vector<SlotPageIndex>& indexes,
                          std::vector<PageInfo>* pages, Closure<void>* callback) {
    LOG_CALL_DEBUG()
        .put("PartitionId", partition_->GetPartitionID())
        .put("PageCount", indexes.size());
    LoadPagesContext* context = new LoadPagesContext;
    context->ctrl = ctrl;
    context->indexes = std::move(indexes);
    context->ctrls.resize(context->indexes.size());
    context->pages = pages;
    context->pages->resize(context->indexes.size());
    context->callback = callback;
    uint64_t total_size = 0;
    for (size_t i = 0; i < context->indexes.size(); ++i) {
        total_size += context->indexes[i].page_index.page_size;
        // the page index indicates where the page is
        if (context->indexes[i].page_index.page_in_log) {
            op_logger_->ReadPage(&context->ctrls[i], context->indexes[i].slot_id,
                                 context->indexes[i].page_index, &context->pages->at(i),
                                 NewClosure(this, &SlotStore::OnLoadPagesDone, context, i));
        } else {
            page_store_->ReadPage(&context->ctrls[i], context->indexes[i].page_index,
                                  &context->pages->at(i),
                                  NewClosure(this, &SlotStore::OnLoadPagesDone, context, i));
        }
    }
    metrics_.load_throughput->get()->Add(total_size);
}

void SlotStore::OnLoadPagesDone(LoadPagesContext* context, size_t index) {
    LOG_CALL_DEBUG().put("PartitionId", partition_->GetPartitionID()).put("Index", index);
    ++context->complete_count;
    BYTE_ASSERT(context->complete_count <= context->indexes.size());
    if (context->complete_count < context->indexes.size()) {
        return;
    }

    std::unique_ptr<LoadPagesContext> _context(context);
    ScopedInvoker done(context->callback);
    for (size_t i = 0; i < context->ctrls.size(); ++i) {
        if (!context->ctrls[i].status().ok()) {
            context->ctrl->set_status(context->ctrls[i].status());
            return;
        }
    }

    // sort by version
    // TODO(wangluping.502): only compare version after bug fixed
    std::sort(context->pages->begin(), context->pages->end(),
              [](const PageInfo& lhs, const PageInfo& rhs) {
                  if (lhs.header.version() < rhs.header.version()) {
                      return true;
                  } else if (lhs.header.version() == rhs.header.version()) {
                      return lhs.header.page_id() < rhs.header.page_id();
                  }
                  return false;
              });

    context->ctrl->set_status(Status::OK());
}

}  // namespace partition
}  // namespace bcache2
