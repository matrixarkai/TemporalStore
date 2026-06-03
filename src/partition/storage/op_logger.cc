// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/storage/op_logger.h"

#include "common/function_closure.h"
#include "common/scoped_invoker.h"
#include "partition/cmd_context.h"
#include "partition/index/index.h"
#include "partition/partition.h"

namespace bcache2 {
namespace partition {

OpLogger::OpLogger(Partition* partition, Index* index, SlotContextManager* slot_context_manager,
                   MetricsManager* metrics_manager)
    : partition_(partition), index_(index), slot_context_manager_(slot_context_manager) {
    metrics_.Init(metrics_manager);
}

OpLogger::~OpLogger() {}

void OpLogger::WriteKvLog(CmdContext* ctx, uint64_t slot_id, uint64_t object_id,
                          const absl::string_view& object_key, uint16_t model_id, std::string key,
                          std::string value, model::Property property) {
    LOG_CALL_DEBUG()
        .put("PartitionId", partition_->GetPartitionID())
        .put("ObjectKey", object_key)
        .put("ModelId", model_id)
        .put("Key", object_key)
        .put("Value", value);

    storage::OpLog::LogItem* kvlog = current_oplog_.add_item();
    kvlog->set_slot_id(slot_id);
    kvlog->set_object_key(object_key.data(), object_key.size());
    kvlog->set_model_id(model_id);
    kvlog->set_object_id(object_id);
    kvlog->set_key(std::move(key));
    kvlog->set_value(std::move(value));
    kvlog->set_timestamp_ms(GetCurrentTimeInMs());
    kvlog->set_cluster_id(property.cluster_id);
    kvlog->set_deleted(property.deleted);

    // update the slot in the index
    // OPTIMIZE(wangtai.10): pointer to kvlog
    index_->MarkSlotDataDirty(slot_id, Length(), current_sequence_ + 1);
    size_t log_num =
        slot_context_manager_->AddSlotLog(slot_id, Length(), current_sequence_ + 1, 0, *kvlog);
    LOG_DEBUG("Add slot log")
        .put("SlotId", slot_id)
        .put("Log", kvlog->ShortDebugString())
        .put("TotalLogNum", log_num);

    metrics_.write_kvlog_qps->get()->Increment();
    metrics_.write_kvlog_throughput->get()->Add(kvlog->ByteSize());
    metrics_.current_item_size->get()->Set(current_oplog_.item_size());
}

void OpLogger::WriteTtlLog(uint64_t slot_id, const absl::string_view& object_key, uint8_t model_id,
                           uint16_t object_id, uint64_t ttl) {
    LOG_CALL_DEBUG()
        .put("PartitionId", partition_->GetPartitionID())
        .put("SlotId", slot_id)
        .put("ObjectKey", object_key)
        .put("ObjectId", static_cast<uint16_t>(object_id))
        .put("Ttl", ttl);
    storage::OpLog::LogItem* log = current_oplog_.add_item();
    log->set_slot_id(slot_id);
    log->set_object_key(object_key.data(), object_key.size());
    log->set_object_id(object_id);
    log->set_ttl(ttl);
    log->set_meta_log(true);
    log->set_model_id(model_id);
    log->set_timestamp_ms(GetCurrentTimeInMs());

    index_->MarkSlotObjectTtlDirty(slot_id, Length(), current_sequence_ + 1, object_id, ttl);
    size_t log_num =
        slot_context_manager_->AddSlotLog(slot_id, Length(), current_sequence_ + 1, 0, *log);
    LOG_DEBUG("Add slot log")
        .put("SlotId", slot_id)
        .put("Log", log->ShortDebugString())
        .put("TotalLogNum", log_num);

    metrics_.write_ttl_log_qps->get()->Increment();
    metrics_.write_ttl_log_throughput->get()->Add(log->ByteSize());
    metrics_.current_item_size->get()->Set(current_oplog_.item_size());
}

void OpLogger::WritePage(CmdContext* ctx, uint64_t slot_id, const absl::string_view& object_key,
                         uint16_t model_id, uint16_t object_id, uint16_t page_id, std::string page,
                         uint64_t ttl) {
    LOG_CALL_DEBUG()
        .put("PartitionId", partition_->GetPartitionID())
        .put("SlotId", slot_id)
        .put("ObjectKey", object_key)
        .put("Model", model_id)
        .put("ObjectId", static_cast<uint16_t>(object_id))
        .put("Ttl", ttl);
    BYTE_ASSERT(!page.empty());
    storage::OpLog::LogItem* kvlog = current_oplog_.add_item();
    kvlog->set_slot_id(slot_id);
    kvlog->set_object_key(object_key.data(), object_key.size());
    kvlog->set_model_id(model_id);
    kvlog->set_page_log(true);
    kvlog->set_object_id(object_id);
    kvlog->set_page_id(page_id);
    kvlog->set_page(std::move(page));
    kvlog->set_ttl(ttl);
    kvlog->set_timestamp_ms(GetCurrentTimeInMs());

    // the index is not updated here but later in Commit()
    // we do not known log_id and log_size now, so reserve 0 here
    size_t log_num =
        slot_context_manager_->AddSlotLog(slot_id, 0, current_sequence_ + 1, 0, *kvlog);
    LOG_DEBUG("Add slot log")
        .put("SlotId", slot_id)
        .put("Log", kvlog->ShortDebugString())
        .put("TotalLogNum", log_num);
    stage_pages_.emplace_back(Log2Page(0, current_sequence_ + 1, 0, *kvlog),
                              slot_context_manager_->MutableLastLog(slot_id));

    metrics_.write_page_qps->get()->Increment();
    metrics_.write_page_throughput->get()->Add(kvlog->ByteSize());
    metrics_.current_item_size->get()->Set(current_oplog_.item_size());
}

void OpLogger::DeleteObject(uint64_t slot_id, uint64_t object_id,
                            const absl::string_view& object_key, uint16_t model_id) {
    LOG_CALL_DEBUG()
        .put("PartitionId", partition_->GetPartitionID())
        .put("ObjectKey", object_key)
        .put("ModelId", model_id);
    storage::OpLog::LogItem* log = current_oplog_.add_item();
    log->set_slot_id(slot_id);
    log->set_object_id(object_id);
    log->set_object_key(object_key.data(), object_key.size());
    log->set_model_id(model_id);
    log->set_object_deleted(true);
    log->set_timestamp_ms(GetCurrentTimeInMs());

    index_->MarkSlotObjectDeleted(slot_id, Length(), current_sequence_ + 1, object_id);
    size_t log_num =
        slot_context_manager_->AddSlotLog(slot_id, Length(), current_sequence_ + 1, 0, *log);
    LOG_DEBUG("Add slot log")
        .put("SlotId", slot_id)
        .put("Log", log->ShortDebugString())
        .put("TotalLogNum", log_num);

    metrics_.write_del_log_qps->get()->Increment();
    metrics_.write_del_log_throughput->get()->Add(log->ByteSize());
    metrics_.current_item_size->get()->Set(current_oplog_.item_size());
}

// serialize all current oplog entries and write them to the stream
void OpLogger::Commit(Controller* ctrl, Closure<void>* callback) {
    LOG_CALL_DEBUG()
        .put("PartitionId", partition_->GetPartitionID())
        .put("CurrentOplogSize", current_oplog_.item_size());
    if (current_oplog_.item_size() != 0) {
        current_oplog_.set_sequence(current_sequence_ + 1);
        std::string log;
        current_oplog_.set_version(kOpLogVersion);
        BYTE_ASSERT(current_oplog_.SerializeToString(&log));

        uint64_t id = 0;
        uint32_t size = log.size();
        stream_->Append(std::move(log), &id);
        for (auto& stage_page : stage_pages_) {
            stage_page.page_info.address = stage_page.log->log_id = id;
            stage_page.page_info.size = stage_page.log->log_size = size;

            // TODO(wangtai.10): verify page ABA issue,
            //                   UT case: PartitionTest.ObjectDeletedDuringPageLogCommit
            index_->MarkSlotPageDirty(stage_page.page_info.header.slot_id(), id,
                                      current_sequence_ + 1, {stage_page.page_info});
        }
        current_oplog_.Clear();
        stage_pages_.clear();
        current_sequence_ += 1;
    }
    if (callback != nullptr) {
        stream_->Commit(ctrl, callback);
    }
}

void OpLogger::Truncate(uint64_t log_id) { stream_->Truncate(log_id); }

OpLogger::ScopedIterator OpLogger::NewIterator(uint64_t log_id) {
    return ScopedIterator(new Iterator(stream_->NewIterator(log_id, UINT64_MAX)));
}

void OpLogger::ReadPage(Controller* ctrl, uint64_t slot_id, PageIndex index, PageInfo* page_info,
                        Closure<void>* callback) {
    LOG_CALL_DEBUG()
        .put("PartitionId", partition_->GetPartitionID())
        .put("ObjectId", static_cast<uint16_t>(index.object_id))
        .put("PageId", index.page_id)
        .put("Address", index.address)
        .put("Size", index.page_size);
    metrics_.read_page_qps->get()->Increment();
    metrics_.read_page_throughput->get()->Add(index.page_size);
    ReadPageTask* task = new ReadPageTask;
    task->slot_id = slot_id;
    task->ctrl = ctrl;
    task->page_index = index;
    task->page_info = page_info;
    task->callback = callback;
    task->buffer.resize(index.page_size);
    stream_->Read(ctrl, index.address, &task->buffer[0], index.page_size,
                  NewClosure(this, &OpLogger::OnReadPageDone, task));
}

void OpLogger::OnReadPageDone(ReadPageTask* task) {
    LOG_CALL_DEBUG()
        .put("PartitionId", partition_->GetPartitionID())
        .put("ObjectId", static_cast<uint16_t>(task->page_index.object_id))
        .put("SlotId", task->slot_id)
        .put("PageId", task->page_index.page_id)
        .put("Address", task->page_index.address)
        .put("Size", task->page_index.page_size)
        .put("Status", task->ctrl->status());
    std::unique_ptr<ReadPageTask> _task(task);
    ScopedInvoker done(task->callback);
    metrics_.read_page_latency->get()->Set(task->timer.GetElapsedInUs());
    if (!task->ctrl->status().ok()) {
        return;
    }
    storage::OpLog log;
    if (!log.ParseFromArray(task->buffer.data(), task->buffer.size())) {
        task->ctrl->set_status(Status::DataLoss(""));
        return;
    }
    const storage::OpLog::LogItem* item = nullptr;
    for (int i = 0; i < log.item_size(); ++i) {
        const storage::OpLog::LogItem& tmp = log.item(i);
        if (tmp.page_log() && tmp.slot_id() == task->slot_id &&
            tmp.object_id() == task->page_index.object_id &&
            tmp.page_id() == task->page_index.page_id) {
            item = &tmp;
        }
    }
    if (item == nullptr) {
        task->ctrl->set_status(Status::DataLoss(""));
        return;
    }

    *task->page_info =
        Log2Page(task->page_index.address, log.sequence(), task->page_index.page_size, *item);
    task->ctrl->set_status(Status::OK());
}

Status OpLogger::Iterator::Next() {
    Status status = iter_->Next();
    if (status.ok()) {
        log_id_ = iter_->Id();
        size_ = iter_->Data().size();
        if (!log_.ParseFromArray(iter_->Data().data(), iter_->Data().size())) {
            return Status::DataLoss("");
        }
    }
    return status;
}

}  // namespace partition
}  // namespace bcache2
