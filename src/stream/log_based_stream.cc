// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "stream/log_based_stream.h"

#include <absl/strings/numbers.h>
#include <byte/algorithm/crc32.h>
#include <byte/include/assert.h>
#include <byte/string/format.h>
#include <gflags/gflags.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <algorithm>
#include <limits>
#include <utility>

#include "common/function_closure.h"
#include "common/logging.h"
#include "common/scoped_invoker.h"
#include "common/time.h"
#include "protocol/storage.pb.h"
#include "stream/log_based_env.h"
#include "stream/metrics.h"
#include "stream/store_layer.h"
#include "stream/stream.h"

DECLARE_uint64(stream_max_blob_size);
DECLARE_uint64(stream_blob_deletion_min_age);
DECLARE_uint64(stream_blob_deletion_min_gap);
DECLARE_uint64(stream_blob_switch_retry_interval_us);

DECLARE_bool(stream_aggregate_flush);
DECLARE_string(stream_aggregate_flush_profile);
DECLARE_uint64(stream_aggregate_flush_loop_interval_ms);
DECLARE_uint64(stream_aggregate_flush_batch_size_byte);

namespace bcache2 {
namespace stream {
namespace {

void ApplyAggregateFlushProfile() {
    static bool applied = false;
    if (applied) {
        return;
    }
    applied = true;

    if (FLAGS_stream_aggregate_flush_profile == "custom") {
        return;
    }

    if (FLAGS_stream_aggregate_flush_profile == "low_latency") {
        FLAGS_stream_aggregate_flush_loop_interval_ms = 1;
        FLAGS_stream_aggregate_flush_batch_size_byte = 256 * 1024;
    } else if (FLAGS_stream_aggregate_flush_profile == "throughput") {
        FLAGS_stream_aggregate_flush_loop_interval_ms = 5;
        FLAGS_stream_aggregate_flush_batch_size_byte = 1024 * 1024;
    } else if (FLAGS_stream_aggregate_flush_profile == "batch_ingest") {
        FLAGS_stream_aggregate_flush_loop_interval_ms = 50;
        FLAGS_stream_aggregate_flush_batch_size_byte = 4 * 1024 * 1024;
    } else {
        FLAGS_stream_aggregate_flush_loop_interval_ms = 2;
        FLAGS_stream_aggregate_flush_batch_size_byte = 512 * 1024;
    }

    LOG_INFO("Applied stream aggregate flush profile")
        .put("Profile", FLAGS_stream_aggregate_flush_profile)
        .put("Enabled", FLAGS_stream_aggregate_flush)
        .put("LoopIntervalMs", FLAGS_stream_aggregate_flush_loop_interval_ms)
        .put("BatchSizeBytes", FLAGS_stream_aggregate_flush_batch_size_byte);
}

}  // namespace

StreamImpl::StreamImpl(LogBasedEnv* env, const std::string& uri, const Env::Condition& condition,
                       StoreRepPolicy rep_policy, const std::string& token,
                       MetricsManager* metrics_manager)
    : stream_base_(new StreamBaseImpl(env, uri, metrics_manager)),
      condition_(condition),
      rep_policy_(rep_policy),
      token_(token),
      metrics_manager_(metrics_manager) {
    metrics_.Init(metrics_manager, uri);
}

StreamImpl::~StreamImpl() { BYTE_ASSERT(commit_tasks_.Empty()); }

void StreamImpl::LoopFlush() {
    if (UNLIKELY(!IsCoContext())) {
        if (stop_loop_flush_sync_ != nullptr) {
            stop_loop_flush_sync_->Run();
            return;
        }
        if (FLAGS_stream_aggregate_flush) {
            TryAppend(false);
        }
        byte::InvokeLaterInCurrentThread(FLAGS_stream_aggregate_flush_loop_interval_ms * 1000,
                                         NewCoClosure(this, &StreamImpl::LoopFlush));
        return;
    }
    while (stop_loop_flush_sync_ == nullptr) {
        CoSleep(FLAGS_stream_aggregate_flush_loop_interval_ms * 1000);
        if (stop_loop_flush_sync_ == nullptr && FLAGS_stream_aggregate_flush) {
            TryAppend(false);
        }
    }
    if (stop_loop_flush_sync_) {
        stop_loop_flush_sync_->Run();
    }
}

Status StreamImpl::Load() {
    ApplyAggregateFlushProfile();

    std::vector<BlobInfo> tmp_blobs;
    std::vector<BlobInfo> data_blobs;
    Status status = stream_base_->ListBlobs(&tmp_blobs, &data_blobs);
    if (!status.ok()) {
        LOG_ERROR("List blobs failed")
            .put("Uri", stream_base_->Uri())
            .put("Error", status.ToString());
        return status;
    }

    // Clean tmp blobs
    for (auto blob : tmp_blobs) {
        std::string blob_uri = stream_base_->Uri() + BlobInfoToName(blob);
        Controller ctrl;
        Store::DeleteOptions options;
        options.condition = condition_;
        stream_base_->GetStoreLayer()->Delete(&ctrl, blob_uri.c_str(), options);
        if (!ctrl.status().ok()) {
            LOG_ERROR("Delete tmp blob failed")
                .put("Uri", blob_uri)
                .put("Error", ctrl.status().ToString());
        }
    }

    google::protobuf::RepeatedPtrField<storage::BlobInfo> blob_infos;
    google::protobuf::RepeatedPtrField<storage::BlobInfo> obsolete_blobs;
    BlobTailInfo tail_info;
    if (data_blobs.size() > 0) {
        std::string last_blob_name = data_blobs.back().name;
        status = SealBlob(last_blob_name);  // seal the last blob
        if (!status.ok()) {
            LOG_ERROR("Seal blob failed")
                .put("Uri", stream_base_->Uri())
                .put("BlobName", stream_base_->BlobHeader().blob_name())
                .put("Error", status.ToString());
            // TODO(zhangyuan.42): Need return
            // return status;
        }

        storage::BlobHeader blob_header;
        // scan the last blob to recover the offset, sequence, etc...
        status = TailScanBlob(last_blob_name, &blob_header, &tail_info);
        if (!status.ok()) {
            LOG_ERROR("Tail scan failed")
                .put("Uri", stream_base_->Uri())
                .put("LastBlobName", last_blob_name)
                .put("Error", status.ToString());
            return status;
        }
        tail_info.blob_id = data_blobs.back().blob_id;
        LOG_INFO("Tail scan")
            .put("Uri", stream_base_->Uri())
            .put("LastBlobName", last_blob_name)
            .put("BlobHeader", blob_header.ShortDebugString())
            .put("TailInfo", tail_info.ToString());
        blob_infos = std::move(*blob_header.mutable_data_blobs());
        obsolete_blobs = std::move(*blob_header.mutable_obsolete_blobs());
        storage::BlobInfo last_blob_info = FillBlobInfo(blob_header, true);
        last_blob_info.set_end_record_sequence(tail_info.end_record_sequence);
        last_blob_info.set_blob_end_offset(tail_info.blob_end_offset);
        last_blob_info.set_freeze_ms(GetCurrentTimeInMs());
        last_blob_info.set_end_offset(tail_info.end_offset);
        last_blob_info.set_truncated_offset(tail_info.truncated_offset);
        *blob_infos.Add() = last_blob_info;
    }

    incoming_sequence_ = inflight_sequence_ = persistent_sequence_ = tail_info.end_record_sequence;
    inflight_offset_ = persistent_offset_ = tail_info.end_offset;
    inflight_blob_offset_ = persistent_blob_offset_ = tail_info.blob_end_offset;
    incoming_block_crc32c_ = inflight_block_crc32c_ = persistent_block_crc32c_ =
        tail_info.last_block_crc32c;
    incoming_truncated_offset_ = inflight_truncated_offset_ = persistent_truncated_offset_ =
        tail_info.truncated_offset;

    stream_buffer_.reset(new StreamBuffer<Delimiter>(inflight_offset_));

    Status res = New(blob_infos, obsolete_blobs, tail_info);
    if (FLAGS_stream_aggregate_flush && res.ok()) {
        byte::InvokeInCurrentThread(NewCoClosure(this, &StreamImpl::LoopFlush));
    }
    return res;
}

void StreamImpl::Close(Closure<void>* callback) {
    LOG_CALL_INFO().put("Uri", stream_base_->Uri());
    if (!IsCoContext()) {
        LOG_WARNING("Stream close running outside coroutine context")
            .put("Uri", stream_base_->Uri());
    }

    ScopedInvoker done(callback);
    if (closed_) {
        return;
    }

    if (!staled_ && inflight_offset_ != persistent_offset_) {
        CoSyncClosure sync;
        close_callback_ = &sync;  // wait for inflight operations finish to avoid callback coredump
        closed_ = true;
        sync.Wait();
        close_callback_ = nullptr;
    } else {
        closed_ = true;
    }

    if (FLAGS_stream_aggregate_flush) {
        CoSyncClosure sync;
        stop_loop_flush_sync_ = &sync;
        sync.Wait();
    }

    while (!commit_tasks_.Empty()) {
        LOG_INFO("Clear closed callback")
            .put("Uri", stream_base_->Uri())
            .put("Offset", commit_tasks_.Front().offset);
        commit_tasks_.Front().ctrl->set_status(Status::StreamAbort("Stream has been closed"));
        commit_tasks_.Front().callback->Run();
        commit_tasks_.Pop();
    }

    if (writing_blob_ != nullptr) {
        writing_blob_->Close();
    }
}

void StreamImpl::Append(Controller* ctrl, const void* data, size_t size, uint64_t* id,
                        Closure<void>* callback) {
    Append(std::string(reinterpret_cast<const char*>(data), size), id);
    Commit(ctrl, callback);
}

void StreamImpl::Append(std::string data, uint64_t* id) { AppendV({std::move(data)}, id); }

void StreamImpl::AppendV(std::vector<std::string> data, uint64_t* id) {
    AppendToBuffer(std::move(data), id);
}

void StreamImpl::Commit(Controller* ctrl, Closure<void>* callback) {
    std::lock_guard<std::recursive_mutex> lock(stream_mu_);
    LOG_CALL_DEBUG()
        .put("Uri", stream_base_->Uri())
        .put("IncomingOffset", stream_buffer_->Length())
        .put("PersistentOffset", persistent_offset_);
    if (UNLIKELY(closed_ || staled_)) {
        ctrl->set_status(Status::StreamAbort("Stream has been closed or staled"));
        byte::InvokeInCurrentThread(callback);
        return;
    }
    if (stream_buffer_->Length() == persistent_offset_) {
        ctrl->set_status(Status::OK());
        byte::InvokeInCurrentThread(callback);
        return;
    }
    metrics_.commit_qps->get()->Increment();
    CommitTask task;
    task.ctrl = ctrl;
    task.callback = callback;
    task.offset = stream_buffer_->Length();
    commit_tasks_.Push(task);
    TryAppend(false);
}

void StreamImpl::AppendToBuffer(std::vector<std::string> datas, uint64_t* id) {
    std::lock_guard<std::recursive_mutex> lock(stream_mu_);
    size_t size = 0;
    uint32_t record_crc32c = 0;
    for (auto& data : datas) {
        size += data.size();
        record_crc32c = byte::CRCUtil::ComputeCRC32(record_crc32c, data.data(), data.size());
    }
    LOG_CALL_DEBUG()
        .put("Uri", stream_base_->Uri())
        .put("StreamLength", stream_buffer_->Length())
        .put("Size", size);

    char header_buffer[16];
    uint32_t record_header_size = 0;
    BYTE_ASSERT(WriteRecordHeader(size, record_crc32c, header_buffer, sizeof(header_buffer),
                                  &record_header_size));

    size_t block_offset = stream_buffer_->Length() % kBlockSize;
    // offset, header, record, footer
    if (LIKELY(record_header_size + size + block_offset <= kBlockSize - kBlockFooterSize)) {
        // Fast path, can hold the whole value
        *id = stream_buffer_->Length();
        incoming_block_crc32c_ =
            byte::CRCUtil::ComputeCRC32(incoming_block_crc32c_, header_buffer, record_header_size);
        for (auto& data : datas) {
            incoming_block_crc32c_ =
                byte::CRCUtil::ComputeCRC32(incoming_block_crc32c_, &data[0], data.size());
        }
        stream_buffer_->Append(std::string(header_buffer, record_header_size));
        stream_buffer_->AppendV(std::move(datas));

        block_offset = stream_buffer_->Length() % kBlockSize;
    } else {
        AppendToBufferSlowPath(std::move(datas), id);
    }

    LOG_DEBUG("Append data")
        .put("Uri", stream_base_->Uri())
        .put("Id", *id)
        .put("Sequence", incoming_sequence_)
        .put("Size", size)
        .put("IncommingBlockCrc", incoming_block_crc32c_);

    incoming_sequence_ += 1;
    if (stream_buffer_->DistanceWithLastDelimiter() >= kBlockSize) {  // put a delimiter, but why?
        stream_buffer_->PushDelimiter(
            Delimiter(incoming_sequence_, incoming_block_crc32c_, incoming_truncated_offset_));
    }

    TryAppend(FLAGS_stream_aggregate_flush);
}

// Write into multiple blocks (with footers as well)
void StreamImpl::AppendToBufferSlowPath(std::vector<std::string> datas, uint64_t* id) {
    size_t size = 0;
    uint32_t record_crc32c = 0;
    for (auto& data : datas) {
        size += data.size();
        record_crc32c = byte::CRCUtil::ComputeCRC32(record_crc32c, data.data(), data.size());
    }

    char header_buffer[16];
    uint32_t record_header_size = 0;
    BYTE_ASSERT(WriteRecordHeader(size, record_crc32c, header_buffer, sizeof(header_buffer),
                                  &record_header_size));
    datas.insert(datas.begin(), std::string(header_buffer, record_header_size));
    size += record_header_size;

    *id = stream_buffer_->Length();
    size_t block_offset = stream_buffer_->Length() % kBlockSize;
    size_t block_end = block_offset;
    if (block_offset + record_header_size + kBlockFooterSize > kBlockSize) {
        stream_buffer_->Append(std::string(kBlockSize - block_offset - kBlockFooterSize, '\0'));
        block_offset = kBlockSize - kBlockFooterSize;
        *id = UpperAlign(stream_buffer_->Length(), kBlockSize);
    }

    size_t piece_index = 0;
    size_t offset_in_piece = 0;
    size_t left_size = size;
    while (left_size > 0) {
        if (block_offset + kBlockFooterSize == kBlockSize) {
            storage::BlockFooter footer;
            footer.set_magic(kMagic);
            footer.set_version(kVersion);
            footer.set_timestamp_ms(GetCurrentTimeInMs());
            footer.set_block_crc(incoming_block_crc32c_);
            footer.set_block_number(stream_buffer_->Length() / kBlockSize);
            footer.set_block_end(block_end);
            footer.set_last_record_offset(stream_buffer_->Length());
            footer.set_last_record_left_size(left_size < size ? left_size : 0);
            footer.set_last_record_sequence(incoming_sequence_);
            footer.set_client_token(token_);
            footer.set_truncated_offset(incoming_truncated_offset_);

            // Append footer
            std::string buffer;
            buffer.resize(kBlockFooterSize);
            BYTE_ASSERT(SerializeBlockFooter(footer, &buffer[0], kBlockFooterSize));
            stream_buffer_->Append(std::move(buffer));
            BYTE_ASSERT(stream_buffer_->Length() % kBlockSize == 0);

            block_offset = 0;
            block_end = 0;
            incoming_block_crc32c_ = 0;
            continue;
        }
        size_t cur_size = std::min(datas[piece_index].size() - offset_in_piece,
                                   kBlockSize - kBlockFooterSize - block_offset);
        std::string data = datas[piece_index].substr(offset_in_piece, cur_size);
        incoming_block_crc32c_ =
            byte::CRCUtil::ComputeCRC32(incoming_block_crc32c_, data.data(), data.size());
        stream_buffer_->Append(std::move(data));
        offset_in_piece += cur_size;
        block_offset += cur_size;
        block_end += cur_size;
        left_size -= cur_size;

        BYTE_ASSERT(offset_in_piece <= datas[piece_index].size());
        if (offset_in_piece == datas[piece_index].size()) {
            piece_index += 1;
            offset_in_piece = 0;
        }
    }
}

void StreamImpl::TryAppend(bool aggregate_flush) {
    std::lock_guard<std::recursive_mutex> lock(stream_mu_);
    size_t size = stream_buffer_->DistanceWithFirstDelimiter();

    if (UNLIKELY(
            (inflight_offset_ != persistent_offset_ ||  // if there is already a pending TryAppend()
             stream_buffer_->Length() == inflight_offset_ || closed_ || staled_) ||
            (aggregate_flush &&  // loop flush periodically & size satisfied
             size < FLAGS_stream_aggregate_flush_batch_size_byte))) {
        LOG_DEBUG("Append to matrixobjectstore: no need")
            .put("Uri", stream_base_->Uri())
            .put("StreamLength", stream_buffer_->Length())
            .put("InflightOffset", inflight_offset_)
            .put("PersistentOffset", persistent_offset_)
            .put("Size", size);
        return;
    }

    Task* task = new Task;
    task->offset = inflight_offset_;
    task->data.resize(size);
    if (UNLIKELY(!stream_buffer_->CanReadFront(size))) {
        LOG_WARNING("Append to matrixobjectstore skipped because front buffer is not readable")
            .put("Uri", stream_base_->Uri())
            .put("StreamLength", stream_buffer_->Length())
            .put("StreamStart", stream_buffer_->Start())
            .put("InflightOffset", inflight_offset_)
            .put("PersistentOffset", persistent_offset_)
            .put("Size", size);
        delete task;
        return;
    }
    stream_buffer_->GetFrontData(&task->data[0], size);

    LOG_DEBUG("Try append to matrixobjectstore")
        .put("Uri", stream_base_->Uri())
        .put("Offset", task->offset)
        .put("Size", task->data.size());

    inflight_offset_ += task->data.size();
    inflight_sequence_ = stream_buffer_->HasDelimiter()
                             ? stream_buffer_->FrontDelimiter().first.sequence
                             : incoming_sequence_;
    inflight_block_crc32c_ = stream_buffer_->HasDelimiter()
                                 ? stream_buffer_->FrontDelimiter().first.block_crc
                                 : incoming_block_crc32c_;
    inflight_truncated_offset_ = stream_buffer_->HasDelimiter()
                                     ? stream_buffer_->FrontDelimiter().first.truncated_offset
                                     : incoming_truncated_offset_;

    AppendInternal(task);
}

void StreamImpl::AppendInternal(Task* task) {
    if (UNLIKELY(closed_ || staled_)) {
        LOG_WARNING("Stream is closing or staled").put("Uri", stream_base_->Uri());
        task->ctrl.set_status(Status::StreamAbort("Stream has been closed or staled"));
        byte::InvokeInCurrentThread(NewClosure(this, &StreamImpl::OnAppendDone, task));
        return;
    }
    if (UNLIKELY(inflight_blob_offset_ + task->data.size() > FLAGS_stream_max_blob_size)) {
        LOG_INFO("Append: need switch new blob")
            .put("Uri", stream_base_->Uri())
            .put("InflightOffset", inflight_offset_)
            .put("InflightBlobOffset", inflight_blob_offset_)
            .put("DataSize", task->data.size())
            .put("Task", task);
        // Switch the blob in a coroutine because SealAndNew may retry with CoSleep.
        ScheduleSwitchNewBlobToAppend(task);
        return;
    }

    task->blob_offset = inflight_blob_offset_;
    inflight_blob_offset_ += task->data.size();

    task->inplace = true;
    writing_blob_->Append(&task->ctrl, task->data.data(), task->data.size(),
                          NewClosure(this, &StreamImpl::OnAppendDone, task));
    task->inplace = false;
}

void StreamImpl::OnAppendDone(Task* task) {
    std::lock_guard<std::recursive_mutex> lock(stream_mu_);
    BYTE_ASSERT_DEBUG(!task->inplace);
    BYTE_ASSERT(task->offset == persistent_offset_);
    if (!task->ctrl.status().ok() && !closed_ && !staled_) {
        LOG_ERROR("Append matrixobjectstore failed, need to switch new blob")
            .put("Uri", stream_base_->Uri())
            .put("InflightOffset", inflight_offset_)
            .put("InflightBlobOffset", inflight_blob_offset_)
            .put("PersistentOffset", persistent_offset_)
            .put("PersistentBlobOffset", persistent_blob_offset_)
            .put("DataSize", task->data.size())
            .put("Error", task->ctrl.status().ToString())
            .put("Task", task);
        ScheduleSwitchNewBlobToAppend(task);
        return;
    }

    std::unique_ptr<Task> release_guard(task);
    // TODO(wangluping.502): last commit should return ok if this append is ok
    if (UNLIKELY(closed_)) {
        LOG_WARNING("Stream is closing")
            .put("Uri", stream_base_->Uri())
            .put("Offset", persistent_offset_)
            .put("BlobOffset", persistent_blob_offset_)
            .put("Size", task->data.size());
        if (close_callback_ != nullptr) {
            close_callback_->Run();
        }
        return;
    }

    if (UNLIKELY(staled_)) {
        LOG_WARNING("Stream is staled").put("Uri", stream_base_->Uri());
        std::vector<Closure<void>*> callbacks;
        while (!commit_tasks_.Empty()) {
            LOG_INFO("Clear staled callback")
                .put("Uri", stream_base_->Uri())
                .put("Offset", commit_tasks_.Front().offset);
            commit_tasks_.Front().ctrl->set_status(Status::StreamAbort("Stream is staled"));
            callbacks.push_back(commit_tasks_.Front().callback);
            commit_tasks_.Pop();
        }
        for (auto& callback : callbacks) {
            callback->Run();
        }
        return;
    }

    LOG_DEBUG("Append matrixobjectstore success")
        .put("Uri", stream_base_->Uri())
        .put("InflightOffset", inflight_offset_)
        .put("InflightBlobOffset", inflight_blob_offset_)
        .put("InflightSequence", inflight_sequence_)
        .put("InflightTruncatedOffset", inflight_truncated_offset_)
        .put("TaskOffset", task->offset)
        .put("PersistentOffset", persistent_offset_)
        .put("PersistentBlobOffset", persistent_blob_offset_)
        .put("PersistentSequence", persistent_sequence_)
        .put("PersistentTruncatedOffset", persistent_truncated_offset_)
        .put("Size", task->data.size());
    BYTE_ASSERT(task->blob_offset == persistent_blob_offset_);
    BYTE_ASSERT(persistent_sequence_ < inflight_sequence_);
    BYTE_ASSERT(persistent_offset_ + task->data.size() == inflight_offset_);
    BYTE_ASSERT(persistent_blob_offset_ + task->data.size() == inflight_blob_offset_);
    BYTE_ASSERT(persistent_truncated_offset_ <= inflight_truncated_offset_);
    persistent_sequence_ = inflight_sequence_;
    persistent_offset_ = inflight_offset_;
    persistent_blob_offset_ = inflight_blob_offset_;
    persistent_block_crc32c_ = inflight_block_crc32c_;
    if (persistent_offset_ / kBlockSize > task->offset / kBlockSize) {
        persistent_truncated_offset_ = inflight_truncated_offset_;
    }

    stream_base_->UpdateLength(persistent_offset_, persistent_blob_offset_, persistent_sequence_,
                               persistent_truncated_offset_);

    stream_buffer_->Trim(persistent_offset_);

    TryAppend(FLAGS_stream_aggregate_flush);  // keep appending

    RingArray<Closure<void>*> callbacks(0);
    while (!commit_tasks_.Empty() && commit_tasks_.Front().offset <= persistent_offset_) {
        LOG_DEBUG("Response callback")
            .put("Uri", stream_base_->Uri())
            .put("Offset", commit_tasks_.Front().offset);
        metrics_.commit_latency->get()->Set(commit_tasks_.Front().cost.GetElapsedInUs());
        commit_tasks_.Front().ctrl->set_status(Status::OK());
        callbacks.Push(commit_tasks_.Front().callback);
        commit_tasks_.Pop();
    }
    while (!callbacks.Empty()) {
        callbacks.Front()->Run();
        callbacks.Pop();
    }
}

void StreamImpl::ScheduleSwitchNewBlobToAppend(Task* task) {
    byte::InvokeLaterInCurrentThread(0, NewCoClosure(this, &StreamImpl::SwitchNewBlobToAppend, task));
}

void StreamImpl::SwitchNewBlobToAppend(Task* task) {
    if (UNLIKELY(!IsCoContext())) {
        LOG_WARNING("Switch new blob rescheduled outside coroutine context")
            .put("Uri", stream_base_->Uri())
            .put("Task", task);
        ScheduleSwitchNewBlobToAppend(task);
        return;
    }

    // Close writting blob
    writing_blob_->Close();
    writing_blob_.reset(nullptr);

    while (!closed_ && !staled_) {
        Status status = SealAndNew();
        if (status.IsStoreConditionFailed()) {
            staled_ = true;
        }
        if (status.ok() || status.IsStoreConditionFailed()) {
            break;
        }
        CoSleep(FLAGS_stream_blob_switch_retry_interval_us);
    }

    BYTE_ASSERT(writing_blob_ != nullptr || staled_ || closed_);
    LOG_INFO("Switch new blob")
        .put("Uri", stream_base_->Uri())
        .put("Closed", closed_)
        .put("Staled", staled_)
        .put("WritingBlob", writing_blob_.get())
        .put("Task", task)
        .put("Staled", staled_);

    AppendInternal(task);
}

void StreamImpl::CleanObsoleteBlobs(
    const google::protobuf::RepeatedPtrField<storage::BlobInfo>& blobs,
    const google::protobuf::RepeatedPtrField<storage::BlobInfo>& obsolete_blobs,
    google::protobuf::RepeatedPtrField<storage::BlobInfo>* new_blobs,
    google::protobuf::RepeatedPtrField<storage::BlobInfo>* new_obsolete_blobs) {
    uint64_t now_ms = GetCurrentTimeInMs();
    int i = 0;
    for (; i < blobs.size(); ++i) {
        LOG_DEBUG("Data blob info")
            .put("Uri", stream_base_->Uri())
            .put("BlobInfo", blobs[i].ShortDebugString());
        if (blobs[i].freeze_ms() == 0) {
            break;
        }
        if (blobs[i].freeze_ms() + FLAGS_stream_blob_deletion_min_age * 1000 > now_ms) {
            break;
        }
        uint64_t end_offset =
            blobs[i].start_offset() + blobs[i].blob_end_offset() - blobs[i].blob_start_offset();
        if (end_offset + FLAGS_stream_blob_deletion_min_gap > persistent_truncated_offset_) {
            break;
        }
        // The purpose of keeping 2 blocks is to make it easier to iterate, because when starting
        // the iteration, the program needs to read the footer of the previous block.
        if (end_offset + 2 * kBlockSize > persistent_truncated_offset_) {
            break;
        }
        *new_obsolete_blobs->Add() = blobs[i];
        LOG_INFO("Try move blob to obsolete blobs")
            .put("Uri", stream_base_->Uri())
            .put("BlobInfo", blobs[i].ShortDebugString())
            .put("MinAge", FLAGS_stream_blob_deletion_min_age)
            .put("MinGap", FLAGS_stream_blob_deletion_min_gap)
            .put("TruncatedOffset", persistent_truncated_offset_)
            .put("Now", now_ms);
    }
    for (; i < blobs.size(); ++i) {
        LOG_DEBUG("Data blob info")
            .put("Uri", stream_base_->Uri())
            .put("BlobInfo", blobs[i].ShortDebugString());
        *new_blobs->Add() = blobs[i];
    }

    std::unique_ptr<Controller[]> ctrls(new Controller[obsolete_blobs.size()]);
    CoCountDownLatch count_latch(obsolete_blobs.size());
    for (int i = 0; i < obsolete_blobs.size(); ++i) {
        LOG_DEBUG("Obsolete blob info")
            .put("Uri", stream_base_->Uri())
            .put("BlobInfo", obsolete_blobs[i].ShortDebugString());
        BlobInfo info;
        info.type = BlobType::kDataBlob;
        info.blob_id = obsolete_blobs[i].blob_id();
        std::string blob_uri = stream_base_->Uri() + BlobInfoToName(info);

        byte::InvokeInCurrentThread(NewCoFuncClosure([this, blob_uri, &count_latch, &ctrls, i] {
            Store::DeleteOptions options;
            options.condition = condition_;
            stream_base_->GetStoreLayer()->Delete(&ctrls[i], blob_uri, options);
            LOG_INFO("Delete blob").put("Uri", blob_uri).put("Status", ctrls[i].status());
            if (!ctrls[i].status().ok()) {
                LOG_ERROR("Delete blob failed")
                    .put("Uri", blob_uri)
                    .put("Error", ctrls[i].status().ToString());
            }
            count_latch.CountDown();
        }));
    }

    count_latch.Wait();
    for (int i = 0; i < obsolete_blobs.size(); ++i) {
        if (!ctrls[i].status().ok() && !ctrls[i].status().IsStoreNotFound()) {
            *new_obsolete_blobs->Add() = obsolete_blobs[i];
        }
    }

    std::sort(new_obsolete_blobs->begin(), new_obsolete_blobs->end(),
              [](const storage::BlobInfo& lhs, const storage::BlobInfo& rhs) {
                  return lhs.blob_id() < rhs.blob_id();
              });
}

Status StreamImpl::SealAndNew() {
    // Seal
    Status status = SealBlob(stream_base_->BlobHeader().blob_name());
    if (!status.ok()) {
        LOG_WARNING("Seal blob failed")
            .put("Uri", stream_base_->Uri())
            .put("BlobName", stream_base_->BlobHeader().blob_name())
            .put("Error", status.ToString());
    }

    // New
    google::protobuf::RepeatedPtrField<storage::BlobInfo> blob_infos =
        stream_base_->BlobHeader().data_blobs();
    google::protobuf::RepeatedPtrField<storage::BlobInfo> obsolete_blobs =
        stream_base_->BlobHeader().obsolete_blobs();
    storage::BlobInfo sealed_blob = FillBlobInfo(stream_base_->BlobHeader(), true);
    sealed_blob.set_end_record_sequence(persistent_sequence_);
    sealed_blob.set_blob_end_offset(persistent_blob_offset_);
    sealed_blob.set_freeze_ms(GetCurrentTimeInMs());
    sealed_blob.set_end_offset(persistent_offset_);
    sealed_blob.set_truncated_offset(persistent_truncated_offset_);
    *blob_infos.Add() = std::move(sealed_blob);

    BlobTailInfo tail_info;
    tail_info.blob_id = stream_base_->BlobHeader().blob_id();
    tail_info.end_record_sequence = persistent_sequence_;
    tail_info.end_offset = persistent_offset_;
    tail_info.blob_end_offset = persistent_blob_offset_;
    tail_info.last_block_crc32c = persistent_block_crc32c_;
    tail_info.truncated_offset = persistent_truncated_offset_;

    return New(blob_infos, obsolete_blobs, tail_info);
}

Status StreamImpl::New(const google::protobuf::RepeatedPtrField<storage::BlobInfo>& blob_infos,
                       const google::protobuf::RepeatedPtrField<storage::BlobInfo>& obsolete_blobs,
                       const BlobTailInfo& tail_info) {
    google::protobuf::RepeatedPtrField<storage::BlobInfo> new_blob_infos;
    google::protobuf::RepeatedPtrField<storage::BlobInfo> new_obsolete_blobs;
    CleanObsoleteBlobs(blob_infos, obsolete_blobs, &new_blob_infos, &new_obsolete_blobs);
    storage::BlobHeader new_blob_header =
        NewBlobHeader(new_blob_infos, new_obsolete_blobs, tail_info);
    LOG_DEBUG("New blob header")
        .put("Uri", stream_base_->Uri())
        .put("BlobHeader", new_blob_header.ShortDebugString());

    BlobInfo tmp_blob;
    Status status = NewTmpBlob(new_blob_header, &tmp_blob);
    if (!status.ok()) {
        LOG_ERROR("New tmp blob failed")
            .put("Uri", stream_base_->Uri())
            .put("NewBlobHeader", new_blob_header.ShortDebugString())
            .put("Error", status.ToString());
        return status;
    }

    Blob* new_blob = nullptr;
    status = NewBlob(new_blob_header, tmp_blob, &new_blob);
    if (!status.ok()) {
        LOG_ERROR("New data blob failed")
            .put("Uri", stream_base_->Uri())
            .put("NewBlobHeader", new_blob_header.ShortDebugString())
            .put("TmpBlobName", tmp_blob.name)
            .put("Error", status.ToString());
        return status;
    }
    writing_blob_.reset(new_blob);
    inflight_blob_offset_ = persistent_blob_offset_ =
        CalcBlobHeaderSize(new_blob_header.header_size(), new_blob_header.start_offset());
    status = stream_base_->UpdateBlobHeader(new_blob_header);
    if (!status.ok()) {
        LOG_ERROR("Update blob header failed")
            .put("Uri", stream_base_->Uri())
            .put("NewBlobHeader", new_blob_header.ShortDebugString())
            .put("Error", status.ToString());
        return status;
    }

    LOG_INFO("New blob success")
        .put("Uri", stream_base_->Uri())
        .put("WritingBlob", writing_blob_.get())
        .put("StreamLength", stream_buffer_->Length())
        .put("StreamStart", stream_buffer_->Start())
        .put("IncomingSequence", incoming_sequence_)
        .put("InflightSequence", inflight_sequence_)
        .put("PersistentSequence", persistent_sequence_)
        .put("InflightOffset", inflight_offset_)
        .put("PersistentOffset", persistent_offset_)
        .put("InflightBlobOffset", inflight_blob_offset_)
        .put("PersistentBlobOffset", persistent_blob_offset_)
        .put("IncomingBlockCrc32c", incoming_block_crc32c_)
        .put("InflightBlockCrc32c", inflight_block_crc32c_)
        .put("PersistentBlockCrc32c", persistent_block_crc32c_)
        .put("IncomingTruncatedOffset", incoming_truncated_offset_)
        .put("InflightTruncatedOffset", inflight_truncated_offset_)
        .put("PersistentTruncatedOffset", persistent_truncated_offset_);

    return Status::OK();
}

Status StreamImpl::SealBlob(const std::string& blob_name) {
    std::string blob_uri = stream_base_->Uri() + blob_name;
    Controller ctrl;
    Store::FreezeOptions options;
    options.condition = condition_;  // metadata operation, so it must hold the lock
    stream_base_->GetStoreLayer()->Freeze(&ctrl, blob_uri, options);
    if (!ctrl.status().ok()) {
        LOG_ERROR("Freeze blob failed")
            .put("Uri", stream_base_->Uri())
            .put("Blob", blob_uri)
            .put("Error", ctrl.status().ToString());
        return ctrl.status();
    }
    LOG_INFO("Freeze blob success").put("Uri", stream_base_->Uri()).put("Blob", blob_uri);
    return Status::OK();
}

Status StreamImpl::TailScanBlob(const std::string& blob_name, storage::BlobHeader* blob_header,
                                BlobTailInfo* tail_info) {
    std::string blob_uri = stream_base_->Uri() + blob_name;
    Store::OpenOptions open_options;
    open_options.mode = Store::OpenMode::kRead;
    open_options.metrics_manager = metrics_manager_;
    Blob* blob = nullptr;
    Controller ctrl;
    stream_base_->GetStoreLayer()->Open(&ctrl, blob_uri, open_options, &blob);
    if (!ctrl.status().ok()) {
        LOG_ERROR("Open blob failed")
            .put("Uri", stream_base_->Uri())
            .put("Blob", blob_uri)
            .put("Error", ctrl.status().ToString());
        return ctrl.status();
    }
    BYTE_DEFER({
        blob->Close();
        delete blob;
    });
    ctrl.Reset();
    Store::BlobStat stat;
    // 1. get its size
    Store::StatOptions options;
    stream_base_->GetStoreLayer()->Stat(&ctrl, blob_uri, options, &stat);
    if (!ctrl.status().ok()) {
        LOG_ERROR("Bstat blob failed")
            .put("Uri", stream_base_->Uri())
            .put("Blob", blob_uri)
            .put("Error", ctrl.status().ToString());
        return ctrl.status();
    }
    // 2. read its header
    Status status = stream_base_->ReadBlobHeader(blob, stat.size, blob_header);
    if (!status.ok()) {
        LOG_ERROR("Read blob header failed")
            .put("Uri", stream_base_->Uri())
            .put("Blob", blob_uri)
            .put("Error", status.ToString());
        return status;
    }
    LOG_DEBUG("Read blob header success")
        .put("Uri", stream_base_->Uri())
        .put("Blob", blob_uri)
        .put("BlobHeader", blob_header->ShortDebugString());

    return TailScan(blob, *blob_header, stat.size, tail_info);
}

Status StreamImpl::TailScan(Blob* blob, const storage::BlobHeader& blob_header, size_t blob_size,
                            BlobTailInfo* tail_info) {
    uint64_t real_header_size =
        CalcBlobHeaderSize(blob_header.header_size(), blob_header.start_offset());
    size_t offset = real_header_size;
    bool need_block_footer = false;
    if (blob_size >= LowerAlign(real_header_size, kBlockSize) + kBlockSize) {
        offset = LowerAlign(blob_size, kBlockSize) - kBlockFooterSize;
        need_block_footer = true;
    }
    size_t size = blob_size - offset;

    char buffer[kBlockFooterSize + kBlockSize];
    Controller ctrl;
    SYNC_CALL(blob->Read, &ctrl, offset, buffer, size);
    if (!ctrl.status().ok()) {
        LOG_ERROR("Read blob failed")
            .put("Uri", stream_base_->Uri())
            .put("Blob", blob)
            .put("Offset", offset)
            .put("ExpectedSize", size)
            .put("Error", ctrl.status().ToString());
        return ctrl.status();
    }

    size_t buffer_start = 0;
    if (!need_block_footer) {
        tail_info->end_record_sequence = blob_header.start_record_sequence();
        tail_info->end_offset = blob_header.start_offset();
        tail_info->last_block_crc32c = blob_header.prev_block_crc32c();
        tail_info->truncated_offset = blob_header.truncated_offset();
        buffer_start = 0;
    } else {
        storage::BlockFooter block_footer;
        if (!GetBlockFooter(reinterpret_cast<const char*>(buffer), &block_footer)) {
            LOG_ERROR("Parse block footer failed")
                .put("Uri", stream_base_->Uri())
                .put("Blob", blob)
                .put("Offset", offset)
                .put("Size", size);
            return Status::Internal("Parse footer failed");
        }
        if (kBlockFooterSize + block_footer.last_record_left_size() > size) {
            LOG_ERROR("Data corrupted")
                .put("Uri", stream_base_->Uri())
                .put("Blob", blob)
                .put("Offset", offset)
                .put("Size", size)
                .put("BlockFooter", block_footer.ShortDebugString())
                .put("BlockFooterSize", kBlockFooterSize);
            BYTE_ASSERT_DEBUG(false);
        }
        buffer_start = kBlockFooterSize + block_footer.last_record_left_size();
        tail_info->end_offset =
            block_footer.block_number() * kBlockSize + kBlockSize - kBlockFooterSize + buffer_start;
        tail_info->end_record_sequence = block_footer.last_record_sequence();
        // if cross block, sequence + 1
        tail_info->end_record_sequence += block_footer.last_record_left_size() != 0;
        tail_info->last_block_crc32c = byte::CRCUtil::ComputeCRC32(
            0, buffer + kBlockFooterSize, block_footer.last_record_left_size());
        tail_info->truncated_offset = block_footer.truncated_offset();
    }
    while (buffer_start < size) {
        uint32_t record_length = 0;
        uint32_t record_crc32c = 0;
        uint32_t consumed_size = 0;
        if (!ReadRecordHeader(buffer + buffer_start, size - buffer_start, &record_length,
                              &record_crc32c, &consumed_size)) {
            break;
        }

        if (buffer_start + consumed_size + record_length > size) {
            break;
        }
        uint32_t real_crc32c =
            byte::CRCUtil::ComputeCRC32(0, buffer + buffer_start + consumed_size, record_length);
        if (real_crc32c != record_crc32c) {
            BYTE_ASSERT_DEBUG(false);
            LOG_ERROR("Record crc mismatch")
                .put("Uri", stream_base_->Uri())
                .put("RecordOffset", tail_info->end_offset)
                .put("RecordSequence", tail_info->end_record_sequence)
                .put("Length", record_length)
                .put("ExpectedCrc32c", record_crc32c)
                .put("RealCrc32c", real_crc32c);
            return Status::DataLoss("Record crc mismatch");
        }
        tail_info->end_offset += consumed_size + record_length;
        tail_info->end_record_sequence += 1;
        tail_info->last_block_crc32c = byte::CRCUtil::ComputeCRC32(
            tail_info->last_block_crc32c, buffer + buffer_start, consumed_size + record_length);
        buffer_start += consumed_size + record_length;
    }
    BYTE_ASSERT_DEBUG(tail_info->end_offset - blob_header.start_offset() ==
                      blob_size - real_header_size);
    tail_info->blob_end_offset =
        tail_info->end_offset - blob_header.start_offset() +
        CalcBlobHeaderSize(blob_header.header_size(), blob_header.start_offset());

    if (tail_info->end_offset % kBlockSize + kBlockFooterSize > kBlockSize) {
        BYTE_ASSERT_DEBUG(false);
        LOG_ERROR("Invalid blob data")
            .put("Uri", stream_base_->Uri())
            .put("BlobInfo", blob_header.ShortDebugString())
            .put("TailInfo", tail_info->ToString());
        return Status::DataLoss("Invalid blob data");
    }

    LOG_INFO("Tail scan success")
        .put("Uri", stream_base_->Uri())
        .put("Blob", blob)
        .put("BlobSize", size)
        .put("TailInfo", tail_info->ToString());
    return Status::OK();
}

storage::BlobHeader StreamImpl::NewBlobHeader(
    const google::protobuf::RepeatedPtrField<storage::BlobInfo>& blob_infos,
    const google::protobuf::RepeatedPtrField<storage::BlobInfo>& obsolete_blobs,
    const BlobTailInfo& tail_info) {
    storage::BlobHeader blob_header;
    blob_header.Clear();
    blob_header.set_magic(kMagic);
    blob_header.set_blob_id(tail_info.blob_id + 1);
    blob_header.set_blob_name(BlobInfoToName(BlobInfo(BlobType::kDataBlob, blob_header.blob_id())));
    blob_header.set_timestamp_ms(GetCurrentTimeInMs());
    blob_header.set_version(kVersion);
    blob_header.set_start_record_sequence(blob_infos.empty() ? 0UL : tail_info.end_record_sequence);
    blob_header.set_client_token(token_);
    blob_header.mutable_data_blobs()->MergeFrom(blob_infos);
    blob_header.mutable_obsolete_blobs()->CopyFrom(obsolete_blobs);
    blob_header.set_start_offset(blob_infos.empty() ? 0UL : tail_info.end_offset);
    blob_header.set_prev_block_crc32c(blob_infos.empty() ? 0U : tail_info.last_block_crc32c);
    blob_header.set_truncated_offset(tail_info.truncated_offset);
    blob_header.set_header_size(1U);  // place holder
    blob_header.set_header_size(blob_header.ByteSize());
    BYTE_ASSERT(blob_header.ByteSize() == static_cast<int>(blob_header.header_size()))
        << blob_header.ByteSize() << " " << blob_header.header_size();
    return blob_header;
}

Status StreamImpl::NewTmpBlob(const storage::BlobHeader& blob_header, BlobInfo* tmp_blob) {
    uint64_t blob_id = std::max(tmp_blob_timestamp_ + 1, GetCurrentTimeInMs());
    tmp_blob_timestamp_ = blob_id;
    *tmp_blob = BlobInfo(BlobType::kTmpBlob, blob_id);
    tmp_blob->name = BlobInfoToName(*tmp_blob);
    std::string uri = stream_base_->Uri() + tmp_blob->name;
    Store::OpenOptions options;
    options.mode = Store::OpenMode::kWrite;
    options.condition = condition_;
    options.rep_policy = rep_policy_;
    options.metrics_manager = metrics_manager_;
    Blob* blob = nullptr;
    Controller ctrl;
    stream_base_->GetStoreLayer()->Open(&ctrl, uri, options, &blob);
    if (!ctrl.status().ok()) {
        LOG_ERROR("Open blob failed")
            .put("Uri", stream_base_->Uri())
            .put("Blob", uri)
            .put("Error", ctrl.status().ToString());
        return ctrl.status();
    }
    BYTE_DEFER({
        blob->Close();
        delete blob;
    });
    Status status = WriteBlobHeader(blob, blob_header);
    if (!status.ok()) {
        LOG_ERROR("Write blob header failed")
            .put("Uri", stream_base_->Uri())
            .put("Blob", uri)
            .put("Error", status.ToString());
    }
    return status;
}

Status StreamImpl::WriteBlobHeader(Blob* blob, const storage::BlobHeader& blob_header) {
    size_t size = CalcBlobHeaderSize(blob_header.ByteSize(), blob_header.start_offset());
    std::unique_ptr<char[]> buffer(new char[size]);
    if (!blob_header.SerializeToArray(buffer.get() + sizeof(ProtoHeader), blob_header.ByteSize())) {
        LOG_ERROR("Serialize blob header failed").put("BlobHeader", blob_header.ShortDebugString());
        return Status::Unknown("");
    }
    ProtoHeader* proto_header = reinterpret_cast<ProtoHeader*>(buffer.get());
    proto_header->proto_size = blob_header.ByteSize();
    proto_header->proto_crc =
        byte::CRCUtil::ComputeCRC32(0, buffer.get() + sizeof(ProtoHeader), blob_header.ByteSize());

    memset(buffer.get() + sizeof(ProtoHeader) + blob_header.ByteSize(), 0,
           size - sizeof(ProtoHeader) - blob_header.ByteSize());
    BYTE_ASSERT(static_cast<int>(blob_header.header_size()) == blob_header.ByteSize());

    Controller ctrl;
    SYNC_CALL(blob->Append, &ctrl, buffer.get(), size);
    if (!ctrl.status().ok()) {
        LOG_ERROR("Write blob failed")
            .put("Uri", stream_base_->Uri())
            .put("Blob", blob)
            .put("Size", size)
            .put("Error", ctrl.status().ToString());
        return ctrl.status();
    }
    return Status::OK();
}

// Rename a tmp blob to data blob
Status StreamImpl::NewBlob(const storage::BlobHeader& blob_header, const BlobInfo& tmp_blob,
                           Blob** blob) {
    std::string tmp_blob_uri = stream_base_->Uri() + tmp_blob.name;
    std::string blob_uri = stream_base_->Uri() + blob_header.blob_name();
    Store::RenameOptions rename_options;
    rename_options.condition = condition_;
    Controller ctrl;
    stream_base_->GetStoreLayer()->Rename(&ctrl, tmp_blob_uri, blob_uri, rename_options);
    if (!ctrl.status().ok()) {
        LOG_ERROR("Rename blob failed")
            .put("Uri", stream_base_->Uri())
            .put("OldBlob", tmp_blob_uri)
            .put("NewBlob", blob_uri)
            .put("Error", ctrl.status().ToString());
        return ctrl.status();
    }

    Store::OpenOptions open_options;
    open_options.mode = Store::OpenMode::kWrite;
    open_options.condition = condition_;
    open_options.metrics_manager = metrics_manager_;
    open_options.rep_policy = rep_policy_;
    ctrl.Reset();
    stream_base_->GetStoreLayer()->Open(&ctrl, blob_uri, open_options, blob);
    if (!ctrl.status().ok()) {
        LOG_ERROR("Open blob failed")
            .put("Uri", stream_base_->Uri())
            .put("Blob", blob_uri)
            .put("Error", ctrl.status().ToString());
        return ctrl.status();
    }

    return Status::OK();
}

void StreamImpl::ReapMetrics() const {
    const storage::BlobHeader& header = stream_base_->BlobHeader();
    metrics_.blob_count->get()->Set(header.data_blobs().size() + 1);
    metrics_.obsolete_blob_count->get()->Set(header.obsolete_blobs().size());
    metrics_.usage_size->get()->Set(persistent_offset_ - persistent_truncated_offset_);
    metrics_.incoming_size->get()->Set(stream_buffer_->Length() - inflight_offset_);
    metrics_.inflight_size->get()->Set(inflight_offset_ - persistent_offset_);
    metrics_.buffer_size->get()->Set(stream_buffer_->Size());

    uint64_t start_offset = header.start_offset();
    if (!header.data_blobs().empty()) {
        start_offset = header.data_blobs().begin()->start_offset();
    }
    if (!header.obsolete_blobs().empty()) {
        start_offset = header.obsolete_blobs().begin()->start_offset();
    }
    metrics_.physical_size->get()->Set(persistent_offset_ - start_offset);
}

std::string StreamImpl::BlobTailInfo::ToString() const {
    return byte::StringPrint(
        "blob_id=%lu,end_record_sequence=%lu,end_offset=%lu,blob_end_offset=%lu,last_block_crc32c=%"
        "lu,"
        "truncated_offset=%lu",
        blob_id, end_record_sequence, end_offset, blob_end_offset, last_block_crc32c,
        truncated_offset);
}

}  // namespace stream
}  // namespace bcache2
