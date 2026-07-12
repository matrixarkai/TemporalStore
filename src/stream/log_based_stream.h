// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/strings/string_view.h>
#include <byte/container/intrusive_list.h>
#include <byte/include/macros.h>
#include <byte/thread/async_thread.h>

#include <algorithm>
#include <map>
#include <mutex>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/controller.h"
#include "common/metrics.h"
#include "common/ring_array.h"
#include "common/stream_buffer.h"
#include "protocol/info.pb.h"
#include "protocol/storage.pb.h"
#include "stream/log_based_stream_base.h"
#include "stream/metrics.h"
#include "stream/stream.h"

namespace bcache2 {
namespace stream {

class StoreLayer;
class Blob;
class LogBasedEnv;

class StreamImpl : public Stream {
 public:
    StreamImpl(LogBasedEnv* env, const std::string& uri, const Env::Condition& condition,
               StoreRepPolicy rep_policy, const std::string& token,
               MetricsManager* metrics_manager);
    virtual ~StreamImpl();

    Status Load() override;
    void Close(Closure<void>* callback) override;

    void Append(Controller* ctrl, const void* data, size_t size, uint64_t* id,
                Closure<void>* callback) override;
    void Append(std::string, uint64_t* id) override;
    void AppendV(std::vector<std::string> data, uint64_t* id) override;
    void Commit(Controller* ctrl, Closure<void>* callback) override;
    void Truncate(uint64_t id) override {
        std::lock_guard<std::recursive_mutex> lock(stream_mu_);
        incoming_truncated_offset_ =
            std::min(std::max(id, incoming_truncated_offset_), stream_buffer_->Length());
    }
    Stats Stat() override {
        std::lock_guard<std::recursive_mutex> lock(stream_mu_);
        Stats stats;
        stats.start_record_id = incoming_truncated_offset_;
        stats.usage_bytes = persistent_offset_ - persistent_truncated_offset_;
        stats.length = stream_buffer_->Length();
        stats.persistent_length = persistent_offset_;
        return stats;
    }

    void Read(Controller* ctrl, uint64_t id, void* data, size_t size,
              Closure<void>* callback) override {
        stream_base_->Read(ctrl, id, data, size, callback);
    }

    ScopedIterator NewIterator(size_t start_id, size_t end_id) override {
        return stream_base_->NewIterator(start_id, end_id);
    }

    StreamInfo GetInfo() override { return stream_base_->GetInfo(); }
    Status RestoreInfo(const StreamInfo& stream_info) override {
        BYTE_ASSERT(false) << "not supported";
        return Status::OK();
    }

    void UpdateConfig(const StreamConfig& config) override {
        rep_policy_.MergeFrom(config.store_rep_policy());
    }

    void ReapMetrics() const override;

 private:
    struct CommitTask {
        Controller* ctrl = nullptr;
        Closure<void>* callback = nullptr;
        size_t offset = 0;
        TimeCost cost;
    };

    struct Task {
        Controller ctrl;
        uint64_t offset = 0;
        uint64_t blob_offset = 0;
        std::string data;

        bool inplace = false;
        matrixobjectstore_message message;
    };

    struct BlobTailInfo {
        uint64_t blob_id = 0;
        uint64_t end_record_sequence = 0;
        uint64_t end_offset = 0;
        uint64_t blob_end_offset = 0;
        uint64_t last_block_crc32c = 0;
        uint64_t truncated_offset = 0;

        std::string ToString() const;
    };

    struct Delimiter {
        uint64_t sequence = 0;
        uint64_t block_crc = 0;
        uint64_t truncated_offset = 0;

        Delimiter() {}
        Delimiter(uint64_t sequence, uint64_t block_crc, uint64_t truncated_offset)
            : sequence(sequence), block_crc(block_crc), truncated_offset(truncated_offset) {}
    };

    void LoopFlush();
    void AppendToBuffer(std::vector<std::string> datas, uint64_t* id);
    void AppendToBufferSlowPath(std::vector<std::string> datas, uint64_t* id);
    void TryAppend(bool aggregate_flush);
    void AppendInternal(Task* task);
    void OnAppendDone(Task* task);
    void ScheduleSwitchNewBlobToAppend(Task* task);
    void SwitchNewBlobToAppend(Task* task);
    void CleanObsoleteBlobs(
        const google::protobuf::RepeatedPtrField<storage::BlobInfo>& blobs,
        const google::protobuf::RepeatedPtrField<storage::BlobInfo>& obsolete_blobs,
        google::protobuf::RepeatedPtrField<storage::BlobInfo>* new_blobs,
        google::protobuf::RepeatedPtrField<storage::BlobInfo>* new_obsolete_blobs);
    Status SealAndNew();

    Status SealBlob(const std::string& blob_name);
    Status TailScanBlob(const std::string& blob_name, storage::BlobHeader* blob_header,
                        BlobTailInfo* tail_info);
    Status TailScan(Blob* blob, const storage::BlobHeader& blob_header, size_t blob_size,
                    BlobTailInfo* tail_info);
    Status New(const google::protobuf::RepeatedPtrField<storage::BlobInfo>& blob_infos,
               const google::protobuf::RepeatedPtrField<storage::BlobInfo>& obsolete_blobs,
               const BlobTailInfo& tail_info);
    storage::BlobHeader NewBlobHeader(
        const google::protobuf::RepeatedPtrField<storage::BlobInfo>& blob_infos,
        const google::protobuf::RepeatedPtrField<storage::BlobInfo>& obsolete_blobs,
        const BlobTailInfo& tail_info);
    Status NewTmpBlob(const storage::BlobHeader& blob_header, BlobInfo* tmp_blob);
    Status WriteBlobHeader(Blob* blob, const storage::BlobHeader& blob_header);
    Status NewBlob(const storage::BlobHeader& blob_header, const BlobInfo& tmp_blob, Blob** blob);

    std::unique_ptr<StreamBaseImpl> stream_base_;
    Env::Condition condition_;
    StoreRepPolicy rep_policy_;
    std::string token_;

    MetricsManager* metrics_manager_ = nullptr;

    StreamMetrics metrics_;
    std::recursive_mutex stream_mu_;
    std::unique_ptr<StreamBuffer<Delimiter>> stream_buffer_;
    RingArray<CommitTask> commit_tasks_{0};

    std::unique_ptr<Blob> writing_blob_ = nullptr;

    uint64_t incoming_sequence_ = 0;
    uint32_t incoming_block_crc32c_ = 0;
    uint64_t incoming_truncated_offset_ = 0;

    uint64_t inflight_sequence_ = 0;
    uint64_t inflight_block_crc32c_ = 0;
    uint64_t inflight_truncated_offset_ = 0;
    uint64_t inflight_offset_ = 0;
    uint64_t inflight_blob_offset_ = 0;

    uint64_t persistent_sequence_ = 0;
    uint32_t persistent_block_crc32c_ = 0;
    uint64_t persistent_truncated_offset_ = 0;
    uint64_t persistent_offset_ = 0;
    uint64_t persistent_blob_offset_ = 0;

    uint64_t tmp_blob_timestamp_ = 0;

    bool closed_ = false;
    Closure<void>* close_callback_ = nullptr;
    bool staled_ = false;

    Closure<void>* stop_loop_flush_sync_ = nullptr;

    DISALLOW_COPY_AND_ASSIGN(StreamImpl);
};

}  // namespace stream
}  // namespace bcache2
