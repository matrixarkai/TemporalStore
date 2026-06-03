// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/controller.h"
#include "protocol/info.pb.h"
#include "stream/log_based_stream_base.h"
#include "stream/metrics.h"
#include "stream/stream.h"

namespace bcache2 {
namespace stream {

class StreamReaderImpl : public Stream {
 public:
    StreamReaderImpl(LogBasedEnv* env, const std::string& uri, MetricsManager* metrics_manager)
        : stream_base_(new StreamBaseImpl(env, uri, metrics_manager)) {}
    virtual ~StreamReaderImpl() {}

    Status Load() override {
        // we can load the stream meta data through RestoreInfo, so do nothing here
        return Status::OK();
    }

    Stats Stat() override {
        Stats stats;
        const auto& blob_info = stream_base_->LastBlobInfo();
        stats.start_record_id = blob_info.truncated_offset();
        stats.usage_bytes = blob_info.end_offset() - blob_info.truncated_offset();
        stats.length = blob_info.end_offset();
        stats.persistent_length = stats.length;
        return stats;
    }

    void Read(Controller* ctrl, uint64_t id, void* data, size_t size,
              Closure<void>* callback) override {
        stream_base_->Read(ctrl, id, data, size, callback);
    }

    ScopedIterator NewIterator(size_t start_id, size_t end_id) override {
        return stream_base_->NewIterator(start_id, end_id);
    }

    void Commit(Controller* ctrl, Closure<void>* callback) override {
        ctrl->set_status(Status::OK());
        byte::InvokeInCurrentThread(callback);
    }

    void Close(Closure<void>* callback) override { byte::InvokeInCurrentThread(callback); }

    void Append(Controller* ctrl, const void* data, size_t size, uint64_t* id,
                Closure<void>* callback) override {
        BYTE_ASSERT(false);
    }
    void AppendV(std::vector<std::string> data, uint64_t* id) override { BYTE_ASSERT(false); }
    void Append(std::string, uint64_t* id) override { BYTE_ASSERT(false); }
    void Truncate(uint64_t id) override { BYTE_ASSERT(false); }

    StreamInfo GetInfo() override { return stream_base_->GetInfo(); }
    Status RestoreInfo(const StreamInfo& stream_info) override {
        return stream_base_->RestoreInfo(stream_info);
    }

    void ReapMetrics() const override {
        // TODO(wangtai.10): impl
    }

    void UpdateConfig(const StreamConfig& config) override {}

 private:
    std::unique_ptr<StreamBaseImpl> stream_base_;

    DISALLOW_COPY_AND_ASSIGN(StreamReaderImpl);
};

}  // namespace stream
}  // namespace bcache2
