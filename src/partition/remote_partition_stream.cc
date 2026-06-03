// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/remote_partition_stream.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <utility>
#include <vector>

#include "byte/include/assert.h"
#include "byte/thread/async_thread.h"
#include "brpc/controller.h"
#include "butil/rand_util.h"
#include "common/coclosure.h"
#include "common/logging.h"
#include "common/scoped_invoker.h"
#include "partition/condition.h"
#include "partition/partition.h"

namespace bcache2 {
namespace partition {

namespace {

constexpr uint32_t kDefaultScanRecords = 1;
constexpr uint64_t kDefaultScanBytes = 64ULL * 1024 * 1024;
constexpr int64_t kRemoteStreamRpcTimeoutMs = 5000;

void OnRemoteRpcDone(CoSyncClosure* sync) { sync->Run(); }

Status RpcStatusToStatus(const RpcStatus& status) {
    return Status::FromRpcStatus(status);
}

}  // namespace

class RemotePartitionStream::RemoteIterator : public stream::Iterator {
 public:
    RemoteIterator(RemotePartitionStream* stream, uint64_t start_id, uint64_t end_id)
        : stream_(stream),
          next_start_id_(start_id),
          end_id_(end_id) {}

    Status Next() override {
        if (next_start_id_ > end_id_) {
            return Status::OutOfRange("End of remote stream iterator");
        }

        brpc::Controller ctrl;
        ctrl.set_timeout_ms(kRemoteStreamRpcTimeoutMs);
        Status channel_status = stream_->EnsureChannel();
        if (!channel_status.ok()) {
            return channel_status;
        }

        ServerService_Stub stub(stream_->channel_.get());
        ScanPartitionStreamRequest request;
        response_.Clear();

        request.mutable_opt()->set_trace_id(butil::fast_rand());
        request.set_partition_id(stream_->PrimaryPartitionId());
        request.set_stream_kind(stream_->options_.stream_kind);
        request.set_zone_id(stream_->options_.zone_id);
        request.set_start_offset(next_start_id_);
        request.set_end_offset(end_id_);
        request.set_max_records(kDefaultScanRecords);
        request.set_max_bytes(kDefaultScanBytes);

        CoSyncClosure sync;
        stub.ScanPartitionStream(&ctrl, &request, &response_,
                                 brpc::NewCallback(&OnRemoteRpcDone, &sync));
        sync.Wait();

        if (ctrl.Failed()) {
            LOG_WARNING("Remote stream scan RPC failed")
                .put("PartitionId", stream_->PrimaryPartitionId())
                .put("StreamKind", stream_->options_.stream_kind)
                .put("ZoneId", stream_->options_.zone_id)
                .put("StartOffset", next_start_id_)
                .put("ErrorText", ctrl.ErrorText());
            return Status::DeadlineExceeded(ctrl.ErrorText());
        }

        Status status = RpcStatusToStatus(response_.status());
        if (!status.ok()) {
            return status;
        }
        if (response_.records_size() == 0) {
            return Status::OutOfRange("No more remote stream records");
        }

        const PartitionStreamRecord& record = response_.records(0);
        record_id_ = record.offset();
        record_ = absl::string_view(record.data().data(), record.data().size());

        // The stream iterator seeks by byte offset. Advancing by one byte past
        // the previous record start is enough to skip the record just returned,
        // while avoiding duplicated knowledge of the stream's block framing.
        next_start_id_ = record_id_ + 1;
        return Status::OK();
    }

    uint64_t Id() const override { return record_id_; }
    absl::string_view Data() const override { return record_; }

 private:
    RemotePartitionStream* stream_ = nullptr;
    uint64_t next_start_id_ = 0;
    uint64_t end_id_ = 0;
    uint64_t record_id_ = 0;
    absl::string_view record_;
    ScanPartitionStreamResponse response_;

    DISALLOW_COPY_AND_ASSIGN(RemoteIterator);
};

RemotePartitionStream::RemotePartitionStream(const Options& options) : options_(options) {}

Status RemotePartitionStream::Load() {
    return EnsureChannel();
}

uint64_t RemotePartitionStream::PrimaryPartitionId() const {
    return options_.partition ? options_.partition->GetPrimaryPartitionId() : primary_partition_id_;
}

Status RemotePartitionStream::EnsureChannel() {
    if (options_.partition == nullptr) {
        return Status::InvalidArgument("Remote stream has no owning partition");
    }

    ConditionInfoObserver condition_info(options_.partition->GetCondition().data.data());
    const std::string remote_ip = condition_info.RemoteIpStr();
    const uint16_t remote_port = condition_info.RemotePort();
    const uint64_t primary_partition_id = options_.partition->GetPrimaryPartitionId();
    if (remote_ip.empty() || remote_port == 0 || primary_partition_id == 0) {
        return Status::InvalidArgument("Invalid remote stream primary endpoint");
    }

    if (channel_ != nullptr && remote_ip_ == remote_ip && remote_port_ == remote_port &&
        primary_partition_id_ == primary_partition_id) {
        return Status::OK();
    }

    std::unique_ptr<brpc::Channel> channel(new brpc::Channel());
    brpc::ChannelOptions opts;
    if (channel->Init(remote_ip.c_str(), remote_port, &opts) != 0) {
        LOG_ERROR("Failed to init remote stream channel")
            .put("RemoteIp", remote_ip)
            .put("RemotePort", remote_port)
            .put("PartitionId", primary_partition_id);
        return Status::InvalidArgument("Invalid remote stream endpoint");
    }

    channel_.swap(channel);
    remote_ip_ = remote_ip;
    remote_port_ = remote_port;
    primary_partition_id_ = primary_partition_id;
    return Status::OK();
}

void RemotePartitionStream::Append(Controller* ctrl, const void* data, size_t size, uint64_t* id,
                                   Closure<void>* callback) {
    ctrl->set_status(Status::PermissionDenied("RemotePartitionStream is read-only"));
    byte::InvokeInCurrentThread(callback);
}

void RemotePartitionStream::AppendV(std::vector<std::string> data, uint64_t* id) {
    BYTE_ASSERT(false) << "RemotePartitionStream is read-only";
}

void RemotePartitionStream::Append(std::string data, uint64_t* id) {
    BYTE_ASSERT(false) << "RemotePartitionStream is read-only";
}

void RemotePartitionStream::Commit(Controller* ctrl, Closure<void>* callback) {
    ctrl->set_status(Status::OK());
    byte::InvokeInCurrentThread(callback);
}

void RemotePartitionStream::Truncate(uint64_t id) {
    BYTE_ASSERT(false) << "RemotePartitionStream is read-only";
}

struct RemoteReadTask {
    Controller* ctrl = nullptr;
    void* data = nullptr;
    size_t size = 0;
    Closure<void>* callback = nullptr;
    brpc::Controller brpc_ctrl;
    ReadPartitionStreamRequest request;
    ReadPartitionStreamResponse response;
};

static void OnRemoteReadDone(RemoteReadTask* task) {
    std::unique_ptr<RemoteReadTask> task_guard(task);
    ScopedInvoker done(task->callback);

    if (task->brpc_ctrl.Failed()) {
        task->ctrl->set_status(Status::DeadlineExceeded(task->brpc_ctrl.ErrorText()));
        return;
    }

    Status status = RpcStatusToStatus(task->response.status());
    if (!status.ok()) {
        task->ctrl->set_status(status);
        return;
    }

    if (task->response.data().size() != task->size) {
        LOG_ERROR("Remote stream read returned unexpected size")
            .put("ExpectedSize", task->size)
            .put("ActualSize", task->response.data().size())
            .put("Request", task->request.ShortDebugString());
        task->ctrl->set_status(Status::DataLoss("Remote stream read size mismatch"));
        return;
    }

    if (task->size > 0) {
        std::memcpy(task->data, task->response.data().data(), task->size);
    }
    task->ctrl->set_status(Status::OK());
}

void RemotePartitionStream::Read(Controller* ctrl, uint64_t id, void* data, size_t size,
                                 Closure<void>* callback) {
    Status status = EnsureChannel();
    if (!status.ok()) {
        ctrl->set_status(status);
        byte::InvokeInCurrentThread(callback);
        return;
    }

    RemoteReadTask* task = new RemoteReadTask;
    task->ctrl = ctrl;
    task->data = data;
    task->size = size;
    task->callback = callback;
    task->request.mutable_opt()->set_trace_id(butil::fast_rand());
    task->request.set_partition_id(PrimaryPartitionId());
    task->request.set_stream_kind(options_.stream_kind);
    task->request.set_zone_id(options_.zone_id);
    task->request.set_offset(id);
    task->request.set_size(size);
    task->brpc_ctrl.set_timeout_ms(kRemoteStreamRpcTimeoutMs);

    ServerService_Stub stub(channel_.get());
    stub.ReadPartitionStream(&task->brpc_ctrl, &task->request, &task->response,
                             brpc::NewCallback(&OnRemoteReadDone, task));
}

stream::ScopedIterator RemotePartitionStream::NewIterator(size_t start_id, size_t end_id) {
    return stream::ScopedIterator(new RemoteIterator(this, start_id, end_id));
}

stream::Stats RemotePartitionStream::Stat() {
    stream::Stats stats;
    if (stream_info_.blob_infos_size() == 0) {
        return stats;
    }
    const auto& blob_info = *stream_info_.blob_infos().rbegin();
    stats.start_record_id = blob_info.truncated_offset();
    stats.usage_bytes = blob_info.end_offset() - blob_info.truncated_offset();
    stats.length = blob_info.end_offset();
    stats.persistent_length = stats.length;
    return stats;
}

void RemotePartitionStream::Close(Closure<void>* callback) {
    if (callback) {
        byte::InvokeInCurrentThread(callback);
    }
}

Status RemotePartitionStream::RestoreInfo(const StreamInfo& info) {
    stream_info_.CopyFrom(info);
    return Status::OK();
}

}  // namespace partition
}  // namespace bcache2
