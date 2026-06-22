// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/storage/replicator.h"

#include <utility>

#include "common/coclosure.h"
#include "common/time_tracer.h"
#include "partition/index/index.h"
#include "partition/partition.h"
#include "partition/storage/object_manager.h"
#include "partition/storage/op_logger.h"
#include "partition/storage/page_store.h"

DECLARE_uint64(replicator_loop_interval_us);
DECLARE_uint64(replicator_max_oplog_per_loop);
DECLARE_uint64(replicator_max_indexlog_per_loop);
DECLARE_uint64(replicator_out_of_sync_s);
DECLARE_uint64(replicator_update_remote_interval_ms);

namespace bcache2 {
namespace partition {

static void OnBrpcDone(CoSyncClosure* sync) { sync->Run(); }

Replicator::Replicator(Partition* partition, Index* index, OpLogger* op_logger,
                       PageStore* page_store, ObjectManager* object_manager,
                       MetricsManager* metrics_manager)
    : partition_(partition),
      index_(index),
      op_logger_(op_logger),
      page_store_(page_store),
      object_manager_(object_manager) {
    metrics_.Init(metrics_manager);
}

void Replicator::Start() {
    LOG_CALL_INFO().put("PartitionId", partition_->GetPartitionID()).put("Stopped", stopped_);
    if (!stopped_) {
        return;
    }
    stopped_ = false;
    byte::InvokeInCurrentThread(NewCoClosure(this, &Replicator::LoopWorker));
    LOG_INFO("Replicator started").put("PartitionId", partition_->GetPartitionID());
}

void Replicator::Stop() {
    LOG_CALL_INFO().put("PartitionId", partition_->GetPartitionID()).put("Stopped", stopped_);
    if (stopped_) {
        return;
    }
    stopped_ = true;
    stop_sync_.reset(new CoSyncClosure());
    stop_sync_->Wait();
    LOG_INFO("Replicator stopped").put("PartitionId", partition_->GetPartitionID());
}

void Replicator::LoopWorker() {
    auto init_loop = [this] {
        if (loop_initialized_) {
            return;
        }
        UpdateRemoteChannel();
        index_log_iter_ = index_->NewIndexLogIterator(index_->LogLength());
        op_logger_iter_ = op_logger_->NewIterator(op_logger_->Length());
        loop_initialized_ = true;
    };

    if (UNLIKELY(!IsCoContext())) {
        LOG_WARNING("Replicator running outside coroutine context")
            .put("PartitionId", partition_->GetPartitionID());
        init_loop();
        if (stop_sync_.get() == nullptr && status_.ok()) {
            MainLoop();
        }
        if (stopped_) {
            if (stop_sync_ != nullptr) {
                stop_sync_->Run();
            }
            stopped_ = true;
            return;
        }
        if (stop_sync_.get() == nullptr && status_.ok()) {
            uint64_t delay_us = LastLoopMadeProgress() ? 0 : FLAGS_replicator_loop_interval_us;
            byte::InvokeLaterInCurrentThread(delay_us, NewCoClosure(this, &Replicator::LoopWorker));
        }
        return;
    }

    init_loop();

    while (stop_sync_.get() == nullptr && status_.ok()) {
        MainLoop();
        if (!LastLoopMadeProgress()) {
            CoSleep(FLAGS_replicator_loop_interval_us);
        }
    }

    if (stopped_) {
        BYTE_ASSERT(stop_sync_ != nullptr);
        stop_sync_->Run();
    }
    stopped_ = true;
}

void Replicator::MainLoop() {
    TimeTracer tracer;
    const uint64_t replayed_oplog_before = replayed_oplog_num_;
    const uint64_t replayed_index_log_before = replayed_index_log_num_;
    last_loop_made_progress_ = false;

    // check out of sync
    if (last_replay_time_ms_ > 0) {
        uint64_t now = GetCurrentTimeInMs();
        uint64_t lag_ms = now >= last_replay_time_ms_ ? now - last_replay_time_ms_ : 0;
        if (UNLIKELY(lag_ms > FLAGS_replicator_out_of_sync_s * 1000)) {
            if (ShouldLogOutOfSync(now)) {
                LOG_WARNING("Replica replay idle beyond out-of-sync threshold; continue catching up")
                    .put("PartitionId", partition_->GetPartitionID())
                    .put("Now", now)
                    .put("LagMs", lag_ms)
                    .put("ReplayedOplogNum", replayed_oplog_num_)
                    .put("ReplayedIndexLogNum", replayed_index_log_num_);
            }
        }
    }

    // update page store zones
    Status status = page_store_->UpdateZones();
    if (!status.ok()) {
        LOG_WARNING("Failed to update zones")
            .put("PartitionId", partition_->GetPartitionID())
            .put("Status", status);
        return;
    }

    // update remote info if needed
    if (need_update_remote_ || GetCurrentTimeInMs() - last_update_remote_ms_ >
                                   FLAGS_replicator_update_remote_interval_ms) {
        status = UpdateRemoteInfo();
        if (!status.ok()) {
            LOG_WARNING("Failed update remote info")
                .put("PartitionId", partition_->GetPartitionID())
                .put("Status", status);
            UpdateRemoteChannel();
            return;
        }
        tracer.AddEvent("UpdateRemoteInfo");
    }

    // replay oplog
    status = ReplayOpLog(FLAGS_replicator_max_oplog_per_loop);
    if (!status.ok()) {
        LOG_WARNING("Failed replay oplog")
            .put("PartitionId", partition_->GetPartitionID())
            .put("Status", status);
        return;
    }
    tracer.AddEvent("ReplayOpLog");

    // replay index log
    status = ReplayIndexLog(FLAGS_replicator_max_indexlog_per_loop);
    if (!status.ok() && !status.IsFailedPrecondition()) {
        LOG_WARNING("Failed replay index log")
            .put("PartitionId", partition_->GetPartitionID())
            .put("Status", status);
        return;
    }
    tracer.AddEvent("ReplayIndexLog");

    LOG_DEBUG("Replicator Loop")
        .put("PartitionId", partition_->GetPartitionID())
        .put("Tracer", tracer.ToString());
    LOG_INFO_SAMPLE("Replicator Loop")
        .put("PartitionId", partition_->GetPartitionID())
        .put("Tracer", tracer.ToString());
    last_loop_made_progress_ = replayed_oplog_num_ != replayed_oplog_before ||
                               replayed_index_log_num_ != replayed_index_log_before;
    metrics_.loop_qps->get()->Increment();
    metrics_.loop_latency->get()->Set(tracer.TotalSpentUs());
}

Status Replicator::UpdateRemoteInfo() {
    if (UNLIKELY(!IsCoContext())) {
        LOG_WARNING("UpdateRemoteInfo running outside coroutine context")
            .put("PartitionId", partition_->GetPartitionID());
    }
    if (UNLIKELY(remote_channel_ == nullptr)) {
        return Status::FailedPrecondition("Empty channel");
    }

    // TODO(wangtai.10): custom timeout
    brpc::Controller ctrl;
    ServerService_Stub stub(remote_channel_.get());
    GetInfoRequest request;
    GetInfoResponse response;
    request.mutable_opt()->set_trace_id(butil::fast_rand());
    request.set_partition_id(partition_->GetPrimaryPartitionId());

    CoSyncClosure sync;
    stub.GetInfo(&ctrl, &request, &response, brpc::NewCallback(&OnBrpcDone, &sync));
    sync.Wait();

    if (ctrl.Failed()) {
        LOG_WARNING("Failed to send rpc call")
            .put("PartitionId", partition_->GetPartitionID())
            .put("ErrorText", ctrl.ErrorText());
        return Status::DeadlineExceeded(ctrl.ErrorText());
    }

    if (response.status().code() != kOK) {
        LOG_WARNING("Failed to get remote partition info")
            .put("PartitionId", partition_->GetPartitionID())
            .put("Status", response.status().message());
        return Status::FromRpcStatus(response.status());
    }

    if (response.partition_info().stage() != PartitionLoadStage::LOADED) {
        return Status::FailedPrecondition("remote partition is loading");
    }

    const PartitionInfo& remote_info = response.partition_info();
    uint64_t index_log_gap =
        remote_info.index_info().current_sequence() - index_->CurrentSequence();
    uint64_t oplog_gap =
        remote_info.op_logger_info().current_sequence() - op_logger_->CurrentSequence();
    metrics_.index_log_gap->get()->Set(index_log_gap);
    metrics_.oplog_gap->get()->Set(oplog_gap);
    LOG_DEBUG("Get remote info success")
        .put("PartitionId", partition_->GetPartitionID())
        .put("IndexLogGap", index_log_gap)
        .put("OplogGap", oplog_gap)
        .put("RemoteInfo", remote_info.ShortDebugString());
    LOG_INFO_SAMPLE("Get remote info success")
        .put("PartitionId", partition_->GetPartitionID())
        .put("IndexLogGap", index_log_gap)
        .put("OplogGap", oplog_gap)
        .put("RemoteInfo", remote_info.ShortDebugString());

    Status status = page_store_->RestoreInfo(remote_info.page_store_info());
    if (!status.ok()) {
        LOG_WARNING("Failed to restore page store info")
            .put("PartitionId", partition_->GetPartitionID())
            .put("PageStoreInfo", remote_info.page_store_info().ShortDebugString())
            .put("Status", status);
        status_ = Status::Aborted("Failed to restore page store info" + status.ToString());
        return status_;
    }

    status = index_->RestoreInfo(remote_info.index_info());
    if (!status.ok()) {
        LOG_WARNING("Failed to restore index info")
            .put("PartitionId", partition_->GetPartitionID())
            .put("Status", status);
        status_ = Status::Aborted("Failed to restore index info" + status.ToString());
        return status_;
    }

    status = op_logger_->RestoreInfo(remote_info.op_logger_info());
    if (!status.ok()) {
        LOG_WARNING("Failed to restore oplogger info")
            .put("PartitionId", partition_->GetPartitionID())
            .put("Status", status);
        status_ = Status::Aborted("Failed to restore oplogger info" + status.ToString());
        return status_;
    }

    need_update_remote_ = false;
    last_update_remote_ms_ = GetCurrentTimeInMs();
    return Status::OK();
}

Status Replicator::UpdateRemoteChannel() {
    Status status = partition_->UpdateCondition();
    if (!status.ok()) {
        LOG_WARNING("Failed to update partition condition")
            .put("PartitionId", partition_->GetPartitionID())
            .put("Status", status);
        return status;
    }

    ConditionInfoObserver condition_info(partition_->GetCondition().data.data());

    std::unique_ptr<brpc::Channel> remote_channel(new brpc::Channel());
    brpc::ChannelOptions opts;
    if (remote_channel->Init(condition_info.RemoteIpStr().c_str(), condition_info.RemotePort(),
                             &opts) != 0) {
        LOG_ERROR("Invalid remote addr")
            .put("PartitionId", partition_->GetPartitionID())
            .put("ConditionInfo", condition_info.ToString());
        return Status::InvalidArgument("Invalid remote addr");
    }

    LOG_INFO("Update remote channel")
        .put("PartitionId", partition_->GetPartitionID())
        .put("ConditionInfo", condition_info.ToString());
    remote_channel_.swap(remote_channel);
    return Status::OK();
}

bool Replicator::ShouldLogOutOfSync(uint64_t now_ms) {
    constexpr uint64_t kMinOutOfSyncLogIntervalMs = 10000;
    if (last_out_of_sync_log_ms_ != 0 &&
        now_ms - last_out_of_sync_log_ms_ < kMinOutOfSyncLogIntervalMs) {
        return false;
    }
    last_out_of_sync_log_ms_ = now_ms;
    return true;
}

Status Replicator::ReplayOpLog(uint64_t max_log_per_loop) {
    for (size_t i = 0; i < max_log_per_loop; ++i) {
        Status status = op_logger_iter_->Next();
        if (status.IsOutOfRange()) {
            LOG_DEBUG("No more oplog")
                .put("PartitionId", partition_->GetPartitionID())
                .put("LogId", op_logger_iter_->GetLogId());
            need_update_remote_ = true;
            metrics_.oplog_lag_ms->get()->Set(0);
            last_replay_time_ms_ = GetCurrentTimeInMs();
            return Status::OK();
        }
        if (!status.ok()) {
            LOG_WARNING("Failed fetch next oplog")
                .put("PartitionId", partition_->GetPartitionID())
                .put("Status", status);
            return Status::FailedPrecondition("Failed fetch next oplog");
        }

        const uint64_t log_id = op_logger_iter_->GetLogId();
        const uint32_t log_size = op_logger_iter_->GetSize();
        const storage::OpLog& log = op_logger_iter_->GetLog();

        uint64_t now = GetCurrentTimeInMs();
        if (LIKELY(log.item_size() > 0)) {
            uint64_t lag_ms =
                now >= log.item(0).timestamp_ms() ? now - log.item(0).timestamp_ms() : 0;
            metrics_.oplog_lag_ms->get()->Set(lag_ms);
            if (UNLIKELY(lag_ms > FLAGS_replicator_out_of_sync_s * 1000)) {
                if (ShouldLogOutOfSync(now)) {
                    LOG_WARNING("Replica oplog lag exceeds out-of-sync threshold; continue replay")
                        .put("PartitionId", partition_->GetPartitionID())
                        .put("Now", now)
                        .put("LagMs", lag_ms)
                        .put("ReplayedOplogNum", replayed_oplog_num_)
                        .put("ReplayedIndexLogNum", replayed_index_log_num_)
                        .put("Log", log.ShortDebugString());
                }
            }
        }

        status = object_manager_->ReplayOplog(log_id, log_size, log);
        if (!status.ok()) {
            LOG_WARNING("Failed to replay oplog")
                .put("PartitionId", partition_->GetPartitionID())
                .put("LogId", log_id)
                .put("LogSize", log_size)
                .put("Status", status)
                .put("Log", log.ShortDebugString());
            status_ = Status::Aborted("Failed to replay oplog: " + status.ToString());
            return Status::Aborted("Failed to replay oplog");
        }

        ++replayed_oplog_num_;
        last_replay_time_ms_ = now;
        metrics_.replay_oplog_qps->get()->Increment();
    }
    return Status::OK();
}

Status Replicator::ReplayIndexLog(uint64_t max_log_per_loop) {
    for (size_t i = 0; i < max_log_per_loop; ++i) {
        if (LIKELY(!index_log_staged_)) {
            Status status = index_log_iter_->Next();
            if (status.IsOutOfRange()) {
                LOG_DEBUG("No more index log")
                    .put("PartitionId", partition_->GetPartitionID())
                    .put("LogId", op_logger_iter_->GetLogId());
                metrics_.index_log_lag_ms->get()->Set(0);
                return Status::OK();
            }
            if (!status.ok()) {
                LOG_WARNING("Failed fetch next index log")
                    .put("PartitionId", partition_->GetPartitionID())
                    .put("Status", status);
                return Status::FailedPrecondition("Failed fetch next index log");
            }
        }

        const uint64_t log_id = index_log_iter_->GetLogId();
        const size_t log_size = index_log_iter_->GetLogSize();
        const storage::IndexLog& log = index_log_iter_->GetLog();

        if (LIKELY(log.timestamp_ms()) > 0) {
            uint64_t now = GetCurrentTimeInMs();
            uint64_t lag_ms = now >= log.timestamp_ms() ? now - log.timestamp_ms() : 0;
            metrics_.index_log_lag_ms->get()->Set(lag_ms);
            if (UNLIKELY(lag_ms > FLAGS_replicator_out_of_sync_s * 1000)) {
                if (ShouldLogOutOfSync(now)) {
                    LOG_WARNING("Replica index-log lag exceeds out-of-sync threshold; continue replay")
                        .put("PartitionId", partition_->GetPartitionID())
                        .put("Now", now)
                        .put("LagMs", lag_ms)
                        .put("ReplayedOplogNum", replayed_oplog_num_)
                        .put("ReplayedIndexLogNum", replayed_index_log_num_)
                        .put("Log", log.ShortDebugString());
                }
            }
        }

        if (log.oplog_sequence() > op_logger_->CurrentSequence()) {
            // wait oplog replay to keep the relative order between oplog and index log
            LOG_DEBUG("index log is ahead of oplog, just wait")
                .put("PartitionId", partition_->GetPartitionID())
                .put("slotId", log.slot_id())
                .put("index log oplog sequence", log.oplog_sequence())
                .put("current oplog sequence", op_logger_->CurrentSequence());
            index_log_staged_ = true;
            return Status::OK();
        }

        Status status = index_->ReplayIndexLog(log, log_size);
        if (status.IsFailedPrecondition()) {
            // index log is not applied
            index_log_staged_ = true;
            return status;
        }

        index_log_staged_ = false;
        if (status.IsZoneChanged()) {
            need_update_remote_ = true;
            return status;
        }
        if (!status.ok()) {
            LOG_WARNING("Failed to replay index log")
                .put("PartitionId", partition_->GetPartitionID())
                .put("LogId", log_id)
                .put("Status", status)
                .put("Log", log.ShortDebugString());
            status_ = Status::Aborted("Failed to replay index log");
            return status_;
        }

        ++replayed_index_log_num_;
        metrics_.replay_index_log_qps->get()->Increment();
    }
    return Status::OK();
}

}  // namespace partition
}  // namespace bcache2
