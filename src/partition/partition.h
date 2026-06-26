// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/base/closure.h>
#include <byte/include/macros.h>
#include <gflags/gflags.h>
#include <arpa/inet.h>
#if defined(__linux__)
#include <linux/in6.h>
#else
#include <netinet/in.h>
#endif
#include <protocol/info.pb.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "butil/endpoint.h"
#include "common/controller.h"
#include "common/function_closure.h"
#include "common/metrics.h"
#include "common/status.h"
#include "common/sync_closure.h"
#include "common/token_bucket.h"
#include "model/ips/ips_table_manager.h"
#include "model/schema.h"
#include "partition/allocator_manager.h"
#include "partition/cmd_context.h"
#include "partition/compute/cmd.h"
#include "partition/compute/cmd_executor.h"
#include "partition/condition.h"
#include "partition/index/index.h"
#include "partition/quota_manager.h"
#include "partition/storage/op_logger.h"
#include "partition/storage/replicator.h"
#include "protocol/server.pb.h"
#include "stream/metrics.h"
#include "stream/stream.h"

DECLARE_bool(storage_async);
DECLARE_bool(partition_commit_oplog);
DECLARE_bool(start_storage_manager_when_loading);

namespace byte {
class AsyncThread;
}

namespace bcache2 {
namespace partition {

class Expirer;
class Evicter;
class SlotStore;
class SlotContextManager;
class ObjectManager;
class StorageManager;
class PageStore;
class PageGc;
class PageCompactor;
class DataRaftConsensusBackend;
struct DataRaftStatus;

class Partition {
 public:
    struct Options {
        stream::Env* env = nullptr;
        std::string uri;
        std::string table_name;
        uint64_t partition_id = 0;
        uint32_t load_version = 0;  // TODO(wuzhenyu) 有的地方是u64
        std::string host;
        std::string host_v6;
        uint16_t port = 0;
        Config config;
        PersistentType persistent_type = PersistentType::PERSISTENT_ASYNC;
        bool default_start_storage_mananger = true;
        bcache2::blockcache::BlockCache* blockcache = nullptr;
        bool readonly = false;
        MembershipInfo membership;
        byte::AsyncThread* owning_thread = nullptr;
    };

    explicit Partition(const Options& options);
    virtual ~Partition();

    // MUST in co-context
    Status Load();
    Status Unload();

    Status ExecuteCheck() const {
        if (UNLIKELY(!IsLoaded())) {
            return Status::FailedPrecondition("Partition not loaded");
        }
        if (!replicator_->GetStatus().ok()) {
            return Status::Aborted("Replicate failed");
        }
        return Status::OK();
    }

    void ExecuteCmd(Controller* ctrl, const CmdRequest* request, CmdResponse* response,
                    Closure<void>* callback);

    void ExecuteCmd(Controller* ctrl, uint16_t module_id, uint16_t function_id,
                    const google::protobuf::Message* request, google::protobuf::Message* response,
                    Status* response_status, Closure<void>* callback);

    bool Readonly() const { return readonly_; }
    bool IsApplyingDataRaftEntry() const { return data_raft_applying_; }
    bool IsLoaded() const { return stage_ == PartitionLoadStage::LOADED; }
    bool IsLoading() const { return stage_ == PartitionLoadStage::LOADING; }
    bool IsUnloading() const { return stage_ == PartitionLoadStage::UNLOADING; }
    PartitionLoadStage GetStage() const { return stage_; }
    uint64_t GetLoadVersion() const { return options_.load_version; }
    uint64_t GetPartitionID() const { return options_.partition_id; }

    Status SetConfig(const Config& config);
    const Config& GetConfig() const;
    PartitionInfo GetInfo() const;
    Status OpenPartitionStream(const std::string& uri, PartitionStreamKind stream_kind,
                               uint32_t zone_id, bool created, stream::Stream** stream);
    void ReadPartitionStream(Controller* ctrl, const ReadPartitionStreamRequest* request,
                             ReadPartitionStreamResponse* response, Closure<void>* callback);
    void ScanPartitionStream(Controller* ctrl, const ScanPartitionStreamRequest* request,
                             ScanPartitionStreamResponse* response, Closure<void>* callback);
    Status ApplyDataRaftLog(uint64_t raft_index, const std::string& committed_log,
                            uint64_t* applied_raft_index, uint64_t* applied_oplog_sequence);
    Status ApplyDataRaftCommand(uint64_t raft_index, const std::string& committed_command);
    Status ApplyDataRaftEntry(uint64_t raft_index, const std::string& committed_entry,
                              uint64_t* applied_raft_index, uint64_t* applied_oplog_sequence);
    Status ProposeDataRaftCommand(const BatchExecuteCmdRequest& request, uint64_t request_id,
                                  uint64_t* committed_index, BatchExecuteCmdResponse* response);
    Status DataRaftReadIndex(uint64_t timeout_ms);
    Status CanServeDataRaftBoundedStaleRead(uint64_t max_stale_index_lag) const;
    Status GetDataRaftStatus(DataRaftStatus* status) const;
    Status TriggerDataRaftSnapshot(uint64_t* snapshot_index);
    Status CreateDataRaftSnapshot(const std::string& path, uint64_t* applied_index);
    Status LoadDataRaftSnapshot(const std::string& path);

    uint64_t GetPrimaryPartitionId() const { return primary_partition_id_; }
    uint64_t GetDataRaftGroupPartitionId() const { return data_raft_group_partition_id_; }
    void InitMembership();
    Status UpdateMembership(const MembershipInfo& info);
    Status GetStats(PartitionStats* stats);

    Status UpdateCondition();
    const stream::Env::Condition& GetCondition() const { return condition_; }

 private:
    Status SetupCondition();
    Status SetupRemoteInfo(PartitionInfo* remote_info);
    Status SetupIndex(const PartitionInfo& remote_info);
    Status SetupPageStore(const PartitionInfo& remote_info);
    Status SetupOplogger(const PartitionInfo& remote_info);
    Status SetupObjectManager();
    Status SetupStorageManager();
    Status SetupReplicator();
    Status LoadStream(const std::string& uri, PartitionStreamKind stream_kind, uint32_t zone_id,
                      std::unique_ptr<stream::Stream>* stream_ptr);
    Status ApplyDataRaftEntryOnOwnerThread(uint64_t raft_index,
                                           const std::string& committed_entry,
                                           uint64_t* applied_raft_index,
                                           uint64_t* applied_oplog_sequence);
    Status RebuildLocalStateAfterDataRaftSnapshot();
    std::string DataRaftAppliedIndexPath() const;
    Status LoadDataRaftAppliedIndex();
    Status PersistDataRaftAppliedIndex(uint64_t raft_index);
    void OnExecuteCmdDone(CmdContext* ctx, Closure<void>* callback);

    Options options_;
    stream::Env::Condition condition_;

    // Shared by all partitions
    bcache2::blockcache::BlockCache* blockcache_ = nullptr;

    bool readonly_ = false;
    PartitionLoadStage stage_ = PartitionLoadStage::INIT;
    uint64_t inflight_io_count_ = 0;
    PartitionInfo remote_info_;

    uint64_t primary_partition_id_{0};
    uint64_t data_raft_group_partition_id_{0};
    uint32_t partition_unit_id_{0};
    uint64_t partition_unit_version_{0};
    MembershipInfo membership_info_;

    std::unique_ptr<MetricsManager> metrics_manager_;
    std::unique_ptr<AllocatorManager> allocator_manager_;
    std::unique_ptr<Schema> schema_;
    std::unique_ptr<ips::IpsTableSchemaManager> ips_table_schema_manager_;
    std::unique_ptr<SlotContextManager> slot_context_manager_;
    std::unique_ptr<RequestMetrics> cmd_metrics_;
    std::unique_ptr<Index> index_;
    std::unique_ptr<OpLogger> op_logger_;
    std::unique_ptr<PageStore> page_store_;
    std::unique_ptr<SlotStore> slot_store_;
    std::unique_ptr<ObjectManager> object_manager_;
    std::unique_ptr<Evicter> evicter_;
    std::unique_ptr<PageGc> page_gc_;
    std::unique_ptr<PageCompactor> page_compactor_;
    std::unique_ptr<Expirer> expirer_;
    std::unique_ptr<CmdExecutor> cmd_executor_;
    std::unique_ptr<StorageManager> storage_manager_;
    std::unique_ptr<Replicator> replicator_;
    std::unique_ptr<DataRaftConsensusBackend> data_raft_consensus_;
    std::unique_ptr<CmdExecutorManager> cmd_;

    struct PendingDataRaftApply {
        bool done = false;
        Status status = Status::OK();
        BatchExecuteCmdResponse response;
        std::condition_variable cv;
    };
    std::mutex data_raft_pending_mu_;
    std::unordered_map<uint64_t, std::shared_ptr<PendingDataRaftApply>> data_raft_pending_;
    std::mutex data_raft_campaign_mu_;
    std::condition_variable data_raft_campaign_cv_;
    bool data_raft_campaign_inflight_ = false;
    std::mutex data_raft_snapshot_mu_;
    std::mutex data_raft_membership_mu_;
    std::atomic<uint64_t> data_raft_applied_index_{0};
    bool data_raft_applying_ = false;

    std::unique_ptr<MetricsEnv::CounterHolder> load_success_;
    std::unique_ptr<MetricsEnv::CounterHolder> load_failed_;
    std::unique_ptr<MetricsEnv::HistogramHolder> load_latency_;
    std::unique_ptr<MetricsEnv::CounterHolder> unload_success_;
    std::unique_ptr<MetricsEnv::HistogramHolder> unload_latency_;

    DISALLOW_COPY_AND_ASSIGN(Partition);
};

inline void Partition::ExecuteCmd(Controller* ctrl, const CmdRequest* request,
                                  CmdResponse* response, Closure<void>* callback) {
    inflight_io_count_++;
    CmdContext* ctx = new CmdContext(options_.partition_id, 0, 0, ctrl, op_logger_.get(),
                                     cmd_metrics_.get(), request, response, nullptr);
    cmd_->ExecuteCmd(ctx, request, response,
                     NewClosure(this, &Partition::OnExecuteCmdDone, ctx, callback));
}

inline void Partition::ExecuteCmd(Controller* ctrl, uint16_t module_id, uint16_t function_id,
                                  const google::protobuf::Message* request,
                                  google::protobuf::Message* response, Status* response_status,
                                  Closure<void>* callback) {
    inflight_io_count_++;
    CmdContext* ctx =
        new CmdContext(options_.partition_id, module_id, function_id, ctrl, op_logger_.get(),
                       cmd_metrics_.get(), request, response, response_status);
    cmd_executor_->Execute(module_id, function_id, request, response, ctx,
                           NewClosure(this, &Partition::OnExecuteCmdDone, ctx, callback));
}

inline void Partition::OnExecuteCmdDone(CmdContext* ctx, Closure<void>* callback) {
    BYTE_ASSERT(inflight_io_count_ > 0) << inflight_io_count_;

    if (UNLIKELY(data_raft_consensus_ != nullptr || IsApplyingDataRaftEntry())) {
        delete ctx;
        callback->Run();
        inflight_io_count_--;
        return;
    }

    const bool request_forces_sync =
        ctx->ctrl->event_replication_mode() == EVENT_REPLICATION_SYNC_STORAGE;
    const bool request_forces_async =
        ctx->ctrl->event_replication_mode() == EVENT_REPLICATION_ASYNC_STORAGE;
    if (LIKELY(!request_forces_sync &&
               (request_forces_async ||
                (options_.persistent_type == PersistentType::PERSISTENT_ASYNC &&
                 FLAGS_storage_async)))) {
        delete ctx;
        if (FLAGS_partition_commit_oplog) {
            // set FLAGS_partition_commit_oplog to false only in test now
            op_logger_->Commit(nullptr, nullptr);
        }
        callback->Run();
        inflight_io_count_--;
        return;
    }

    Controller* commit_ctrl = new Controller();
    auto func = [this, commit_ctrl, ctx, callback] {
        ctx->time_tracer.AddEvent("Commit");
        if (!commit_ctrl->status().ok()) {
            ctx->ctrl->set_status(commit_ctrl->status());
        }
        delete commit_ctrl;
        delete ctx;
        callback->Run();
        inflight_io_count_--;
    };
    op_logger_->Commit(commit_ctrl, NewFuncClosure(func));
}

}  // namespace partition
}  // namespace bcache2
