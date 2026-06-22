// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "server/partition_manager.h"

#include <byte/base/closure.h>
#include <byte/concurrent/count_down_latch.h>
#include <byte/thread/async_thread.h>
#include <gflags/gflags.h>

#include <cstdio>
#include <memory>
#include <string>
#include <utility>

#include "common/bits.h"
#include "common/cmd_manager.h"
#include "common/coclosure.h"
#include "common/function_closure.h"
#include "common/scoped_invoker.h"
#include "partition/partition.h"
#include "partition/storage/data_raft_consensus.h"
#include "protocol/metaserver.pb.h"
#include "server/server.h"
#include "server/util.h"

DECLARE_string(data_replication_mode);
DECLARE_string(data_raft_work_dir);

DEFINE_bool(data_raft_enable_experimental_direct_writes, false,
            "Allow direct write execution while data_replication_mode=raft_consensus. "
            "This is only for Byteraft backend bring-up tests. Production must keep this false "
            "until writes are proposed through Raft before local mutation.");
DEFINE_string(data_raft_read_mode, "leader",
              "Read policy when data_replication_mode=raft_consensus. Supported values: "
              "leader, linearizable, bounded_stale, unsafe_any_replica. leader rejects "
              "secondary reads; linearizable performs ReadIndex on the leader; bounded_stale "
              "allows secondary reads only when applied lag is within "
              "data_raft_bounded_stale_max_index_lag; unsafe_any_replica is for bring-up tests.");
DEFINE_uint64(data_raft_bounded_stale_max_index_lag, 0,
              "Maximum committed-applied Raft index lag allowed for bounded_stale replica reads.");
DEFINE_uint64(data_raft_read_index_timeout_ms, 1000,
              "ReadIndex timeout for linearizable reads in data-node Raft mode.");

namespace bcache2 {
namespace server {

__thread PartitionManager::ThreadLocalInfo* PartitionManager::thread_info_ = nullptr;

namespace {

bool IsLegacyCmdWrite(const CmdRequest& request) {
    switch (request.module_case()) {
        case CmdRequest::kCommonRequest:
            return request.common_request().cmd_case() != CommonModuleRequest::kTtlRequest;
        case CmdRequest::kHashRequest:
            return request.hash_request().cmd_case() != hash::HashModuleRequest::kGetRequest;
        case CmdRequest::kFeatureRequest:
            return request.feature_request().cmd_case() !=
                       feature::FeatureModuleRequest::kQueryRequest &&
                   request.feature_request().cmd_case() !=
                       feature::FeatureModuleRequest::kAggQueryRequest;
        case CmdRequest::kStringRequest:
            return request.string_request().cmd_case() != str::StringModuleRequest::kGetRequest;
        case CmdRequest::kSetRequest:
        case CmdRequest::MODULE_NOT_SET:
            return true;
    }
    return true;
}

bool IsCmdWrite(const CmdRequest& request) {
    if (request.module_id() == 0) {
        return IsLegacyCmdWrite(request);
    }

    const CmdManager::CmdInfo* cmd =
        CmdManager::GetCmd(request.module_id(), request.function_id());
    return cmd == nullptr || cmd->flag == CmdRwFlag::kWrite;
}

bool HasWriteCmd(const BatchExecuteCmdRequest& request) {
    for (int i = 0; i < request.request_size(); ++i) {
        if (IsCmdWrite(request.request(i))) {
            return true;
        }
    }
    return false;
}

bool IsWriteOnlyBatch(const BatchExecuteCmdRequest& request) {
    for (int i = 0; i < request.request_size(); ++i) {
        if (!IsCmdWrite(request.request(i))) {
            return false;
        }
    }
    return true;
}

bool IsPrimaryPartition(const partition::Partition* partition) {
    return partition->GetPartitionID() == partition->GetPrimaryPartitionId();
}

bool IsDataRaftConsensusMode() {
    return ::FLAGS_data_replication_mode == "raft_consensus";
}

std::string ResolvePartitionUriForDataRaft(uint64_t partition_id, const std::string& logical_uri) {
    if (!IsDataRaftConsensusMode()) {
        return logical_uri;
    }
    return "file://" + FLAGS_data_raft_work_dir + "/local-streams/" +
           std::to_string(partition_id) + "/partition";
}

Status CheckDataRaftReadPolicy(partition::Partition* partition) {
    if (!IsDataRaftConsensusMode()) {
        return Status::OK();
    }
    if (partition == nullptr) {
        return Status::FailedPrecondition("missing partition");
    }

    const bool is_primary = IsPrimaryPartition(partition);
    if (FLAGS_data_raft_read_mode == "leader") {
        return is_primary ? Status::OK()
                          : Status::FailedPrecondition(
                                "data raft secondary read rejected by leader read mode");
    }
    if (FLAGS_data_raft_read_mode == "linearizable") {
        if (!is_primary) {
            return Status::FailedPrecondition(
                "data raft linearizable read must be served by the leader");
        }
        return partition->DataRaftReadIndex(FLAGS_data_raft_read_index_timeout_ms);
    }
    if (FLAGS_data_raft_read_mode == "bounded_stale") {
        if (is_primary) {
            return Status::OK();
        }
        return partition->CanServeDataRaftBoundedStaleRead(
            FLAGS_data_raft_bounded_stale_max_index_lag);
    }
    if (FLAGS_data_raft_read_mode == "unsafe_any_replica") {
        return Status::OK();
    }
    return Status::InvalidArgument("invalid data_raft_read_mode");
}

}  // namespace

PartitionManager::PartitionManager(const std::string& cluster_name, Server* server,
                                   byte::AsyncThreadPool* thread_pool,
                                   byte::AsyncThreadPool* raft_propose_pool, stream::Env* env,
                                   blockcache::BlockCache* blockcache)
    : cluster_name_(cluster_name),
      server_(server),
      thread_pool_(thread_pool),
      raft_propose_pool_(raft_propose_pool),
      env_(env),
      blockcache_(blockcache) {
    thread_infos_.reset(new ThreadLocalInfo[thread_pool_->ThreadNum()]);
    for (int i = 0; i < thread_pool_->ThreadNum(); ++i) {
        thread_pool_->KthThread(i)->Invoke(
            NewFuncClosure([this, i] { thread_info_ = &thread_infos_[i]; }));
    }
}

PartitionManager::~PartitionManager() {}

void PartitionManager::Load(Controller* ctrl, const LoadRequest* request, LoadResponse* response,
                            Closure<void>* callback) {
    if (thread_info_ == nullptr) {
        GetThread(request->partition_id())
            ->Invoke(
                NewCoClosure(this, &PartitionManager::Load, ctrl, request, response, callback));
        return;
    }

    ScopedInvoker done(callback);

    uint64_t partition_id = request->partition_id();
    auto it = thread_info_->partition_map.find(partition_id);
    if (it != thread_info_->partition_map.end()) {
        ctrl->set_status(Status::AlreadyExists("Partition already exists"));
        return;
    }

    partition::Partition::Options options;
    options.env = env_;
    options.uri = ResolvePartitionUriForDataRaft(partition_id, request->partition_uri());
    options.table_name = request->table_name();
    options.host = server_->GetHost();
    options.host_v6 = server_->GetHostV6();
    options.port = server_->GetListenPort();
    options.partition_id = partition_id;
    options.owning_thread = GetThread(partition_id);
    options.load_version = request->load_version();
    options.config.MergeFrom(request->config());
    options.persistent_type = request->persistent_type();
    options.blockcache = blockcache_;
    options.readonly = request->readonly();
    options.membership = request->membership();
    partition::Partition* partition = new partition::Partition(options);
    thread_info_->partition_map[partition_id].reset(partition);
    if (!request->sync()) {
        LoadAsync(partition);
        ctrl->set_status(Status::OK());
        return;
    }

    Status status = partition->Load();
    if (!status.ok()) {
        LOG_WARNING("Failed to load partition")
            .put("Status", status)
            .put("PartitionId", partition_id);
        ctrl->set_status(status);
        return;
    }

    ctrl->set_status(Status::OK());
}

static void OnRpcFinished(CoSyncClosure* sync) { sync->Run(); }

void PartitionManager::ReportLoadResult(uint64_t partition_id, Status result) {
    // TODO(wuzhenyu) graceful quit if server is stopped
    const int max_try_count = 10;
    int try_count = 0;
    while (try_count <= max_try_count) {
        if (try_count++ > 0) {
            CoSleep(10 * 1000 * 1000);
        }

        LOG_INFO("try to report load result to metaserver")
            .put("try_count", try_count)
            .put("partition_id", partition_id)
            .put("load_result", result);
        butil::EndPoint leader_endpoint;
        Status status = server_->GetMetaServerTracker()->GetLeaderEndpoint(&leader_endpoint);
        if (!status.ok()) {
            LOG_WARNING("failed to get leader").put("result", status);
            continue;
        }
        brpc::Channel channel;
        brpc::ChannelOptions opts;
        opts.connect_timeout_ms = -1;
        channel.Init(leader_endpoint, &opts);
        metaserver::ManageService_Stub stub(&channel);

        brpc::Controller cntl;
        uint64_t log_id = butil::fast_rand();
        cntl.set_log_id(log_id);
        cntl.set_timeout_ms(10'000);

        metaserver::LoadPartitionFinishRequest request;
        AckResponse response;
        InitRequestId(cluster_name_, request.mutable_id());
        request.set_partition_id(partition_id);
        *request.mutable_load_result() = result.ToRpcStatus();

        CoSyncClosure sync;
        stub.FinishLoadPartition(&cntl, &request, &response,
                                 brpc::NewCallback(OnRpcFinished, &sync));
        sync.Wait();
        if (cntl.Failed()) {
            LOG_WARNING("report load finish failed")
                .put("partition_id", partition_id)
                .put("load_result", result)

                .put("log_id", log_id)
                .put("remote", leader_endpoint)
                .put("err", cntl.ErrorText());
            continue;
        }
        Status result_status = Status::FromRpcStatus(response.status());
        if (!result_status.ok()) {
            LOG_WARNING("report load finish got error response")
                .put("partition_id", partition_id)
                .put("load_result", result)
                .put("log_id", log_id)
                .put("remote", leader_endpoint)
                .put("response", result_status);
            if (result_status.IsFailedPrecondition()) {
                // Note: From MetaServer side: LoadFinish maybe arrived before CreateFinish
                continue;
            }
        }
        return;
    }
    LOG_WARNING("failed to report load result to metaserver");
}

void PartitionManager::LoadAsync(partition::Partition* partition) {
    auto func = [partition, this]() {
        const uint64_t partition_id = partition->GetPartitionID();
        Status status = partition->Load();
        if (!status.ok()) {
            LOG_WARNING("Failed to load partition")
                .put("Status", status)
                .put("PartitionId", partition_id);
        }
        byte::AsyncThreadPool* report_pool =
            raft_propose_pool_ != nullptr ? raft_propose_pool_ : thread_pool_;
        report_pool->PushTask(partition_id, NewFuncClosure([this, partition_id, status] {
            ReportLoadResult(partition_id, status);
        }));
    };
    GetThread(partition->GetPartitionID())->Invoke(NewCoFuncClosure(func));
}

void PartitionManager::Unload(Controller* ctrl, const UnloadRequest* request,
                              UnloadResponse* response, Closure<void>* callback) {
    if (thread_info_ == nullptr) {
        GetThread(request->partition_id())
            ->Invoke(
                NewCoClosure(this, &PartitionManager::Unload, ctrl, request, response, callback));
        return;
    }

    ScopedInvoker done(callback);

    const uint64_t partition_id = request->partition_id();
    auto it = thread_info_->partition_map.find(partition_id);
    if (it == thread_info_->partition_map.end()) {
        ctrl->set_status(Status::NotFound("Partition not found"));
        return;
    }

    partition::Partition* partition_ptr = it->second.get();
    Status status = partition_ptr->Unload();
    if (!status.ok()) {
        ctrl->set_status(status);
        return;
    }

    it = thread_info_->partition_map.find(partition_id);
    if (it == thread_info_->partition_map.end()) {
        return;
    }

    partition_ptr = it->second.get();
    BYTE_ASSERT(partition_ptr->GetStage() == PartitionLoadStage::UNLOADED);
    thread_info_->partition_map.erase(it);
    ctrl->set_status(Status::OK());
}

void PartitionManager::BatchExecuteCmd(Controller* ctrl, const BatchExecuteCmdRequest* request,
                                       BatchExecuteCmdResponse* response, Closure<void>* callback) {
    BatchExecuteContext* context = NewBatchExecuteContext(ctrl, request, response, callback);
    const uint64_t partition_id = request->partition_id();
    byte::AsyncThreadPool* execute_pool =
        raft_propose_pool_ != nullptr ? raft_propose_pool_ : thread_pool_;
    execute_pool->PushTask(partition_id, NewCoFuncClosure([this, context, partition_id] {
        ThreadLocalInfo* previous_thread_info = thread_info_;
        thread_info_ = &thread_infos_[partition_id % thread_pool_->ThreadNum()];
        BatchExecuteCmdInternal(context);
        thread_info_ = previous_thread_info;
    }));
}

void PartitionManager::BatchExecuteCmdLocally(Controller* ctrl,
                                              const BatchExecuteCmdRequest* request,
                                              BatchExecuteCmdResponse* response,
                                              Closure<void>* callback) {
    BatchExecuteContext* context = NewBatchExecuteContext(ctrl, request, response, callback);
    auto execute = [this, context, partition_id = request->partition_id()] {
        ThreadLocalInfo* previous_thread_info = thread_info_;
        thread_info_ = &thread_infos_[partition_id % thread_pool_->ThreadNum()];
        BatchExecuteCmdInternal(context);
        thread_info_ = previous_thread_info;
    };
    if (IsCoContext()) {
        execute();
        return;
    }
    NewCoFuncClosure(std::move(execute))->Run();
}

PartitionManager::BatchExecuteContext* PartitionManager::NewBatchExecuteContext(
        Controller* ctrl, const BatchExecuteCmdRequest* request, BatchExecuteCmdResponse* response,
        Closure<void>* callback) {
    BatchExecuteContext* context = new BatchExecuteContext;
    context->ctrl = ctrl;
    context->request = request;
    context->response = response;
    context->callback = callback;
    context->ctrls.reset(new Controller[request->request_size()]);
    context->statuses.reset(new Status[request->request_size()]);
    context->requests.resize(request->request_size());
    context->responses.resize(request->request_size());
    for (int i = 0; i < request->request_size(); ++i) {
        context->ctrls[i].set_trace_id(ctrl->trace_id());
        const CmdRequest& cmd_request = request->request(i);
        if (cmd_request.module_id() == 0) {  // For compatibility, request from old clients
            continue;
        }

        const CmdManager::CmdInfo* cmd =
            CmdManager::GetCmd(cmd_request.module_id(), cmd_request.function_id());
        if (cmd == nullptr) {
            LOG_ERROR("Cmd not found")
                .put("ModuleId", cmd_request.module_id())
                .put("FunctionId", cmd_request.function_id())
                .put("TraceId", ctrl->trace_id());
            context->ctrls[i].set_status(Status::Unimplemented("Cmd not implemented"));
            continue;
        }
        context->requests[i].reset(cmd->request_builder());
        context->responses[i].reset(cmd->response_builder());
        if (!context->requests[i]->ParseFromString(cmd_request.request_bytes())) {
            LOG_ERROR("Request parse failed")
                .put("CmdRequest", cmd_request.ShortDebugString())
                .put("TraceId", ctrl->trace_id());
            context->ctrls[i].set_status(Status::InvalidArgument("Parse request failed"));
            continue;
        }
    }
    return context;
}

void PartitionManager::BatchExecuteCmdInternal(BatchExecuteContext* context) {
    const bool has_write_cmd = HasWriteCmd(*context->request);
    LOG_CALL_DEBUG()
        .put("PartitionId", context->request->partition_id())
        .put("LoadVersion", context->request->load_version())
        .put("PinPrimary", context->request->pin_primary())
        .put("HasWriteCmd", has_write_cmd)
        .put("TraceId", context->ctrl->trace_id());

    ScopedInvoker done(context->callback);
    auto it = thread_info_->partition_map.find(context->request->partition_id());
    const bool need_primary = context->request->pin_primary() || has_write_cmd;
    if (it == thread_info_->partition_map.end() ||
        (need_primary && !IsPrimaryPartition(it->second.get()))) {
        // client should refresh table topo
        context->ctrl->set_status(Status::TopomError("Partition not exists or not primary"));
        return;
    }

    if (has_write_cmd && IsDataRaftConsensusMode() &&
        !FLAGS_data_raft_enable_experimental_direct_writes) {
        LOG_INFO("Data raft write batch received")
            .put("PartitionId", context->request->partition_id())
            .put("RequestSize", context->request->request_size())
            .put("PinPrimary", context->request->pin_primary())
            .put("TraceId", context->ctrl->trace_id());
        if (!IsWriteOnlyBatch(*context->request)) {
            context->ctrl->set_status(Status::FailedPrecondition(
                "data_replication_mode=raft_consensus currently accepts write-only batches. "
                "Mixed read/write batches are rejected until read-index and committed response "
                "plumbing are complete."));
            return;
        }
        uint64_t request_id = context->ctrl->trace_id();
        if (request_id == 0) {
            request_id = butil::fast_rand();
        }
        partition::Partition* partition = it->second.get();
        done.Release();
        byte::AsyncThreadPool* propose_pool =
            raft_propose_pool_ != nullptr ? raft_propose_pool_ : thread_pool_;
        propose_pool->PushTask(context->request->partition_id(),
                               NewFuncClosure([partition, context, request_id] {
            std::unique_ptr<BatchExecuteContext> context_guard(context);
            uint64_t committed_index = 0;
            Status raft_status =
                partition->ProposeDataRaftCommand(*context->request, request_id, &committed_index,
                                                  context->response);
            LOG_INFO("Data raft write batch finished")
                .put("PartitionId", context->request->partition_id())
                .put("RequestId", request_id)
                .put("CommittedIndex", committed_index)
                .put("Status", raft_status);
            context->ctrl->set_status(raft_status.ok() ? Status::OK() : raft_status);
            context->callback->Run();
        }));
        return;
    }

    if (!has_write_cmd) {
        Status read_policy_status = CheckDataRaftReadPolicy(it->second.get());
        if (!read_policy_status.ok()) {
            context->ctrl->set_status(read_policy_status);
            return;
        }
    }

    if (stopping_ &&
        IsBitSet(context->request->opt().supported_features(), RequestOption::CLIENT_FAST_RETRY)) {
        context->ctrl->set_status(Status::RetryLater("Server is stopping"));
        return;
    }

    Status status = it->second->ExecuteCheck();
    if (UNLIKELY(!status.ok())) {
        context->ctrl->set_status(status);
        return;
    }

    int request_size = context->request->request_size();

    for (int i = 0; i < request_size; ++i) {
        if (!context->ctrls[i].status().ok()) {
            ++context->complete_count;
        }
    }
    if (context->complete_count == context->request->request_size()) {
        std::unique_ptr<BatchExecuteContext> context_release_guard(context);
        for (int i = 0; i < context->request->request_size(); ++i) {
            CmdResponse* cmd_response = context->response->add_response();
            cmd_response->mutable_status()->CopyFrom(context->ctrls[i].status().ToRpcStatus());
        }
        context->callback->Run();
        return;
    }

    for (int i = 0; i < request_size; ++i) {
        if (!context->ctrls[i].status().ok()) {
            continue;
        } else if (context->request->request(i).module_id() == 0) {  // For compatibility
            it->second->ExecuteCmd(
                &context->ctrls[i], &context->request->request(i),
                context->response->add_response(),
                NewClosure(this, &PartitionManager::OnExecuteCmdDone, context, i));
            done.Release();
            continue;
        }
        it->second->ExecuteCmd(&context->ctrls[i], context->request->request(i).module_id(),
                               context->request->request(i).function_id(),
                               context->requests[i].get(), context->responses[i].get(),
                               &context->statuses[i],
                               NewClosure(this, &PartitionManager::OnExecuteCmdDone, context, i));
        done.Release();
    }
}

void PartitionManager::OnExecuteCmdDone(BatchExecuteContext* context, int index) {
    BYTE_ASSERT(index < context->request->request_size());
    BYTE_ASSERT(context->complete_count < context->request->request_size());
    ++context->complete_count;
    if (context->complete_count != context->request->request_size()) {
        return;
    }

    std::unique_ptr<BatchExecuteContext> context_release_guard(context);
    for (int i = 0; i < context->request->request_size(); ++i) {
        if (context->request->request(i).module_id() == 0) {  // For compatibility
            if (!context->ctrls[i].status().ok()) {
                context->response->mutable_response(i)->mutable_status()->CopyFrom(
                    context->ctrls[i].status().ToRpcStatus());
            }
            continue;
        }

        CmdResponse* cmd_response = context->response->add_response();
        cmd_response->mutable_status()->CopyFrom(context->ctrls[i].status().ToRpcStatus());
        if (!context->ctrls[i].status().ok()) {
            continue;
        }
        cmd_response->mutable_response_status()->CopyFrom(context->statuses[i].ToRpcStatus());
        if (!context->responses[i]->SerializeToString(cmd_response->mutable_response_bytes())) {
            cmd_response->mutable_status()->CopyFrom(
                Status::Internal("Serialize response failed").ToRpcStatus());
        }
    }
    context->callback->Run();
}

void PartitionManager::GetInfo(Controller* ctrl, const GetInfoRequest* request,
                               GetInfoResponse* response, Closure<void>* callback) {
    if (thread_info_ == nullptr) {
        GetThread(request->partition_id())
            ->Invoke(
                NewClosure(this, &PartitionManager::GetInfo, ctrl, request, response, callback));
        return;
    }

    ScopedInvoker done(callback);

    auto it = thread_info_->partition_map.find(request->partition_id());
    if (it == thread_info_->partition_map.end()) {
        ctrl->set_status(Status::NotFound("Partition not exists"));
        return;
    }

    *response->mutable_partition_info() = std::move(it->second->GetInfo());
}

void PartitionManager::ReadPartitionStream(Controller* ctrl,
                                           const ReadPartitionStreamRequest* request,
                                           ReadPartitionStreamResponse* response,
                                           Closure<void>* callback) {
    if (thread_info_ == nullptr) {
        GetThread(request->partition_id())
            ->Invoke(NewCoClosure(this, &PartitionManager::ReadPartitionStream, ctrl, request,
                                  response, callback));
        return;
    }

    auto it = thread_info_->partition_map.find(request->partition_id());
    if (it == thread_info_->partition_map.end()) {
        ScopedInvoker done(callback);
        ctrl->set_status(Status::NotFound("Partition not exists"));
        return;
    }

    it->second->ReadPartitionStream(ctrl, request, response, callback);
}

void PartitionManager::ScanPartitionStream(Controller* ctrl,
                                           const ScanPartitionStreamRequest* request,
                                           ScanPartitionStreamResponse* response,
                                           Closure<void>* callback) {
    if (thread_info_ == nullptr) {
        GetThread(request->partition_id())
            ->Invoke(NewCoClosure(this, &PartitionManager::ScanPartitionStream, ctrl, request,
                                  response, callback));
        return;
    }

    auto it = thread_info_->partition_map.find(request->partition_id());
    if (it == thread_info_->partition_map.end()) {
        ScopedInvoker done(callback);
        ctrl->set_status(Status::NotFound("Partition not exists"));
        return;
    }

    it->second->ScanPartitionStream(ctrl, request, response, callback);
}

void PartitionManager::ApplyDataRaftLog(Controller* ctrl,
                                        const ApplyDataRaftLogRequest* request,
                                        ApplyDataRaftLogResponse* response,
                                        Closure<void>* callback) {
    if (thread_info_ == nullptr) {
        GetThread(request->partition_id())
            ->Invoke(NewClosure(this, &PartitionManager::ApplyDataRaftLog, ctrl, request,
                                response, callback));
        return;
    }

    ScopedInvoker done(callback);
    auto it = thread_info_->partition_map.find(request->partition_id());
    if (it == thread_info_->partition_map.end()) {
        ctrl->set_status(Status::NotFound("Partition not exists"));
        return;
    }

    uint64_t applied_raft_index = 0;
    uint64_t applied_oplog_sequence = 0;
    Status status = it->second->ApplyDataRaftLog(request->raft_index(),
                                                request->committed_log(),
                                                &applied_raft_index,
                                                &applied_oplog_sequence);
    ctrl->set_status(status);
    if (status.ok()) {
        response->set_applied_raft_index(applied_raft_index);
        response->set_applied_oplog_sequence(applied_oplog_sequence);
    }
}

void PartitionManager::GetDataRaftStatus(
    Controller* ctrl, const GetDataRaftStatusRequest* request,
    GetDataRaftStatusResponse* response, Closure<void>* callback) {
    if (thread_info_ == nullptr) {
        GetThread(request->partition_id())
            ->Invoke(NewClosure(this, &PartitionManager::GetDataRaftStatus, ctrl, request,
                                response, callback));
        return;
    }

    ScopedInvoker done(callback);
    auto it = thread_info_->partition_map.find(request->partition_id());
    if (it == thread_info_->partition_map.end()) {
        ctrl->set_status(Status::NotFound("Partition not exists"));
        return;
    }

    partition::DataRaftStatus status;
    Status result = it->second->GetDataRaftStatus(&status);
    ctrl->set_status(result);
    if (!result.ok()) {
        return;
    }
    response->set_running(status.running);
    response->set_leader(status.leader);
    response->set_learner(status.learner);
    response->set_term(status.term);
    response->set_leader_replica_id(status.leader_replica_id);
    response->set_committed_index(status.committed_index);
    response->set_applied_index(status.applied_index);
    response->set_first_index(status.first_index);
    response->set_last_index(status.last_index);
    response->set_pending_config_change_index(status.pending_config_change_index);
    response->set_voter_count(status.voter_count);
    response->set_learner_count(status.learner_count);
    response->set_fatal_event_count(status.fatal_event_count);
    response->set_snapshot_creating(status.snapshot_creating);
    response->set_snapshot_loading(status.snapshot_loading);
}

void PartitionManager::TriggerDataRaftSnapshot(
    Controller* ctrl, const TriggerDataRaftSnapshotRequest* request,
    TriggerDataRaftSnapshotResponse* response, Closure<void>* callback) {
    if (thread_info_ == nullptr) {
        GetThread(request->partition_id())
            ->Invoke(NewClosure(this, &PartitionManager::TriggerDataRaftSnapshot, ctrl, request,
                                response, callback));
        return;
    }

    ScopedInvoker done(callback);
    auto it = thread_info_->partition_map.find(request->partition_id());
    if (it == thread_info_->partition_map.end()) {
        ctrl->set_status(Status::NotFound("Partition not exists"));
        return;
    }

    partition::Partition* partition = it->second.get();
    done.Release();
    byte::AsyncThreadPool* snapshot_pool =
        raft_propose_pool_ != nullptr ? raft_propose_pool_ : thread_pool_;
    snapshot_pool->PushTask(request->partition_id(),
                            NewFuncClosure([partition, ctrl, response, callback] {
        ScopedInvoker async_done(callback);
        uint64_t snapshot_index = 0;
        Status status = partition->TriggerDataRaftSnapshot(&snapshot_index);
        ctrl->set_status(status);
        if (status.ok()) {
            response->set_snapshot_index(snapshot_index);
        }
    }));
}

void PartitionManager::SetConfig(Controller* ctrl, const SetConfigRequest* request,
                                 SetConfigResponse* response, Closure<void>* callback) {
    if (thread_info_ == nullptr) {
        GetThread(request->partition_id())
            ->Invoke(
                NewClosure(this, &PartitionManager::SetConfig, ctrl, request, response, callback));
        return;
    }

    ScopedInvoker done(callback);
    auto it = thread_info_->partition_map.find(request->partition_id());
    if (it == thread_info_->partition_map.end()) {
        ctrl->set_status(Status::NotFound("Partition not exists"));
        return;
    }

    Status status = it->second->SetConfig(request->config());
    ctrl->set_status(status);
}

void PartitionManager::GetConfig(Controller* ctrl, const GetConfigRequest* request,
                                 GetConfigResponse* response, Closure<void>* callback) {
    if (thread_info_ == nullptr) {
        GetThread(request->partition_id())
            ->Invoke(
                NewClosure(this, &PartitionManager::GetConfig, ctrl, request, response, callback));
        return;
    }

    ScopedInvoker done(callback);
    auto it = thread_info_->partition_map.find(request->partition_id());
    if (it == thread_info_->partition_map.end()) {
        ctrl->set_status(Status::NotFound("Partition not exists"));
        return;
    }

    response->mutable_config()->CopyFrom(it->second->GetConfig());
    response->mutable_status()->set_code(Code::kOK);
}

byte::AsyncThread* PartitionManager::GetThread(uint64_t partition_id) {
    return thread_pool_->KthThread(partition_id % thread_pool_->ThreadNum());
}

void PartitionManager::UpdateMembership(Controller* ctrl, const UpdateMembershipRequest* request,
                                        AckResponse* response, Closure<void>* callback) {
    if (thread_info_ == nullptr) {
        GetThread(request->partition_id())
            ->Invoke(NewCoClosure(this, &PartitionManager::UpdateMembership, ctrl, request,
                                  response, callback));
        return;
    }

    ScopedInvoker done(callback);
    auto it = thread_info_->partition_map.find(request->partition_id());
    if (it == thread_info_->partition_map.end()) {
        ctrl->set_status(Status::NotFound("Partition not exists"));
        return;
    }

    *response->mutable_status() =
        (it->second->UpdateMembership(request->membership())).ToRpcStatus();
    return;
}

void PartitionManager::GetStats(Controller* ctrl, const GetStatsRequest* request,
                                GetStatsResponse* response, Closure<void>* callback) {
    if (thread_info_ == nullptr) {
        GetThread(request->partition_id())
            ->Invoke(
                NewClosure(this, &PartitionManager::GetStats, ctrl, request, response, callback));
        return;
    }

    ScopedInvoker done(callback);
    auto it = thread_info_->partition_map.find(request->partition_id());
    if (it == thread_info_->partition_map.end()) {
        ctrl->set_status(Status::NotFound("Partition not exists"));
        return;
    }

    Status status = it->second->GetStats(response->mutable_stats());
    *response->mutable_status() = status.ToRpcStatus();
    return;
}

void PartitionManager::GetAllStats(google::protobuf::RepeatedPtrField<PartitionStats>* stats) {
    const size_t thd_cnt = thread_pool_->ThreadNum();
    google::protobuf::RepeatedPtrField<PartitionStats> bunches[thd_cnt];
    byte::CountDownLatch countdown(thd_cnt);
    for (size_t i = 0; i < thd_cnt; ++i) {
        auto bunch = &bunches[i];
        auto func = [&countdown, i, bunch] {
            for (auto& pair : thread_info_->partition_map) {
                pair.second->GetStats(bunch->Add());
            }
            countdown.CountDown();
        };
        thread_pool_->KthThread(i)->Invoke(NewCoFuncClosure(func));
    }
    countdown.Wait();
    for (size_t i = 0; i < thd_cnt; ++i) {
        for (auto v : bunches[i]) {
            *stats->Add() = v;
        }
    }
}

void PartitionManager::UnloadAll() {
    byte::CountDownLatch countdown(thread_pool_->ThreadNum());
    for (int i = 0; i < thread_pool_->ThreadNum(); ++i) {
        auto func = [&countdown] {
            for (auto& pair : thread_info_->partition_map) {
                while (pair.second->IsLoading()) {
                    LOG_INFO("Partition waiting for load finish").put("PartitionId", pair.first);
                    CoSleep(1 * 1000 * 1000);  // 1s
                }
                if (pair.second->IsUnloading()) {
                    continue;
                }
                pair.second->Unload();
                BYTE_ASSERT(pair.second->GetStage() == PartitionLoadStage::UNLOADED);
            }
            thread_info_->partition_map.clear();
            countdown.CountDown();
        };
        thread_pool_->KthThread(i)->Invoke(NewCoFuncClosure(func));
    }
    countdown.Wait();
}

absl::flat_hash_map<uint64_t, bool> PartitionManager::GetPartitionLoadedStatus() {
    byte::CountDownLatch countdown(thread_pool_->ThreadNum());
    absl::flat_hash_map<uint64_t, bool> result_arr[thread_pool_->ThreadNum()];
    for (int i = 0; i < thread_pool_->ThreadNum(); ++i) {
        auto func = [&countdown, &result_arr, i] {
            for (auto& pair : thread_info_->partition_map) {
                result_arr[i][pair.first] = pair.second->IsLoaded();
            }
            countdown.CountDown();
        };
        thread_pool_->KthThread(i)->Invoke(NewCoFuncClosure(func));
    }
    countdown.Wait();
    absl::flat_hash_map<uint64_t, bool> result;
    for (int i = 0; i < thread_pool_->ThreadNum(); ++i) {
        result.insert(result_arr[i].begin(), result_arr[i].end());
    }
    return result;
}

void PartitionManager::SetStopping() { stopping_ = true; }

}  // namespace server
}  // namespace bcache2
