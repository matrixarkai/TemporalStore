// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "server/partition_manager.h"

#include <byte/base/closure.h>
#include <byte/concurrent/count_down_latch.h>
#include <byte/thread/async_thread.h>
#include <gflags/gflags.h>

#include <memory>
#include <utility>

#include "common/bits.h"
#include "common/cmd_manager.h"
#include "common/function_closure.h"
#include "common/scoped_invoker.h"
#include "partition/partition.h"
#include "protocol/metaserver.pb.h"
#include "server/server.h"
#include "server/util.h"

namespace bcache2 {
namespace server {

__thread PartitionManager::ThreadLocalInfo* PartitionManager::thread_info_ = nullptr;

PartitionManager::PartitionManager(const std::string& cluster_name, Server* server,
                                   byte::AsyncThreadPool* thread_pool, stream::Env* env,
                                   blockcache::BlockCache* blockcache)
    : cluster_name_(cluster_name),
      server_(server),
      thread_pool_(thread_pool),
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
    options.uri = request->partition_uri();
    options.table_name = request->table_name();
    options.host = server_->GetHost();
    options.host_v6 = server_->GetHostV6();
    options.port = server_->GetListenPort();
    options.partition_id = partition_id;
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
        ReportLoadResult(partition_id, std::move(status));
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
    GetThread(request->partition_id())
        ->Invoke(NewClosure(this, &PartitionManager::BatchExecuteCmdInternal, context));
}

void PartitionManager::BatchExecuteCmdInternal(BatchExecuteContext* context) {
    LOG_CALL_DEBUG()
        .put("PartitionId", context->request->partition_id())
        .put("LoadVersion", context->request->load_version())
        .put("TraceId", context->ctrl->trace_id());

    ScopedInvoker done(context->callback);
    auto it = thread_info_->partition_map.find(context->request->partition_id());
    if (it == thread_info_->partition_map.end() ||
        (context->request->pin_primary() &&
        (it->second->GetPartitionID() != it->second->GetPrimaryPartitionId()))) {
        // client should refresh table topo
        context->ctrl->set_status(Status::TopomError("Partition not exists or not primary"));
        return;
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
            ->Invoke(NewClosure(this, &PartitionManager::UpdateMembership, ctrl, request, response,
                                callback));
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
