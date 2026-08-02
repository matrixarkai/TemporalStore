// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/macros.h>

#include <memory>
#include <string>
#include <unordered_map>

#include "brpc/controller.h"
#include "bvar/bvar.h"
#include "common/logging.h"
#include "common/metrics.h"
#include "common/time.h"
#include "protocol/server.pb.h"
#include "server/partition_manager.h"

namespace bcache2 {
namespace server {

struct ServerServiceMetrics {
    bvar::Adder<uint64_t> request_total;
    bvar::Adder<uint64_t> request_success_total;
    bvar::Adder<uint64_t> request_failed_total;
    bvar::Adder<uint64_t> raft_apply_total;
    bvar::LatencyRecorder request_latency_us;

    void Expose(const std::string& prefix) {
        request_total.expose_as(prefix, "request_total");
        request_success_total.expose_as(prefix, "request_success_total");
        request_failed_total.expose_as(prefix, "request_failed_total");
        raft_apply_total.expose_as(prefix, "raft_apply_total");
        request_latency_us.expose(prefix, "request");
    }

    void Record(const std::string& type_name, bool ok, int64_t latency_us) {
        request_total << 1;
        if (type_name.find("ApplyDataRaftLog") != std::string::npos) {
            raft_apply_total << 1;
        }
        if (ok) {
            request_success_total << 1;
        } else {
            request_failed_total << 1;
        }
        request_latency_us << latency_us;
    }
};

// unified call entry for partition manager
template <typename Request, typename Response>
class PartitionManagerCallHelper {
 public:
    static void CallPartitionManager(PartitionManager* partition_manager,
                                     void (PartitionManager::*method)(Controller*, const Request*,
                                                                      Response*, Closure<void>*),
                                     google::protobuf::RpcController* ctrl, const Request* request,
                                     Response* response, google::protobuf::Closure* done,
                                     byte::LogLevel log_level, ServerServiceMetrics* metrics) {
        brpc::Controller* brpc_ctrl = static_cast<brpc::Controller*>(ctrl);
        auto request_cost = std::make_shared<TimeCost>();
        LOG_MESSAGE(log_level, "RPC Received")
            .put("Remote", brpc_ctrl->remote_side())
            .put("TraceId", request->opt().trace_id())
            .put("Type", request->GetTypeName())
            .put("Request", request->ShortDebugString());

        Controller* new_ctrl = new Controller(request->opt().trace_id());
        auto func = [ctrl, request, response, done, log_level, new_ctrl, metrics, request_cost] {
            std::unique_ptr<Controller> _ctrl(new_ctrl);
            response->mutable_status()->set_code(Code::kOK);
            if (!new_ctrl->status().ok()) {
                response->mutable_status()->CopyFrom(new_ctrl->status().ToRpcStatus());
            }
            if (metrics != nullptr) {
                metrics->Record(request->GetTypeName(), response->status().code() == Code::kOK,
                                request_cost->GetElapsedInUs());
            }

            brpc::Controller* brpc_ctrl = static_cast<brpc::Controller*>(ctrl);
            LOG_MESSAGE(log_level, "RPC Finished")
                .put("Remote", brpc_ctrl->remote_side())
                .put("TraceId", request->opt().trace_id())
                .put("Type", response->GetTypeName())
                .put("Response", response->ShortDebugString());

            done->Run();
        };
        (partition_manager->*method)(new_ctrl, request, response, NewFuncClosure(func));
    }
};

class ServiceImpl : public ServerService {
 public:
    ServiceImpl(PartitionManager* partition_manager, ServerServiceMetrics* metrics)
        : partition_manager_(partition_manager), metrics_(metrics) {}
    virtual ~ServiceImpl() {}

    void Load(google::protobuf::RpcController* ctrl, const LoadRequest* request,
              LoadResponse* response, google::protobuf::Closure* done) override {
        CallPartitionManager(&PartitionManager::Load, ctrl, request, response, done,
                             byte::LOG_LEVEL_INFO);
    }
    void Unload(google::protobuf::RpcController* ctrl, const UnloadRequest* request,
                UnloadResponse* response, google::protobuf::Closure* done) override {
        CallPartitionManager(&PartitionManager::Unload, ctrl, request, response, done,
                             byte::LOG_LEVEL_INFO);
    }
    // TODO(zhangyuan.42): refine service proto
    void ExecuteCmd(google::protobuf::RpcController* ctrl, const ExecuteCmdRequest* request,
                    ExecuteCmdResponse* response, google::protobuf::Closure* done) override {
        BatchExecuteCmdRequest* batch_request = new BatchExecuteCmdRequest();
        BatchExecuteCmdResponse* batch_response = new BatchExecuteCmdResponse();
        *batch_request->mutable_opt() = request->opt();
        batch_request->set_partition_id(request->partition_id());
        batch_request->set_load_version(request->load_version());
        *batch_request->add_request() = request->request();
        auto func = [batch_request, batch_response, response, done] {
            if (batch_response->response_size() > 0) {
                BYTE_ASSERT(batch_response->response_size() == 1);
                response->set_allocated_response(batch_response->mutable_response()->ReleaseLast());
                if (response->response().status().code() == kOK &&
                    batch_response->status().code() != kOK) {
                    *(response->mutable_response()->mutable_status()) = batch_response->status();
                }
            } else if (batch_response->status().code() != kOK) {
                *(response->mutable_response()->mutable_status()) = batch_response->status();
            }
            delete batch_request;
            delete batch_response;
            done->Run();
        };
        BatchExecuteCmd(ctrl, batch_request, batch_response, NewGoogleClosure(func));
    }
    void BatchExecuteCmd(google::protobuf::RpcController* ctrl,
                         const BatchExecuteCmdRequest* request, BatchExecuteCmdResponse* response,
                         google::protobuf::Closure* done) override {
        CallPartitionManager(&PartitionManager::BatchExecuteCmd, ctrl, request, response, done,
                             byte::LOG_LEVEL_DEBUG);
    }
    void GetInfo(::google::protobuf::RpcController* ctrl, const ::bcache2::GetInfoRequest* request,
                 ::bcache2::GetInfoResponse* response, ::google::protobuf::Closure* done) override {
        CallPartitionManager(&PartitionManager::GetInfo, ctrl, request, response, done,
                             byte::LOG_LEVEL_DEBUG);
    }
    void ReadPartitionStream(::google::protobuf::RpcController* ctrl,
                             const ::bcache2::ReadPartitionStreamRequest* request,
                             ::bcache2::ReadPartitionStreamResponse* response,
                             ::google::protobuf::Closure* done) override {
        CallPartitionManager(&PartitionManager::ReadPartitionStream, ctrl, request, response, done,
                             byte::LOG_LEVEL_DEBUG);
    }
    void ScanPartitionStream(::google::protobuf::RpcController* ctrl,
                             const ::bcache2::ScanPartitionStreamRequest* request,
                             ::bcache2::ScanPartitionStreamResponse* response,
                             ::google::protobuf::Closure* done) override {
        CallPartitionManager(&PartitionManager::ScanPartitionStream, ctrl, request, response, done,
                             byte::LOG_LEVEL_DEBUG);
    }
    void ApplyDataRaftLog(::google::protobuf::RpcController* ctrl,
                          const ::bcache2::ApplyDataRaftLogRequest* request,
                          ::bcache2::ApplyDataRaftLogResponse* response,
                          ::google::protobuf::Closure* done) override {
        CallPartitionManager(&PartitionManager::ApplyDataRaftLog, ctrl, request, response, done,
                             byte::LOG_LEVEL_DEBUG);
    }
    void GetDataRaftStatus(
        ::google::protobuf::RpcController* ctrl,
        const ::bcache2::GetDataRaftStatusRequest* request,
        ::bcache2::GetDataRaftStatusResponse* response,
        ::google::protobuf::Closure* done) override {
        CallPartitionManager(&PartitionManager::GetDataRaftStatus, ctrl, request, response, done,
                             byte::LOG_LEVEL_DEBUG);
    }
    void TriggerDataRaftSnapshot(
        ::google::protobuf::RpcController* ctrl,
        const ::bcache2::TriggerDataRaftSnapshotRequest* request,
        ::bcache2::TriggerDataRaftSnapshotResponse* response,
        ::google::protobuf::Closure* done) override {
        CallPartitionManager(&PartitionManager::TriggerDataRaftSnapshot, ctrl, request, response,
                             done, byte::LOG_LEVEL_INFO);
    }
    void SetConfig(google::protobuf::RpcController* ctrl, const SetConfigRequest* request,
                   SetConfigResponse* response, google::protobuf::Closure* done) override {
        CallPartitionManager(&PartitionManager::SetConfig, ctrl, request, response, done,
                             byte::LOG_LEVEL_INFO);
    }
    void GetConfig(google::protobuf::RpcController* ctrl, const GetConfigRequest* request,
                   GetConfigResponse* response, google::protobuf::Closure* done) override {
        CallPartitionManager(&PartitionManager::GetConfig, ctrl, request, response, done,
                             byte::LOG_LEVEL_DEBUG);
    }

    void UpdateMembership(google::protobuf::RpcController* ctrl,
                          const UpdateMembershipRequest* request, AckResponse* response,
                          google::protobuf::Closure* done) override {
        CallPartitionManager(&PartitionManager::UpdateMembership, ctrl, request, response, done,
                             byte::LOG_LEVEL_INFO);
    }
    void Ping(::google::protobuf::RpcController* controller, const ::bcache2::EmptyMessage* request,
              ::bcache2::EmptyMessage* response, ::google::protobuf::Closure* done) {
        done->Run();
    }

 private:
    template <typename Request, typename Response>
    void CallPartitionManager(void (PartitionManager::*method)(Controller*, const Request*,
                                                               Response*, Closure<void>*),
                              google::protobuf::RpcController* ctrl, const Request* request,
                              Response* response, google::protobuf::Closure* done,
                              byte::LogLevel log_level) {
        PartitionManagerCallHelper<Request, Response>::CallPartitionManager(
            partition_manager_, method, ctrl, request, response, done, log_level, metrics_);
    }

    PartitionManager* partition_manager_ = nullptr;
    ServerServiceMetrics* metrics_ = nullptr;

    DISALLOW_COPY_AND_ASSIGN(ServiceImpl);
};

}  // namespace server
}  // namespace bcache2
