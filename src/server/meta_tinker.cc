// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "server/meta_tinker.h"

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>

#include "brpc/channel.h"
#include "butil/endpoint.h"
#include "butil/time.h"
#include "gflags/gflags.h"

#include "common/controller.h"
#include "common/logging.h"
#include "protocol/host_spec.pb.h"
#include "protocol/metaserver.pb.h"
#include "server/partition_manager.h"
#include "server/util.h"

namespace bcache2 {
namespace server {

DEFINE_bool(server_meta_tinker_enabled, true, "meta tinker trigger");
BRPC_VALIDATE_GFLAG(server_meta_tinker_enabled, brpc::PassValidate);
DEFINE_uint64(server_meta_tinker_interval_ms, 60'000, "interval in millisecond");
BRPC_VALIDATE_GFLAG(server_meta_tinker_interval_ms, brpc::PassValidate);
DEFINE_uint64(server_meta_tinker_timeout_ms, 3'000, "timeout in millisecond");
BRPC_VALIDATE_GFLAG(server_meta_tinker_timeout_ms, brpc::PassValidate);
DEFINE_int64(server_meta_tinker_deprecated_partition_cold_time_sec, 10800,  // 3h
             "cold time to unload remote missing partition");
BRPC_VALIDATE_GFLAG(server_meta_tinker_deprecated_partition_cold_time_sec, brpc::PassValidate);

////////

MetaTinker::MetaTinker(std::string cluster_name, HostSpec host_spec,
                       std::shared_ptr<MetaServerTracker> ms_tracker,
                       PartitionManager* partition_manager)
    : cluster_name_(std::move(cluster_name)),
      host_spec_(std::move(host_spec)),
      ms_tracker_(std::move(ms_tracker)),
      partition_manager_(partition_manager) {}

MetaTinker::~MetaTinker() { Stop(); }

void MetaTinker::Stop() { LoopThread::Stop(); }

uint64_t MetaTinker::LoopIntervalMs() { return FLAGS_server_meta_tinker_interval_ms; }

void MetaTinker::DoLoop() {
    if (!FLAGS_server_meta_tinker_enabled) {
        LOG_INFO_SAMPLE("meta tinker disabled");
        return;
    }
    metaserver::ListServerPartitionResponse response;
    Status status = Fetch(&response);
    if (!status.ok()) {
        LOG_WARNING("failed to fetch meta from metaserver").put("status", status);
        return;
    }

    const ServerInfo& server_info = response.server_info();
    std::unordered_set<uint64_t> local_partition_ids;
    std::unordered_set<uint64_t> remote_partition_ids;
    // TODO(wuzhenyu) compare node list
    // TODO(wuzhenyu) refactor, there is no multi-node currently,
    // we just call partition manager
    google::protobuf::RepeatedPtrField<PartitionStats> stats;
    partition_manager_->GetAllStats(&stats);
    for (const auto& p : stats) {
        local_partition_ids.insert(p.id());
    }

    for (const auto& node_partitions : response.node_partitions()) {
        for (const auto& p : node_partitions.partitions()) {
            remote_partition_ids.insert(p.id());
            auto iter = remote_missing_partition_id_map_.find(p.id());
            if (iter != remote_missing_partition_id_map_.end()) {
                remote_missing_partition_id_map_.erase(iter);
            }

            bool local_exist = local_partition_ids.count(p.id()) > 0;
            if (p.state() == PartitionState::P_NORMAL && !local_exist) {
                LOG_ERROR("partition in metaserver but not exists in local!!!")
                    .put("partition_id", p.id());
                continue;
            }
            if (local_exist) {
                TinkPartition(p);
            }
        }
    }

    std::unordered_set<uint64_t> remote_missing_ids;
    for (uint64_t local_pid : local_partition_ids) {
        if (remote_partition_ids.count(local_pid) == 0) {
            LOG_WARNING("partition exists in local but not in metaserver")
                .put("partition_id", local_pid);
            remote_missing_ids.insert(local_pid);
        }
    }
    if (!remote_missing_ids.empty()) {
        LOG_WARNING("found a few of partitions exist in local but not in metaserver")
            .put("count", remote_missing_ids.size())
            .put("server_state", ServerState_Name(server_info.state()));
        if (server_info.state() == ServerState::SERVER_FROZEN) {
            for (const uint64_t pid : remote_missing_ids) {
                UnloadPartition(pid);
            }
        } else {
            // TODO(wuzhenyu) fall-back due to considering metaserver problem
            const int64_t now = butil::gettimeofday_s();
            for (uint64_t pid : remote_missing_ids) {
                auto iter = remote_missing_partition_id_map_.find(pid);
                if (iter == remote_missing_partition_id_map_.end()) {
                    remote_missing_partition_id_map_[pid] = now;
                } else if (now > iter->second +
                                     FLAGS_server_meta_tinker_deprecated_partition_cold_time_sec) {
                    LOG_WARNING("partition missing in metaserver too long, try to unload")
                        .put("partition_id", pid)
                        .put("first_found_time", iter->second);
                    UnloadPartition(pid);
                    remote_missing_partition_id_map_.erase(iter);
                }
            }  // for
        }
    }  // remote missing but exists in local
}

Status MetaTinker::Fetch(metaserver::ListServerPartitionResponse* response) {
    butil::EndPoint leader_endpoint;
    Status status = ms_tracker_->GetLeaderEndpoint(&leader_endpoint);
    if (!status.ok()) {
        return status;
    }
    brpc::Channel channel;
    brpc::ChannelOptions opts;
    opts.connect_timeout_ms = -1;
    channel.Init(leader_endpoint, &opts);

    brpc::Controller cntl;
    uint64_t log_id = butil::fast_rand();
    cntl.set_log_id(log_id);
    cntl.set_timeout_ms(FLAGS_server_meta_tinker_timeout_ms);

    metaserver::QueryService_Stub stub(&channel);
    metaserver::ListServerPartitionRequest request;
    InitRequestId(cluster_name_, request.mutable_id());
    *request.mutable_endpoint() = host_spec_.endpoint();
    stub.ListServerPartition(&cntl, &request, response, nullptr);
    if (cntl.Failed()) {
        LOG_WARNING("fetch meta failed")
            .put("log_id", log_id)
            .put("remote", leader_endpoint)
            .put("err", cntl.ErrorText());
        return Status::Internal("rpc failed");
    }
    status = Status::FromRpcStatus(response->status());
    if (!status.ok()) {
        LOG_WARNING("metaserver returned failure")
            .put("log_id", log_id)
            .put("remote", leader_endpoint)
            .put("status", status);
        return status;
    }
    return Status::OK();
}

void MetaTinker::TinkPartition(const NodePartition& p) {
    if (p.has_membership()) {
        Controller cntl;
        UpdateMembershipRequest request;
        request.set_partition_id(p.id());
        *request.mutable_membership() = p.membership();
        AckResponse response;
        SyncClosure sync;
        partition_manager_->UpdateMembership(&cntl, &request, &response, &sync);
        sync.Wait();
        if (response.status().code() != 0) {
            LOG_WARNING("failed to update membership")
                .put("partition_id", p.id())
                .put("response", response.status().ShortDebugString());
        }
    }
    if (p.state() == PartitionState::P_FROZEN) {
        LOG_WARNING("partition is frozen state, try to unload").put("partition_id", p.id());
        UnloadPartition(p.id());
        return;
    }

    if (p.state() != PartitionState::P_NORMAL) {
        return;
    }
    if (p.has_config()) {
        Controller cntl;
        SetConfigRequest request;
        request.set_partition_id(p.id());
        *request.mutable_config() = p.config();
        SetConfigResponse response;
        SyncClosure sync;
        partition_manager_->SetConfig(&cntl, &request, &response, &sync);
        sync.Wait();
        if (response.status().code() != 0) {
            LOG_INFO("failed to update config")
                .put("partition_id", p.id())
                .put("response", response.status().ShortDebugString());
        }
    }
}

struct OneWayClosure : public Closure<void> {
    void Run() override { delete this; }
    bool IsSelfDelete() const override { return true; }

    std::shared_ptr<google::protobuf::Message> request;
    std::shared_ptr<google::protobuf::Message> response;
    std::shared_ptr<Controller> cntl;
};

void MetaTinker::UnloadPartition(uint64_t pid) {
    auto request = std::make_shared<UnloadRequest>();
    request->set_partition_id(pid);
    auto response = std::make_shared<UnloadResponse>();

    OneWayClosure* cb = new OneWayClosure();
    cb->request = request;
    cb->response = response;
    cb->cntl = std::make_shared<Controller>();
    partition_manager_->Unload(cb->cntl.get(), request.get(), response.get(), cb);
    // TODO(wuzhenyu) FIXME unload may block forever
    // SyncClosure sync;
    // partition_manager_->Unload(&cntl, &request, &response, &sync);
    // sync.Wait();
    // LOG_WARNING("try to unload partition")
    //     .put("partition_id", pid)
    //     .put("result", response.status().ShortDebugString());
}

}  // namespace server
}  // namespace bcache2
