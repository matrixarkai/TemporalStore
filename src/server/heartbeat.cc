// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "server/heartbeat.h"

#include <memory>
#include <string>
#include <utility>

#include "brpc/channel.h"
#include "butil/endpoint.h"
#include "butil/time.h"
#include "gflags/gflags.h"

#include "common/logging.h"
#include "protocol/host_spec.pb.h"
#include "protocol/metaserver.pb.h"
#include "server/partition_manager.h"
#include "server/util.h"

namespace bcache2 {
namespace server {

DEFINE_uint64(server_heartbeat_interval_ms, 3'000, "interval in millisecond");
BRPC_VALIDATE_GFLAG(server_heartbeat_interval_ms, brpc::PassValidate);
DEFINE_uint64(server_heartbeat_timeout_ms, 3'000, "timeout in millisecond");
BRPC_VALIDATE_GFLAG(server_heartbeat_timeout_ms, brpc::PassValidate);
DEFINE_uint64(server_heartbeat_report_stats_sample, 60, "stats would be piped by heartbeat");
BRPC_VALIDATE_GFLAG(server_heartbeat_report_stats_sample, brpc::PassValidate);
DEFINE_bool(server_auto_register, true, "register to metaserver automatically");
BRPC_VALIDATE_GFLAG(server_auto_register, brpc::PassValidate);
DEFINE_bool(server_notify_stop_on_shutdown, true, "notify metaserver when server stops");
BRPC_VALIDATE_GFLAG(server_notify_stop_on_shutdown, brpc::PassValidate);

////////

Heartbeat::Heartbeat(std::string cluster_name, HostSpec host_spec,
                     std::shared_ptr<MetaServerTracker> ms_tracker,
                     PartitionManager* partition_manager)
    : cluster_name_(std::move(cluster_name)),
      host_spec_(std::move(host_spec)),
      boot_time_us_(butil::gettimeofday_us()),
      ms_tracker_(std::move(ms_tracker)),
      partition_manager_(partition_manager) {}

Heartbeat::~Heartbeat() { Stop(); }

void Heartbeat::Stop() {
    LoopThread::Stop();
    SendStopSignal();
}

uint64_t Heartbeat::LoopIntervalMs() {
    if (FLAGS_server_heartbeat_interval_ms >= last_heartbeat_elapse_ms_) {
        return FLAGS_server_heartbeat_interval_ms - last_heartbeat_elapse_ms_;
    }
    return 0;
}

void Heartbeat::DoLoop() {
    int64_t start = butil::cpuwide_time_ms();
    if (fallback_end_timepoint_ms_ > 0 && start < fallback_end_timepoint_ms_) {
        return;
    }
    fallback_end_timepoint_ms_ = 0;
    SendHeartbeat();
    last_heartbeat_elapse_ms_ = butil::cpuwide_time_ms() - start;
}

void Heartbeat::SendHeartbeat() {
    TimeTracer tt;
    BYTE_DEFER({
        if (tt.TotalSpentMs() > FLAGS_server_heartbeat_interval_ms / 2) {
            LOG_INFO("slow heartbeat").put("t", tt.ToString());
        }
    });
    butil::EndPoint leader_endpoint;
    Status status = ms_tracker_->GetLeaderEndpoint(&leader_endpoint);
    if (!status.ok()) {
        LOG_WARNING("failed to get leader").put("result", status);
        return;
    }
    brpc::Channel channel;
    brpc::ChannelOptions opts;
    opts.connect_timeout_ms = -1;
    channel.Init(leader_endpoint, &opts);

    brpc::Controller cntl;
    uint64_t log_id = butil::fast_rand();
    cntl.set_log_id(log_id);
    cntl.set_timeout_ms(FLAGS_server_heartbeat_timeout_ms);
    metaserver::HeartbeatService_Stub stub(&channel);
    metaserver::ServerHeartbeatRequest request;
    metaserver::ServerHeartbeatResponse response;
    InitHeartbeatRequest(&request);
    tt.AddEvent("prep_request");
    stub.ServerHeartbeat(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        LOG_WARNING("send heartbeat failed")
            .put("log_id", log_id)
            .put("remote", leader_endpoint)
            .put("err", cntl.ErrorText());
        return;
    }
    tt.AddEvent("send_request");

    status = Status::FromRpcStatus(response.status());
    if (!status.ok()) {
        LOG_WARNING("heartbeat failed")
            .put("log_id", log_id)
            .put("remote", leader_endpoint)
            .put("response", response.ShortDebugString());
        registered_ = false;
        MaybeAutoRegister(status, response);
        tt.AddEvent("maybe_auto_register");
        return;
    }
    if (!registered_) {
        LOG_INFO("server registered successfully!");
        registered_ = true;
    }
}

void Heartbeat::MaybeAutoRegister(Status status,
                                  const metaserver::ServerHeartbeatResponse& response) {
    if (!FLAGS_server_auto_register || status.ok() ||
        (!status.IsNotFound() && !status.IsResourceFrozen())) {
        return;
    }
    int64_t now = butil::cpuwide_time_ms();
    if (response.forbid_auto_register()) {
        LOG_INFO("auto register forbidden");
        fallback_end_timepoint_ms_ = now + 60 * 1000;
        return;
    }

    if (now < last_register_timestamp_ms_ + 60 * 1000) {
        return;
    }
    last_register_timestamp_ms_ = now;
    if (status.IsResourceFrozen()) {
        LOG_WARNING("try to drop legacy me from metaserver");
        Status result = TryToDropLegacyMe();
        if (!result.ok()) {
            LOG_WARNING("failed to drop myself").put("result", result);
            return;
        }
    }
    AutoRegisterInternal();
}

Status Heartbeat::TryToDropLegacyMe() {
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
    cntl.set_timeout_ms(FLAGS_server_heartbeat_timeout_ms);
    metaserver::DropServerRequest request;
    InitRequestId(cluster_name_, request.mutable_id());
    request.mutable_endpoint()->CopyFrom(host_spec_.endpoint());

    AckResponse response;
    metaserver::ManageService_Stub stub(&channel);
    stub.DropServer(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        LOG_WARNING("drop failed")
            .put("log_id", log_id)
            .put("remote", leader_endpoint)
            .put("err", cntl.ErrorText());
        return Status::Internal("rpc failed");
    }
    status = Status::FromRpcStatus(response.status());
    if (!status.ok()) {
        LOG_WARNING("drop failed")
            .put("log_id", log_id)
            .put("remote", leader_endpoint)
            .put("response", response.ShortDebugString());
    }
    return status;
}

void Heartbeat::AutoRegisterInternal() {
    butil::EndPoint leader_endpoint;
    Status status = ms_tracker_->GetLeaderEndpoint(&leader_endpoint);
    if (!status.ok()) {
        LOG_WARNING("register failed, get leader failed").put("result", status);
        return;
    }
    brpc::Channel channel;
    brpc::ChannelOptions opts;
    opts.connect_timeout_ms = -1;
    channel.Init(leader_endpoint, &opts);

    brpc::Controller cntl;
    uint64_t log_id = butil::fast_rand();
    cntl.set_log_id(log_id);
    cntl.set_timeout_ms(FLAGS_server_heartbeat_timeout_ms);
    metaserver::AddServerRequest request;
    InitRequestId(cluster_name_, request.mutable_id());
    request.mutable_endpoint()->CopyFrom(host_spec_.endpoint());
    request.mutable_location()->CopyFrom(host_spec_.location());
    request.mutable_numa_nodes()->CopyFrom(host_spec_.numa_nodes());

    AckResponse response;
    metaserver::ManageService_Stub stub(&channel);
    stub.AddServer(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        LOG_WARNING("register failed")
            .put("log_id", log_id)
            .put("remote", leader_endpoint)
            .put("err", cntl.ErrorText());
        return;
    }
    status = Status::FromRpcStatus(response.status());
    if (!status.ok()) {
        LOG_WARNING("heartbeat failed")
            .put("log_id", log_id)
            .put("remote", leader_endpoint)
            .put("response", response.ShortDebugString());
        return;
    }
    LOG_INFO("server auto registered successfully!");
    registered_ = true;
}

void Heartbeat::InitHeartbeatRequest(metaserver::ServerHeartbeatRequest* request) {
    InitRequestId(cluster_name_, request->mutable_id());
    *request->mutable_endpoint() = host_spec_.endpoint();
    request->set_boot_time_us(boot_time_us_);
    uint64_t sample = FLAGS_server_heartbeat_report_stats_sample;
    if (sample == 0 || (round_++) % sample == 0) {
        request->set_with_stats(true);
        partition_manager_->GetAllStats(request->mutable_stats());
    }
    request->set_binary_version(BCACHE2_VERSION);
}

void Heartbeat::SendStopSignal() {
    if (!FLAGS_server_notify_stop_on_shutdown) {
        return;
    }
    butil::EndPoint leader_endpoint;
    Status status = ms_tracker_->GetLeaderEndpoint(&leader_endpoint);
    if (!status.ok()) {
        LOG_WARNING("failed to get leader").put("result", status);
        return;
    }
    brpc::Channel channel;
    brpc::ChannelOptions opts;
    opts.connect_timeout_ms = -1;
    channel.Init(leader_endpoint, &opts);
    metaserver::HeartbeatService_Stub stub(&channel);

    brpc::Controller cntl;
    uint64_t log_id = butil::fast_rand();
    cntl.set_log_id(log_id);
    cntl.set_timeout_ms(FLAGS_server_heartbeat_timeout_ms);

    metaserver::ServerNotifyStopRequest request;
    InitRequestId(cluster_name_, request.mutable_id());
    *request.mutable_endpoint() = host_spec_.endpoint();

    AckResponse response;
    stub.ServerNotifyStop(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        LOG_WARNING("send notify stop failed")
            .put("log_id", log_id)
            .put("remote", leader_endpoint)
            .put("err", cntl.ErrorText());
        return;
    }
    status = Status::FromRpcStatus(response.status());
    LOG_INFO("notify metaserver returned")
        .put("log_id", log_id)
        .put("remote", leader_endpoint)
        .put("response", response.ShortDebugString());
}

}  // namespace server
}  // namespace bcache2
