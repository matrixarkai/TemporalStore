// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "proxy/heartbeat.h"

#include <memory>
#include <string>
#include <utility>

#include "common/logging.h"
#include "common/time_tracer.h"
#include "proxy/flags.h"
#include "proxy/proxy.h"

namespace bcache2 {
namespace proxy {

HeartBeat::HeartBeat(std::string cluster_name, Endpoint ep, Location loc,
                     std::shared_ptr<MetaServerTracker> ms_tracker, Proxy* proxy)
    : cluster_name_(std::move(cluster_name)),
      self_endpoint_(std::move(ep)),
      self_location_(std::move(loc)),
      boot_time_us_(butil::gettimeofday_us()),
      ms_tracker_(std::move(ms_tracker)),
      proxy_(proxy) {}

void HeartBeat::Start() {
    if (started_) {
        return;
    }
    started_ = true;
    stopped_ = false;
    loop_thread_ = std::thread(&HeartBeat::LoopWorker, this);
    LOG_INFO("Heartbeat start").put("This", this);
}

void HeartBeat::Stop() {
    if (!started_) {
        return;
    }
    started_ = false;
    LOG_INFO("Heartbeat going to stop").put("This", this);
    SendStopSignal();
}

void HeartBeat::Join() {
    if (stopped_) {
        return;
    }

    if (loop_thread_.joinable()) {
        loop_thread_.join();
    }

    stopped_ = true;
    LOG_INFO("Heartbeat stopped").put("This", this);
}

void HeartBeat::LoopWorker() {
#ifndef __CYGWIN__
    pthread_setname_np(pthread_self(), "HeartBeatThread");
#endif

    while (started_) {
        int64_t start = butil::cpuwide_time_ms();

        SendHeartbeat();
        RegisterService();

        int64_t elapse = butil::cpuwide_time_ms() - start;
        if (elapse > 500) {
            LOG_INFO("slow heartbeat").put("elapse_ms", elapse);
        }
        if (FLAGS_heartbeat_interval_ms > static_cast<uint64_t>(elapse)) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(FLAGS_heartbeat_interval_ms - elapse));
        }
    }
    LOG_INFO("Heartbeat exit loop");
}

void HeartBeat::RegisterService() {
    const int port = self_endpoint_.port();
    for (const auto& name : proxy_->GetConsulNames()) {
        LOG_DEBUG("register consul").put("name", name).put("prot", port);
        Status status = proxy_->GetConsul()->Register(name, port, FLAGS_register_ttl_s);
        if (!status.ok()) {
            LOG_WARNING("Failed to register service")
                .put("Service", name)
                .put("Port", port)
                .put("Ttl", FLAGS_register_ttl_s)
                .put("Status", status);
        }
    }
}

static void InitRequestId(const std::string& cluster_name, metaserver::RequestId* id) {
    id->set_timestamp(butil::gettimeofday_s());
    id->set_cluster_name(cluster_name);
    id->set_operator_name("proxy");
}

void HeartBeat::SendHeartbeat() {
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
    cntl.set_timeout_ms(FLAGS_proxy_heartbeat_timeout_ms);
    metaserver::HeartbeatService_Stub stub(&channel);
    metaserver::ProxyHeartbeatRequest request;
    metaserver::ProxyHeartbeatResponse response;
    InitHeartbeatRequest(&request);
    stub.ProxyHeartbeat(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        LOG_WARNING("send heartbeat failed")
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
        proxy_->UpdateConfig("", ProxyConfig());
        MaybeAutoRegister(status, response);
        return;
    }
    registered_ = true;

    HandleHeartbeatResponse(response);
}

void HeartBeat::InitHeartbeatRequest(metaserver::ProxyHeartbeatRequest* request) {
    InitRequestId(cluster_name_, request->mutable_id());
    *request->mutable_endpoint() = self_endpoint_;
    request->set_boot_time_us(boot_time_us_);
    request->set_binary_version(BCACHE2_VERSION);
    Proxy::Config config = proxy_->GetConfig();
    request->set_namespace_name(config.namespace_name);
    request->set_config_version(config.config.version());
}

void HeartBeat::HandleHeartbeatResponse(const metaserver::ProxyHeartbeatResponse& response) {
    BYTE_ASSERT(response.status().code() == 0) << this;
    if (response.config_changed()) {
        LOG_INFO("config changed, try to update");
        proxy_->UpdateConfig(response.namespace_name(), response.config());
    }
}

void HeartBeat::SendStopSignal() {
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
    cntl.set_timeout_ms(FLAGS_proxy_heartbeat_timeout_ms);

    metaserver::ProxyNotifyStopRequest request;
    InitRequestId(cluster_name_, request.mutable_id());
    *request.mutable_endpoint() = self_endpoint_;

    AckResponse response;
    stub.ProxyNotifyStop(&cntl, &request, &response, nullptr);
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

void HeartBeat::MaybeAutoRegister(Status status,
                                  const metaserver::ProxyHeartbeatResponse& response) {
    if (!FLAGS_proxy_auto_register || registered_ || !status.IsNotFound()) {
        return;
    }
    int64_t now = butil::cpuwide_time_ms();
    if (now < last_register_timestamp_ms_ + 60 * 1000) {
        return;
    }
    last_register_timestamp_ms_ = now;
    AutoRegisterInternal();
}

void HeartBeat::AutoRegisterInternal() {
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
    cntl.set_timeout_ms(FLAGS_proxy_heartbeat_timeout_ms);
    metaserver::AddProxyRequest request;
    InitRequestId(cluster_name_, request.mutable_id());
    request.mutable_endpoint()->CopyFrom(self_endpoint_);
    request.mutable_location()->CopyFrom(self_location_);

    AckResponse response;
    metaserver::ManageService_Stub stub(&channel);
    stub.AddProxy(&cntl, &request, &response, nullptr);
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
    registered_ = true;
}

}  // namespace proxy
}  // namespace bcache2
