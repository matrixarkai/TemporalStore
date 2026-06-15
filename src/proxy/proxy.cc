// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "proxy/proxy.h"

#include <utility>

#include "common/metrics.h"
#include "common/operator_tool.h"
#include "common/proto_enhance.h"
#include "protocol/info.pb.h"
#include "proxy/flags.h"
#include "proxy/service.h"

namespace bcache2 {
namespace proxy {

bool EnsureBrpcThriftProtocolRegistered();

DEFINE_string(proxy_cmdb_jwt_uri, "", "cmdb jwt uri");
DEFINE_string(proxy_cmdb_key, "", "cmdb api key");
DEFINE_string(proxy_cmdb_host, "", "cmdb host");

Status Proxy::Start(Options opts) {
    opts_ = opts;
    // get self host (v4 first)
    std::string host;
    if (std::getenv("BYTED_HOST_IP")) {
        host = std::getenv("BYTED_HOST_IP");
    }
    if (host.empty() && std::getenv("BYTED_HOST_IPV6")) {
        host = std::getenv("BYTED_HOST_IPV6");
    }
    if (host.empty()) {
        return Status::Internal("Failed get host");
    }

    // reset location & port in tce env.
    char* env_port = getenv("PORT0");
    if (env_port != nullptr) {
        opts_.announce_port = atoi(env_port);
    }

    if (!Validate(opts_.location)) {
        LOG_INFO("location is invalid").put("v", opts_.location.ShortDebugString());
        if (!FLAGS_proxy_cmdb_host.empty()) {
            operator_tool::CMDBClient cmdb_client(FLAGS_proxy_cmdb_host, FLAGS_proxy_cmdb_jwt_uri,
                                                  FLAGS_proxy_cmdb_key);
            Status status = cmdb_client.QueryHostLocation(host, &opts_.location);
            if (!status.ok()) {
                return status;
            }
        } else {
            return Status::Internal("location is invalid");
        }
    }

    if (opts_.idc.empty()) {
        if (!opts_.location.vdc().empty()) {
            opts_.idc = opts_.location.vdc();
        } else {
            return Status::Internal("idc is invalid");
        }
    }

    // start proxy
    LOG_INFO("start metaserver tracker");
    metaserver_tracker_ = std::make_shared<MetaServerTracker>(opts_.cluster_name);
    Status status = metaserver_tracker_->Start();
    if (!status.ok()) {
        // Note: ignore error here
        LOG_WARNING("failed to start metaserver tracker").put("result", status);
    }

    LOG_INFO("start client");
    client::ClientOptions client_opts;
    client_opts.psm = "bytedance.bcache2.proxy";
    client_opts.idc = opts_.idc;
    client_opts.host = host;
    client_opts.log_dir = opts_.log_dir;
    client_opts.log_level = static_cast<client::LogLevel>(static_cast<int>(opts_.log_level));
    client_opts.af = client::AddressFamily::kIp4;
    client_opts.master_consul = opts_.master_consul;
    client_opts.master_addr = opts_.master_endpoint;
    client_.reset(new client::ClientImpl());
    status = client_->Init(client_opts);
    if (!status.ok()) {
        LOG_ERROR("Failed to create client").put("Status", status);
        return status;
    }

    LOG_INFO("start rpc server");
    if (!EnsureBrpcThriftProtocolRegistered()) {
        return Status::FailedPrecondition(
            "BRPC thrift protocol is unavailable; rebuild BRPC with thrift framed protocol or "
            "provide BRPC_SOURCE_DIR for the proxy compatibility build");
    }
    brpc::ServerOptions options;
    options.thrift_service = new Bcache2ThriftService(client_.get());
    if (server_.Start(opts.listen_port, &options) != 0) {
        return Status::Aborted("Failed to start brpc server");
    }

    LOG_INFO("start heartbeat routine");
    status = StartHeartbeat();
    if (!status.ok()) {
        LOG_ERROR("failed to start heartbeat routine").put("result", status);
        return status;
    }

    LOG_INFO("start proxy success");
    return Status::OK();
}

void Proxy::Stop() {
    LOG_INFO("stopping heartbeat");
    heart_beat_->Stop();

    LOG_INFO("de-register consul names");
    Config config = GetConfig();
    for (const auto& name : config.consul_names) {
        consul_.DeRegister(name, opts_.announce_port);
    }

    LOG_INFO("stopping rpc server");
    server_.Stop(0);
}

void Proxy::Join() {
    LOG_INFO("joining heartbeat");
    heart_beat_->Join();
    LOG_INFO("joining rpc server");
    server_.Join();
}

Status Proxy::StartHeartbeat() {
    Endpoint self_endpoint;
    self_endpoint.set_ip4(getenv("BYTED_HOST_IP"));
    self_endpoint.set_ip6(getenv("BYTED_HOST_IPV6"));
    if (self_endpoint.ip4().empty() && self_endpoint.ip6().empty()) {
        return Status::Internal("ip4/ip6 are all empty");
    } else if (self_endpoint.ip4().empty()) {
        self_endpoint.set_addr_family(Endpoint::ADDR_V6);
    } else {
        self_endpoint.set_addr_family(Endpoint::ADDR_DUAL_STACK);
    }
    self_endpoint.set_port(opts_.announce_port);

    if (!ValidateFuzzily(opts_.location)) {
        LOG_WARNING("invalid location").put("got", to_string(opts_.location));
        return Status::FailedPrecondition("location invalid");
    }

    heart_beat_.reset(new HeartBeat(opts_.cluster_name,             //
                                    self_endpoint, opts_.location,  //
                                    metaserver_tracker_, this));
    heart_beat_->Start();
    return Status::OK();
}

Proxy::Config Proxy::GetConfig() {
    butil::DoublyBufferedData<Config>::ScopedPtr s;
    if (config_.Read(&s) != 0) {
        return {};
    }
    return *s;
}

std::unordered_set<std::string> Proxy::GetConsulNames() {
    butil::DoublyBufferedData<Config>::ScopedPtr s;
    if (config_.Read(&s) != 0) {
        return {};
    }
    return s->consul_names;
}

void Proxy::UpdateConfig(std::string namespace_name, ProxyConfig pcfg) {
    Config original = GetConfig();
    if (original.namespace_name == namespace_name && original.config.version() == pcfg.version()) {
        return;
    }

    LOG_INFO("change config")
        .put("original_ns_name", original.namespace_name)
        .put("original_config", original.config.ShortDebugString())
        .put("new_ns_name", namespace_name)
        .put("new_config", pcfg.ShortDebugString());
    std::unordered_set<std::string> new_consul_names;
    for (const auto& name : pcfg.consul_names()) {
        new_consul_names.insert(name);
    }

    Config new_config;
    new_config.namespace_name = namespace_name;
    new_config.config = std::move(pcfg);
    new_config.consul_names = new_consul_names;
    auto fn = [new_config = std::move(new_config)](Proxy::Config& store) {
        store = std::move(new_config);
        return 1;
    };
    config_.Modify(fn);

    // NOTE:
    // here we only remove abnormal consul names,
    // heartbeat routine will new lease for normal consul names
    for (const auto& name : original.consul_names) {
        if (new_consul_names.count(name) == 0) {
            Status status = consul_.DeRegister(name, opts_.announce_port);
            LOG_WARNING("deregister consul name").put("name", name).put("result", status);
        }
    }
}

}  // namespace proxy
}  // namespace bcache2
