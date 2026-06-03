// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "test/mini_cluster/mini_cluster.h"

#include <brpc/channel.h>

#include <random>

#include "spdlog/fmt/fmt.h"

#include "common/logging.h"
#include "metaserver_v2/flags.h"
#include "metaserver_v2/metaserver.h"
#include "metaserver_v2/metrics.h"
#include "protocol/master.pb.h"
#include "protocol/metaserver.pb.h"
#include "proxy/proxy.h"
#include "server/server.h"
#include "test/common/util.h"

// Note: proxy's flag has no namespace protection
DECLARE_uint64(heartbeat_interval_ms);
DECLARE_uint64(server_stopping_wait_s);

namespace bcache2 {

DECLARE_string(metaserver_uri);
namespace server {
DECLARE_uint64(server_heartbeat_interval_ms);
}  // namespace server

MiniCluster::MiniCluster() {}

MiniCluster::~MiniCluster() {}

void MiniCluster::Init(const Options& options) {
    options_ = options;
    if (options_.work_dir.empty()) {
        temp_dir_.reset(new TempDir());
        options_.work_dir = temp_dir_->GetDir();
    }
    if (options_.master_port == 0) {
        options_.master_port = RandomPort();
    }
    if (options_.server_port == 0) {
        options_.server_port = RandomPort();
    }
    if (options_.proxy_port == 0) {
        options_.proxy_port = RandomPort();
    }
    if (options_.location.vregion().empty()) {
        options_.location = MockLocation();
    }

    FLAGS_metaserver_uri = options.host + ":" + std::to_string(options_.master_port);
    master_.reset(new MasteWrapper());
    master_->Init(options_);

    return;
}

Status MiniCluster::Start() {
    Status status = master_->Start();
    if (!status.ok()) {
        return status;
    }

    for (size_t i = 0; i < options_.server_count; ++i) {
        Status status = AddServer(options_.server_port + i * 3);
        if (!status.ok()) {
            LOG_ERROR("Server start fail").put("Status", status.ToString());
            return status;
        }
    }

    for (size_t i = 0; i < options_.proxy_count; ++i) {
        Status status = AddProxy(options_.proxy_port + i * 3);
        if (!status.ok()) {
            LOG_ERROR("Proxy start fail").put("Status", status.ToString());
            return status;
        }
    }

    return Status::OK();
}

Status MiniCluster::AddServer(uint32_t port) {
    Options server_options = options_;
    server_options.server_port = port;
    server::FLAGS_server_heartbeat_interval_ms = 100;
    FLAGS_server_stopping_wait_s = 0;

    std::string host_spec_path = options_.work_dir + "/spec_" + std::to_string(port) + ".json";
    Status status = InitHostSpec(host_spec_path, port, options_.location);
    if (!status.ok()) {
        return status;
    }
    std::unique_ptr<ServerWrapper> server(new ServerWrapper());
    server->Init(std::move(server_options));
    status = server->Start();
    if (!status.ok()) {
        return status;
    }

    server_map_.insert(std::make_pair(port, std::move(server)));
    bool y = false;
    for (int i = 0; i < 30; i++) {
        bthread_usleep(1000 * 1000);
        metaserver::ListServerRequest request;
        request.mutable_id()->set_cluster_name(options_.cluster_name);
        metaserver::ListServerResponse response;
        brpc::Controller cntl;
        GetMaster()->GetQueryStub()->ListServer(&cntl, &request, &response, nullptr);
        if (cntl.Failed() || response.status().code() != 0) {
            LOG_WARNING("query metaserver failed")
                .put("is_cntl_failed", cntl.Failed())
                .put("code", response.status().code());
            continue;
        }
        for (auto& server : response.servers()) {
            const auto& info = server.server_info();
            uint32_t p2 = info.endpoint().port();
            if (p2 == port) {
                server_map_[port]->SetId(info.id());
                y = true;
                break;
            }
        }
        if (y) {
            break;
        }
    }
    if (!y) {
        return Status::Internal("add server failed");
    }
    return Status::OK();
}

Status MiniCluster::AddProxy(uint32_t port) {
    FLAGS_heartbeat_interval_ms = 100;
    Options proxy_options = options_;
    proxy_options.proxy_port = port;
    std::unique_ptr<ProxyWrapper> proxy(new ProxyWrapper());
    proxy->Init(std::move(proxy_options));
    Status status = proxy->Start();
    if (!status.ok()) {
        return status;
    }
    proxy_map_.insert(std::make_pair(port, std::move(proxy)));

    bool y = false;
    for (int i = 0; i < 30; i++) {
        bthread_usleep(1000 * 1000);
        metaserver::ListProxyRequest request;
        request.mutable_id()->set_cluster_name(options_.cluster_name);
        metaserver::ListProxyResponse response;
        brpc::Controller cntl;
        GetMaster()->GetQueryStub()->ListProxy(&cntl, &request, &response, nullptr);
        if (cntl.Failed() || response.status().code() != 0) {
            LOG_WARNING("query metaserver failed")
                .put("is_cntl_failed", cntl.Failed())
                .put("code", response.status().code());
            continue;
        }
        for (auto& proxy : response.proxies()) {
            const auto& info = proxy.proxy_info();
            uint32_t p2 = info.endpoint().port();
            if (port == p2) {
                proxy_map_[port]->SetId(info.id());
                y = true;
                break;
            }
        }
        if (y) {
            break;
        }
    }
    if (!y) {
        return Status::Internal("add proxy failed");
    }
    return Status::OK();
}

Status MiniCluster::DropServer(uint32_t port) {
    auto iter = server_map_.find(port);
    if (iter == server_map_.end()) {
        return Status::Internal("");
    }
    iter->second->Stop();
    server_map_.erase(iter);
    return Status::OK();
}

Status MiniCluster::DropAllServer() {
    for (auto iter = server_map_.begin(); iter != server_map_.end(); ++iter) {
        Status status = master_->FreezeServer(iter->second->GetId());
        if (!status.ok()) {
            return status;
        }
        iter->second->Stop();
    }
    server_map_.clear();
    return Status::OK();
}

int MiniCluster::PickProxyPort() const {
    BYTE_ASSERT(!proxy_map_.empty());
    return proxy_map_.begin()->second->GetServerPort();
}

std::vector<ProxyWrapper*> MiniCluster::GetAllProxies() const {
    std::vector<ProxyWrapper*> result;
    for (auto& p : proxy_map_) {
        result.push_back(p.second.get());
    }
    return result;
}

void MiniCluster::Stop() {
    master_->Stop();

    for (auto iter = server_map_.begin(); iter != server_map_.end(); ++iter) {
        iter->second->Stop();
    }

    for (auto& p : proxy_map_) {
        p.second->Stop();
    }
    return;
}

MasteWrapper::MasteWrapper() {}

MasteWrapper::~MasteWrapper() {}

void MasteWrapper::Init(const MiniCluster::Options& options) {
    options_ = options;
    return;
}

Status MasteWrapper::Start() {
    metaserver::InitMetrics(
        "mini_cluster.metaserver",
        {{"cluster", options_.cluster_name}, {"port", std::to_string(options_.master_port)}});

    metaserver::FLAGS_metaserver_cluster_name = options_.cluster_name;
    metaserver::FLAGS_metaserver_server_port = options_.master_port;
    metaserver::FLAGS_metaserver_work_dir = options_.work_dir;
    metaserver::FLAGS_metaserver_log_dir = options_.work_dir;
    metaserver::FLAGS_metaserver_raft_peers =
        fmt::format("1,{}:{},{}:{},0", options_.host, options_.master_port + 10,  //
                    options_.host, options_.master_port + 20);
    metaserver::FLAGS_metaserver_proxy_calibrate_interval_ms = 100;
    metaserver::FLAGS_metaserver_raft_heartbeat_cycle_ms = 10;
    metaserver::FLAGS_metaserver_raft_election_cycle_ms = 20;

    server_.reset(new metaserver::MetaServer());
    Status status = server_->Init();
    RETURN_IF_STATUS_ERROR(status);

    status = server_->Start();
    RETURN_IF_STATUS_ERROR(status);

    endpoint_ = options_.host + ":" + std::to_string(options_.master_port);
    if (channel_.Init(endpoint_.c_str(), &channel_options_) != 0) {
        server_->Stop();
        return Status::Internal("Init Master Channel Failed");
    }
    manage_service_stub_.reset(new metaserver::ManageService_Stub(&channel_));
    query_service_stub_.reset(new metaserver::QueryService_Stub(&channel_));
    bool y = false;
    for (int i = 0; i < 20; i++) {
        bthread_usleep(1000 * 1000);
        metaserver::QueryLeaderRequest request;
        request.mutable_id()->set_cluster_name(options_.cluster_name);
        metaserver::QueryLeaderResponse response;
        brpc::Controller cntl;
        query_service_stub_->QueryLeader(&cntl, &request, &response, nullptr);
        if (cntl.Failed() || response.status().code() != 0) {
            LOG_WARNING("query metaserver failed")
                .put("is_cntl_failed", cntl.Failed())
                .put("code", response.status().code());
            continue;
        }
        if (response.is_leader()) {
            y = true;
            break;
        }
    }
    if (!y) {
        return Status::Internal("no master leader found");
    }
    return Status::OK();
}

void MasteWrapper::Stop() {
    server_->Stop();
    metaserver::QuitMetrics();
    return;
}

static void InitRequestId(const std::string& cluster_name, metaserver::RequestId* id) {
    id->set_timestamp(butil::gettimeofday_s());
    id->set_cluster_name(cluster_name);
    id->set_operator_name("mini_cluster");
}

Status MasteWrapper::CreateNamespace(const std::string& ns) {
    {
        metaserver::AddNamespaceRequest request;
        AckResponse response;
        brpc::Controller cntl;
        InitRequestId(options_.cluster_name, request.mutable_id());
        request.set_name(ns);
        manage_service_stub_->AddNamespace(&cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            return Status::Internal("rpc failed");
        }
        Status status = Status::FromRpcStatus(response.status());
        RETURN_IF_STATUS_ERROR(status);
    }
    {
        metaserver::PutProxyGroupRequest request;
        InitRequestId(options_.cluster_name, request.mutable_id());
        auto info = request.mutable_info();
        info->set_namespace_name(ns);
        *(info->mutable_placement()) = options_.location;
        info->set_instance_num(1);
        info->mutable_config()->add_consul_names("dev.bcache2_mini_cluster." + ns);
        AckResponse response;
        brpc::Controller cntl;
        manage_service_stub_->PutProxyGroup(&cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            return Status::Internal("rpc failed");
        }
        Status status = Status::FromRpcStatus(response.status());
        RETURN_IF_STATUS_ERROR(status);
    }
    return Status::OK();
}

Status MasteWrapper::CreateSimpleTable(const std::string& ns, const std::string& table_name,
                                       int pset_num, int pnum) {
    CreateNamespace(ns);

    metaserver::AddTableRequest request;
    AckResponse response;
    brpc::Controller cntl;

    InitRequestId(options_.cluster_name, request.mutable_id());
    request.set_namespace_name(ns);
    request.set_name(table_name);
    request.set_partition_set_num(pset_num);
    auto unit = request.add_partition_units();
    unit->set_partition_num(pnum);
    for (int i = 0; i < pnum; i++) {
        *(unit->add_placement_set()) = options_.location;
    }
    unit->set_storage_pool_uri(options_.cluster_uri);
    *(unit->mutable_primary_prefer()) = options_.location;
    request.mutable_quota()->set_ops_read(100000);

    manage_service_stub_->AddTable(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return Status::Internal("rpc failed");
    }
    Status status = Status::FromRpcStatus(response.status());
    RETURN_IF_STATUS_ERROR(status);

    for (int i = 0; i < 30; i++) {
        bthread_usleep(1000 * 1000);
        metaserver::ListTableRequest request;
        metaserver::ListTableResponse response;
        request.mutable_id()->set_cluster_name(options_.cluster_name);
        request.set_namespace_name(ns);
        request.set_table_name(table_name);
        brpc::Controller cntl;
        query_service_stub_->ListTable(&cntl, &request, &response, nullptr);
        if (cntl.Failed() || response.status().code() != 0) {
            LOG_WARNING("query metaserver failed")
                .put("is_cntl_failed", cntl.Failed())
                .put("code", response.status().code());
            continue;
        }
        const auto& table = response.tables(0);
        if (table.state() == TableState::TABLE_NORMAL) {
            return Status::OK();
        }
    }
    return Status::Internal("tale state wrong");
}

Status MasteWrapper::FreezeServer(const uint32_t server_id) {
    metaserver::FreezeServerRequest request;
    AckResponse response;
    brpc::Controller cntl;
    InitRequestId(options_.cluster_name, request.mutable_id());
    request.set_server_id(server_id);
    request.set_force(true);
    manage_service_stub_->FreezeServer(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return Status::Internal(cntl.ErrorText());
    }

    return Status::FromRpcStatus(response.status());
}

ServerWrapper::ServerWrapper() {}

ServerWrapper::~ServerWrapper() {}

void ServerWrapper::Init(const MiniCluster::Options& options) {
    options_ = options;
    return;
}

Status ServerWrapper::Start() {
    server::Server::Options options;
    options.service_thread_num = options_.server_thread_num;
    options.worker_thread_num = options_.server_worker_num;
    options.background_thread_num = 4;
    options.port = options_.server_port;
    options.host = options_.host;
    options.cluster_name = options_.cluster_name;
    server_.Init(options);

    Status status = server_.Start();
    if (!status.ok()) {
        LOG_ERROR("Server start fail").put("Status", status.ToString());
        return status;
    }

    endpoint_ = options_.host + ":" + std::to_string(options_.server_port);

    if (channel_.Init(endpoint_.c_str(), &channel_options_) != 0) {
        server_.Stop();
        return Status::Internal("Init Server Channel Failed");
    }
    server_service_stub_.reset(new bcache2::ServerService_Stub(&channel_));
    return Status::OK();
}

void ServerWrapper::Stop() { server_.Stop(); }

/////

ProxyWrapper::ProxyWrapper() {}

ProxyWrapper::~ProxyWrapper() {}

void ProxyWrapper::Init(const MiniCluster::Options& options) { options_ = options; }

Status ProxyWrapper::Start() {
    proxy::Proxy::Options options;
    options.cluster_name = options_.cluster_name;
    options.listen_port = options_.proxy_port;
    options.announce_port = options_.proxy_port;
    options.master_endpoint = options_.host + ":" + std::to_string(options_.master_port);
    options.idc = options_.location.vdc();
    options.location = options_.location;
    proxy_.reset(new proxy::Proxy());
    return proxy_->Start(options);
}

void ProxyWrapper::Stop() {
    proxy_->Stop();
    proxy_->Join();
}

uint32_t ProxyWrapper::GetServerPort() const { return proxy_->GetListenPort(); }

proxy::Proxy* ProxyWrapper::GetServer() const { return proxy_.get(); }

}  // namespace bcache2
