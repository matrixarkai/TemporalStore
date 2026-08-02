// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <brpc/channel.h>
#include <byte/include/macros.h>
#include <stdint.h>

#include <string>
#include <vector>

#include "common/status.h"
#include "protocol/master.pb.h"
#include "protocol/server.pb.h"
#include "server/server.h"
#include "test/common/temp_dir.h"

namespace bcache2 {

namespace metaserver {
class MetaServer;
}
namespace proxy {
class Proxy;
}

class ProxyWrapper;
class ServerWrapper;
class MasteWrapper;

class MiniCluster {
 public:
    struct Options {
        std::string cluster_name;

        std::string work_dir;
        std::string cluster_uri;
        std::string host = "127.0.0.1";

        uint32_t master_port = 0;
        uint32_t server_port = 0;
        uint32_t proxy_port = 0;

        uint32_t server_count = 1;
        uint32_t proxy_count = 1;
        uint32_t server_thread_num = 8;
        uint32_t server_worker_num = 8;

        Location location;
    };

    MiniCluster();
    virtual ~MiniCluster();

    void Init(const Options& options);
    Status Start();
    void Stop();

    std::string GetClusterAddress() const {
        return options_.host + ":" + std::to_string(options_.master_port);
    };

    MasteWrapper* GetMaster() const { return master_.get(); }
    ServerWrapper* GetServer(uint32_t port) const {
        auto iter = server_map_.find(port);
        if (iter == server_map_.end()) {
            return nullptr;
        }
        return iter->second.get();
    }

    ServerWrapper* GetFirstServer() const { return server_map_.begin()->second.get(); }

    Status AddServer(uint32_t port);
    Status AddProxy(uint32_t port);
    Status DropServer(uint32_t port);
    Status DropAllServer();

    int PickProxyPort() const;
    std::vector<ProxyWrapper*> GetAllProxies() const;

 private:
    Options options_;
    std::unique_ptr<TempDir> temp_dir_;
    std::unique_ptr<MasteWrapper> master_;
    std::unordered_map<uint32_t, std::unique_ptr<ServerWrapper>> server_map_;
    std::unordered_map<uint32_t, std::unique_ptr<ProxyWrapper>> proxy_map_;

    DISALLOW_COPY_AND_ASSIGN(MiniCluster);
};

class ProxyWrapper {
 public:
    ProxyWrapper();
    virtual ~ProxyWrapper();

    void Init(const MiniCluster::Options& options);
    Status Start();
    void Stop();
    uint32_t GetServerPort() const;
    proxy::Proxy* GetServer() const;

    uint32_t GetId() const { return id_; }
    void SetId(uint32_t id) { id_ = id; }

 private:
    MiniCluster::Options options_;
    std::unique_ptr<proxy::Proxy> proxy_;
    uint32_t id_{0};

    DISALLOW_COPY_AND_ASSIGN(ProxyWrapper);
};

class ServerWrapper {
 public:
    ServerWrapper();
    virtual ~ServerWrapper();

    void Init(const MiniCluster::Options& options);
    Status Start();
    void Stop();
    uint32_t GetServerPort() const { return server_.GetListenPort(); }

    uint32_t GetId() const { return id_; }
    void SetId(uint32_t id) { id_ = id; }

 private:
    MiniCluster::Options options_;

    brpc::Channel channel_;
    brpc::ChannelOptions channel_options_;
    std::unique_ptr<bcache2::ServerService_Stub> server_service_stub_;

    std::string endpoint_;
    server::Server server_;
    uint32_t id_{0};

    DISALLOW_COPY_AND_ASSIGN(ServerWrapper);
};

class MasteWrapper {  // why is there no 'r'?
 public:
    MasteWrapper();
    virtual ~MasteWrapper();

    void Init(const MiniCluster::Options& options);
    Status Start();
    void Stop();

    uint32_t GetMasterPort() { return options_.master_port; };
    std::string GetEndpoint() const { return endpoint_; }

    metaserver::ManageService_Stub* GetManageStub() { return manage_service_stub_.get(); }
    metaserver::QueryService_Stub* GetQueryStub() { return query_service_stub_.get(); }

    Status CreateNamespace(const std::string& ns);
    Status CreateSimpleTable(const std::string& ns, const std::string& table_name,
                             int partition_set_num = 1, int partition_num = 1);
    Status FreezeServer(uint32_t server_id);

 private:
    MiniCluster::Options options_;

    brpc::Channel channel_;
    brpc::ChannelOptions channel_options_;
    std::unique_ptr<metaserver::ManageService_Stub> manage_service_stub_;
    std::unique_ptr<metaserver::QueryService_Stub> query_service_stub_;

    std::string endpoint_;
    std::unique_ptr<metaserver::MetaServer> server_;

    DISALLOW_COPY_AND_ASSIGN(MasteWrapper);
};
}  // namespace bcache2
