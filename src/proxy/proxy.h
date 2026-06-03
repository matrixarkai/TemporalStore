// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>
#include <string>
#include <unordered_set>

#include "brpc/server.h"
#include "butil/containers/doubly_buffered_data.h"

#include "client/client_impl.h"
#include "common/consul.h"
#include "common/logging.h"
#include "common/metaserver_tracker.h"
#include "protocol/info.pb.h"
#include "proxy/heartbeat.h"

namespace bcache2 {
namespace proxy {

class Proxy {
 public:
    struct Options {
        std::string cluster_name;
        int listen_port = 0;
        int announce_port = 0;
        std::string log_dir;
        byte::LogLevel log_level = byte::LOG_LEVEL_DEBUG;

        // TODO(wuzhenyu) replace with metaserver tracker
        std::string master_consul;
        std::string master_endpoint;

        std::string idc;

        Location location;
    };

    struct Config {
        std::string namespace_name;
        ProxyConfig config;  // pb
        std::unordered_set<std::string> consul_names;
    };

 public:
    Proxy() {}
    ~Proxy() {}

    Status Start(Options opts);
    void Stop();
    void Join();
    int GetAnnouncePort() const { return opts_.announce_port; }
    int GetListenPort() const { return server_.listen_address().port; }

    std::shared_ptr<MetaServerTracker> GetMetaServerTracker() { return metaserver_tracker_; }
    service_discovery::Consul* GetConsul() { return &consul_; }

    Config GetConfig();
    std::unordered_set<std::string> GetConsulNames();
    void UpdateConfig(std::string namespace_name, ProxyConfig pcfg);

 private:
    Status StartHeartbeat();

 private:
    std::unique_ptr<client::ClientImpl> client_;
    brpc::Server server_;
    std::shared_ptr<MetaServerTracker> metaserver_tracker_;
    std::unique_ptr<HeartBeat> heart_beat_{nullptr};
    butil::DoublyBufferedData<Config> config_;
    service_discovery::Consul consul_;

    Options opts_;

    DISALLOW_COPY_AND_ASSIGN(Proxy);
};

}  // namespace proxy
}  // namespace bcache2
