// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <brpc/server.h>
#include <byte/include/macros.h>
#include <byte/thread/waiter.h>

#include <memory>
#include <string>

#include "blockcache/blockcache.h"
#include "common/metaserver_tracker.h"
#include "common/operator_tool.h"
#include "protocol/host_spec.pb.h"
#include "protocol/master.pb.h"
#include "server/redis_service.h"
#include "stream/log_based_env.h"

namespace bcache2 {
namespace server {

class PartitionManager;
class ServiceImpl;
class Heartbeat;
class MetaTinker;

class Server {
 public:
    struct Options {
        int service_thread_num = 8;
        int worker_thread_num = 4;
        int background_thread_num = 4;
        std::string host;
        std::string host_v6;
        int port = 0;
        int log_level = 0;
        int log_max_file_num = 10;
        uint64_t log_max_file_size = 1 * 1024 * 1024 * 1024;
        std::string log_dir = "./";

        std::string master_consul;    // deprecated
        std::string master_endpoint;  // deprecated
        std::string table_name;       // deprecated

        std::string requirepass;
        int heart_beat_interval = 1000 * 1000 * 10;
        int metrics_update_interval = 1000 * 1000 * 10;

        std::string cluster_name;
    };

    Server();
    virtual ~Server();

    void Init(const Options& options);
    Status Start();
    std::string GetHost() const { return options_.host; }
    std::string GetHostV6() const { return options_.host_v6; }
    int GetListenPort() const { return server_->listen_address().port; }
    void Stop();

    std::shared_ptr<MetaServerTracker> GetMetaServerTracker() { return metaserver_tracker_; }

 private:
    Status SanitizeHostSpec();

 private:
    Options options_;
    HostSpec host_spec_;
    bool stop_ = true;

    std::unique_ptr<byte::AsyncThreadPool> worker_thread_pool_;
    std::unique_ptr<byte::AsyncThreadPool> background_thread_pool_;
    std::unique_ptr<stream::StoreLayer> store_layer_;
    std::unique_ptr<stream::LogBasedEnv> env_;
    std::unique_ptr<PartitionManager> partition_manager_;
    std::unique_ptr<ServiceImpl> service_;
    std::unique_ptr<brpc::Server> server_;
    std::unique_ptr<bcache2::blockcache::BlockCache> blockcache_{nullptr};
    std::shared_ptr<MetricsEnv> metrics_env_;

    std::shared_ptr<MetaServerTracker> metaserver_tracker_;  // shared is ok, unique is better
    std::unique_ptr<Heartbeat> heartbeat_;
    std::unique_ptr<MetaTinker> meta_tinker_;

    RedisServiceImpl* redis_service_ = nullptr;

    DISALLOW_COPY_AND_ASSIGN(Server);
};

}  // namespace server
}  // namespace bcache2
