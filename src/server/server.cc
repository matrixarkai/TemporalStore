// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "server/server.h"

#include <algorithm>
#include <set>
#include <string>
#include <utility>

#include "brpc/channel.h"
#include "butil/endpoint.h"
#include "butil/file_util.h"
#include "byte/base/closure.h"
#include "bytestore/bytestore.h"
#include "gflags/gflags.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/pb_to_json.h"

#include "common/coclosure.h"
#include "common/consul.h"
#include "common/function_closure.h"
#include "common/logging.h"
#include "common/operator_tool.h"
#include "common/proto_enhance.h"
#include "common/scoped_invoker.h"
#include "model/feature_model.h"
#include "model/hash_model.h"
#include "partition/storage/object_manager.h"
#include "protocol/base.pb.h"
#include "server/dashboard_service.h"
#include "server/heartbeat.h"
#include "server/meta_tinker.h"
#include "server/partition_manager.h"
#include "server/redis_command_handler.h"
#include "server/service.h"

DEFINE_uint64(server_stopping_wait_s, 60, "stopping wait time before actual shutdown");
BRPC_VALIDATE_GFLAG(server_stopping_wait_s, brpc::PassValidate);

namespace bcache2 {
namespace server {

// TODO(wuzhenyu) refactor 把gflag和options统一起来
DEFINE_string(host_spec_path, "./host_spec.json", "host specific file path");
DEFINE_string(cmdb_jwt_uri, "", "cmdb jwt uri");
DEFINE_string(cmdb_key, "", "cmdb api key");
DEFINE_string(cmdb_host, "", "cmdb host");
DEFINE_string(server_tag, "", "server tag");

Server::Server() {}

Server::~Server() { Stop(); }

void Server::Init(const Options& options) { options_ = options; }

Status Server::Start() {
    // Init worker thread pool
    byte::AsyncThreadPoolOptions worker_tp_options;
    worker_tp_options.name_ = "worker";  // for PartitionManager
    worker_tp_options.thread_num_ = options_.worker_thread_num;
    worker_thread_pool_.reset(new byte::AsyncThreadPool());
    if (!worker_thread_pool_->Init(worker_tp_options)) {
        return Status::Internal("Worker thread pool init failed");
    }
    if (!worker_thread_pool_->Start()) {
        return Status::Internal("Worker thread pool start failed");
    }

    // Init background thread pool
    byte::AsyncThreadPoolOptions background_tp_options;
    background_tp_options.name_ = "background";  // for StoreLayer, Metrics, StreamEnv
    background_tp_options.thread_num_ = options_.background_thread_num;
    background_thread_pool_.reset(new byte::AsyncThreadPool());
    if (!background_thread_pool_->Init(background_tp_options)) {
        return Status::Internal("Background thread pool init failed");
    }
    if (!background_thread_pool_->Start()) {
        return Status::Internal("Background thread pool start failed");
    }

    // Init server metrics env
    MetricsEnv::Options metrics_env_option;
    metrics_env_option.prefix = "bcache2.server";
    metrics_env_option.common_tags = {{"cluster", options_.cluster_name},
                                      {"port", std::to_string(options_.port)}};
    metrics_env_option.background_pool = background_thread_pool_.get();
    metrics_env_ = std::make_shared<MetricsEnv>();
    metrics_env_->Init(metrics_env_option);

    // Init store layer
    store_layer_.reset(new stream::StoreLayer(background_thread_pool_.get()));

    // Init env
    stream::LogBasedEnv::Options env_options;
    env_options.background_pool = background_thread_pool_.get();
    env_options.store_layer = store_layer_.get();
    env_.reset(new stream::LogBasedEnv());
    env_->Init(env_options);

    // Init blockcache
    if (FLAGS_enable_blockcache) {
        std::string key_list = "";
        std::string value_list = "";
        for (auto it : metrics_env_option.common_tags) {
            key_list += it.first + ";";
            value_list += it.second + ";";
        }
        key_list.pop_back();
        value_list.pop_back();
        FLAGS_blockcache_metric_tag_keys = key_list;
        FLAGS_blockcache_metric_tag_values = value_list;

        FLAGS_blockcache_metric_id_prefix = metrics_env_option.prefix;
        if (FLAGS_blockcache_ssd_path == "/opt/tiger/bcache2_data/data_cache_ssd") {
            FLAGS_blockcache_ssd_path =
                "/opt/tiger/bcache2_data/blockcache_ssd_" + std::to_string(options_.port);
        }
        blockcache_.reset(new bcache2::blockcache::BlockCache());
        Status blockcache_status = blockcache_->Start();
        if (!blockcache_status.ok()) {
            LOG_ERROR("failed to start blockcache")
                .put("metric_id_prefix", FLAGS_blockcache_metric_id_prefix)
                .put("status", blockcache_status);
            return blockcache_status;
        }
        LOG_INFO("Blockcache started")
            .put("metric_id_prefix", FLAGS_blockcache_metric_id_prefix)
            .put("ssd_path", FLAGS_blockcache_ssd_path)
            .put("ssd_capacity", FLAGS_blockcache_ssd_capacity)
            .put("dram_capacity", FLAGS_blockcache_dram_capacity);
    }

    LOG_INFO("starting metaserver tracker");
    metaserver_tracker_ = std::make_shared<MetaServerTracker>(options_.cluster_name);
    Status status = metaserver_tracker_->Start();
    if (!status.ok()) {
        // Note: ignore error here
        LOG_WARNING("failed to start metaserver tracker").put("result", status);
    }

    // Init partition manager
    partition_manager_.reset(new PartitionManager(options_.cluster_name, this,  //
                                                  worker_thread_pool_.get(), env_.get(),
                                                  blockcache_.get()));

    // Init Logger
    bytestore_set_flag("bytestore_log_dir", options_.log_dir.c_str());

    byte::SetByteLogDir(options_.log_dir);
    byte::SetByteLogFilePrefix(std::to_string(options_.port) + "_");
    byte::SetMinLogLevel(byte::LogLevel(options_.log_level));
    byte::SetByteLogMaxFileNum(options_.log_max_file_num);
    byte::SetByteLogMaxFileSize(options_.log_max_file_size);

    // Init service
    service_.reset(new ServiceImpl(partition_manager_.get()));
    redis_service_ = new RedisServiceImpl(partition_manager_.get());
    redis_service_->SetConfig("requirepass", options_.requirepass);
    redis_service_->InitCommands();

    // Init server
    server_.reset(new brpc::Server());
    if (server_->AddService(service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        return Status::Internal("Server add service failed");
    }
    if (server_->AddService(new DashboardServiceImpl(options_.cluster_name, options_.port),
                            brpc::SERVER_OWNS_SERVICE) != 0) {
        return Status::Internal("Server add service failed");
    }

    // Start server
    brpc::ServerOptions options;
    options.num_threads = options_.service_thread_num;
    options.redis_service = redis_service_;
    if (server_->Start(options_.port, &options) != 0) {
        return Status::Internal("Server Start failed");
    }
    LOG_INFO("sanitize host spec");
    status = SanitizeHostSpec();
    if (!status.ok()) {
        LOG_WARNING("sanitize host spec failed").put("result", status);
        return status;
    }

    LOG_INFO("starting heartbeat routine");
    heartbeat_.reset(new Heartbeat(options_.cluster_name, host_spec_, metaserver_tracker_,
                                   partition_manager_.get()));
    status = heartbeat_->Start();
    if (!status.ok()) {
        LOG_WARNING("failed to start heartbeat").put("result", status);
        return status;
    }

    LOG_INFO("starting metat_inker");
    meta_tinker_.reset(new MetaTinker(options_.cluster_name, host_spec_, metaserver_tracker_,
                                      partition_manager_.get()));
    status = meta_tinker_->Start();
    if (!status.ok()) {
        LOG_WARNING("start meta_tinker failed").put("result", status);
        return status;
    }

    stop_ = false;

    LOG_INFO("Start server ok").put("ServiceThreadNum", options_.service_thread_num);
    return Status::OK();
}

Status Server::SanitizeHostSpec() {
    std::string json_data;
    std::string err_msg;
    HostSpec* spec = &host_spec_;
    const std::string path = FLAGS_host_spec_path;
    LOG_INFO("loading host spec").put("path", path);
    butil::FilePath fp = butil::FilePath(path);
    // 1. load
    if (butil::PathExists(fp)) {
        if (!butil::ReadFileToString(fp, &json_data)) {
            return Status::Internal("read spec file failed");
        }

        if (!json2pb::JsonToProtoMessage(json_data, spec, &err_msg)) {
            return Status::Internal(err_msg);
        }
    }

    // 2. sanitize
    auto endpoint = spec->mutable_endpoint();
    if (endpoint->ip4().empty()) {
        const char* host = getenv("BYTED_HOST_IP");
        if (host != nullptr) {
            endpoint->set_ip4(host);
        }
    }
    if (endpoint->ip6().empty()) {
        const char* host_v6 = getenv("BYTED_HOST_IPV6");
        if (host_v6 != nullptr) {
            endpoint->set_ip6(host_v6);
        }
    }
    if (endpoint->ip4().empty() && endpoint->ip6().empty()) {
        return Status::Internal("ip4/ip6 are all empty");
    } else if (endpoint->ip4().empty()) {
        endpoint->set_addr_family(Endpoint::ADDR_V6);
    } else if (endpoint->ip6().empty()) {
        endpoint->set_addr_family(Endpoint::ADDR_V4);
    } else {
        endpoint->set_addr_family(Endpoint::ADDR_DUAL_STACK);
    }
    if (endpoint->port() == 0) {
        char* env_port = getenv("PORT0");
        if (env_port != nullptr) {
            LOG_WARNING("ENV PORT0 is not null, force to replace")
                .put("opt", options_.port)
                .put("env", env_port);
            endpoint->set_port(atoi(env_port));
        } else {
            endpoint->set_port(options_.port);
        }
    }
    if (!Validate(spec->location())) {
        LOG_INFO("location is invalid").put("v", spec->location().ShortDebugString());
        if (!FLAGS_cmdb_host.empty()) {
            operator_tool::CMDBClient cmdb_client(FLAGS_cmdb_host, FLAGS_cmdb_jwt_uri,
                                                  FLAGS_cmdb_key);
            Status status = cmdb_client.QueryHostLocation(
                endpoint->ip4().empty() ? endpoint->ip6() : endpoint->ip4(),
                spec->mutable_location());
            if (!status.ok()) {
                return status;
            }
            if (!FLAGS_server_tag.empty()) {
                spec->mutable_location()->set_tag(FLAGS_server_tag);
            }
        } else {
            return Status::Internal("location is invalid");
        }
    }
    if (spec->numa_nodes_size() == 0) {
        spec->set_set_cpu_affinity(false);
        // make a virtual node, TODO(wuzhenyu) detect and auto fill
        auto node = spec->add_numa_nodes();
        node->set_cpu_list("-");
        node->set_memory_size_mb(1);
    }
    spec->set_updated_at(butil::gettimeofday_s());

    // 3. save
    struct json2pb::Pb2JsonOptions options;
    options.pretty_json = true;
    options.enum_option = json2pb::EnumOption::OUTPUT_ENUM_BY_NUMBER;
    json_data.clear();
    err_msg.clear();
    if (!json2pb::ProtoMessageToJson(*spec, &json_data, options, &err_msg)) {
        return Status::Internal(err_msg);
    }
    butil::FilePath tmp_fp(path + ".tmp");
    int rc = butil::WriteFile(tmp_fp, json_data.data(), json_data.size());
    if (rc < 0) {
        return Status::Internal("write file failed");
    }
    if (!butil::Move(tmp_fp, fp)) {
        return Status::Internal("rename file failed");
    }
    return Status::OK();
}

void Server::Stop() {
    if (stop_) {
        return;
    }
    stop_ = true;

    partition_manager_->SetStopping();
    std::this_thread::sleep_for(std::chrono::seconds(FLAGS_server_stopping_wait_s));

    if (heartbeat_) {
        LOG_INFO("stopping heartbeat");
        heartbeat_->Stop();
    }
    if (meta_tinker_) {
        LOG_INFO("stopping meta_tinker");
        meta_tinker_->Stop();
    }
    LOG_INFO("stopping server");
    server_->Stop(0);
    server_->Join();
    LOG_INFO("unloading all");
    partition_manager_->UnloadAll();

    LOG_INFO("stopping reset all");
    metrics_env_->Stop();

    for (int i = 0; i < background_thread_pool_->ThreadNum(); ++i) {
        CoSyncClosure sync;
        background_thread_pool_->KthThread(i)->Invoke(&sync);
        sync.Wait();
    }
    background_thread_pool_->Stop();

    if (FLAGS_enable_blockcache && blockcache_) {
        blockcache_->Stop();
    }

}

}  // namespace server
}  // namespace bcache2
