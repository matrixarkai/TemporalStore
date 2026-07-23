// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "server/server.h"

#include <brpc/channel.h>
#include <brpc/server.h>
#include <byte/include/assert.h>
#include <byte/include/macros.h>
#include <gtest/gtest.h>

#include <mutex>
#include <regex>

#include "partition/storage/evicter.h"
#include "partition/storage/object_manager.h"
#include "partition/storage/slot_context_manager.h"
#include "partition/storage/storage_manager.h"
#include "protocol/master.pb.h"
#include "protocol/metaserver.pb.h"
#include "protocol/server.pb.h"
#include "test/common/temp_dir.h"
#include "test/common/util.h"

namespace bcache2 {
DECLARE_string(metaserver_uri);
namespace server {
DECLARE_uint64(server_meta_tinker_interval_ms);
namespace test {

class MockMetaServerQueryService : public metaserver::QueryService {
 public:
    void QueryLeader(google::protobuf::RpcController* controller,
                     const metaserver::QueryLeaderRequest* request,
                     metaserver::QueryLeaderResponse* response, google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        response->set_is_leader(true);
    }

    void ListServerPartition(google::protobuf::RpcController* controller,
                             const metaserver::ListServerPartitionRequest* request,
                             metaserver::ListServerPartitionResponse* response,
                             google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);

        std::lock_guard<std::mutex> _(mu);
        *response->mutable_server_info() = server_info;
        auto nps = response->add_node_partitions();
        for (const auto& p : partitions) {
            *nps->add_partitions() = p;
        }
    }

    std::mutex mu;
    ServerInfo server_info;
    std::vector<metaserver::ListServerPartitionResponse_Partition> partitions;
};

}  // namespace test

class ServerTest : public testing::Test {
 public:
    void SetUp() override {
        StartMetaServer();

        bytestore_init();

        Start();
        Load();

        std::string endpoint = "127.0.0.1:" + std::to_string(server_port_);
        brpc::ChannelOptions channel_options;
        channel_options.protocol = "h2:grpc";
        channel_options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
        BYTE_ASSERT(channel_.Init(endpoint.c_str(), &channel_options) == 0);
        server_stub_.reset(new ServerService_Stub(&channel_));

        brpc::ChannelOptions redis_channel_options;
        redis_channel_options.protocol = brpc::PROTOCOL_REDIS;
        redis_channel_options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
        BYTE_ASSERT(redis_channel_.Init(endpoint.c_str(), &redis_channel_options) == 0);
    }

    void TearDown() override {
        Stop();
        bytestore_shutdown();
        StopMetaServer();
    }

    void Start() {
        BYTE_ASSERT(server_.get() == nullptr);

        tmp_dir_.reset(new TempDir());
        server_port_ = RandomPort();
        InitHostSpec(tmp_dir_->GetDir() + "/host_spec_", server_port_);

        FLAGS_enable_blockcache = true;
        FLAGS_blockcache_dram_capacity = 134217728;  // 128 MB
        FLAGS_blockcache_ssd_capacity = 134217728;   // 128 MB

        server::Server::Options server_options;
        server_options.service_thread_num = 8;
        server_options.worker_thread_num = 4;
        server_options.background_thread_num = 4;
        server_options.host = "127.0.0.1";
        server_options.port = server_port_;

        server_.reset(new Server());
        server_->Init(server_options);
        Status status = server_->Start();
        BYTE_ASSERT(status.ok());
    }

    void Stop() {
        BYTE_ASSERT(server_.get() != nullptr);
        server_->Stop();
        server_.reset(nullptr);
    }

    void Restart() {
        Stop();
        Start();
    }

    void Load() {
        BYTE_ASSERT(!loaded_);
        std::unique_ptr<LoadRequest> request(new LoadRequest());
        std::unique_ptr<LoadResponse> response(new LoadResponse());
        std::unique_ptr<Controller> ctrl(new Controller());
        request->set_load_version(1);
        request->set_partition_id(1);
        request->set_partition_uri("file://" + tmp_dir_->GetDir() + "/cluster/public/partition");
        request->set_start_slot(0);
        request->set_end_slot(100);
        request->set_sync(true);
        request->mutable_config()->mutable_evicter_config()->mutable_maxmemory()->set_value(1000);
        SYNC_CALL(server_->partition_manager_->Load, ctrl.get(), request.get(), response.get());
        ASSERT_EQ(Code::kOK, response->status().code());
        loaded_ = true;
    }

    void LoadWithRedis() {
        BYTE_ASSERT(!loaded_);
        BYTE_ASSERT(Exec("partition load 1 1 file://" + tmp_dir_->GetDir() +
                         "/cluster/public/partition 0 100 master")
                        ->reply(0)
                        .data() == "OK");
        loaded_ = true;
    }

    void Unload() {
        BYTE_ASSERT(loaded_);
        std::unique_ptr<UnloadRequest> request(new UnloadRequest());
        std::unique_ptr<UnloadResponse> response(new UnloadResponse());
        std::unique_ptr<Controller> ctrl(new Controller());
        request->set_partition_id(1);
        SYNC_CALL(server_->partition_manager_->Unload, ctrl.get(), request.get(), response.get());
        tmp_dir_.reset(new TempDir());
        loaded_ = false;
    }

    void UnloadWithRedis() {
        BYTE_ASSERT(loaded_);
        BYTE_ASSERT(Exec("partition unload 1")->reply(0).data() == "OK");
        tmp_dir_.reset(new TempDir());
        loaded_ = false;
    }

    void Reload() {
        Unload();
        Load();
    }

    std::unique_ptr<brpc::RedisResponse> Exec(const std::string& command) {
        brpc::RedisRequest request;
        request.AddCommand(command);
        brpc::RedisResponse* response = new brpc::RedisResponse();
        brpc::Controller cntl;
        redis_channel_.CallMethod(NULL, &cntl, &request, response, NULL);
        return std::unique_ptr<brpc::RedisResponse>(response);
    }

    void StartMetaServer() {
        ms_port_ = RandomPort();
        FLAGS_metaserver_uri = "127.0.0.1:" + std::to_string(ms_port_);
        FLAGS_server_meta_tinker_interval_ms = 100;
        ms_query_service_.reset(new test::MockMetaServerQueryService());
        metaserver::ListServerPartitionResponse_Partition p;
        p.set_id(1);
        p.set_state(PartitionState::P_NORMAL);
        ms_query_service_->partitions.push_back(p);

        ms_server_.reset(new brpc::Server());
        ms_server_->AddService(ms_query_service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE);
        brpc::ServerOptions options;
        ms_server_->Start(ms_port_, &options);
    }

    void StopMetaServer() {
        ms_server_->Stop(0);
        ms_server_->Join();
    }

    uint32_t server_port_ = 0;
    std::unique_ptr<Server> server_ = nullptr;
    std::unique_ptr<TempDir> tmp_dir_ = nullptr;
    std::unique_ptr<ServerService_Stub> server_stub_ = nullptr;
    int ms_port_ = 0;
    std::unique_ptr<brpc::Server> ms_server_ = nullptr;
    std::unique_ptr<test::MockMetaServerQueryService> ms_query_service_ = nullptr;
    brpc::Channel channel_;
    brpc::Channel redis_channel_;
    bool loaded_ = false;
};

TEST_F(ServerTest, PartitionConfig) {
    PartitionManager* partition_manager = server_->partition_manager_.get();
    partition::Partition* partition = partition_manager->thread_infos_[1].partition_map[1].get();
    ASSERT_EQ(partition->options_.config.evicter_config().maxmemory().value(), 1000ul);

    {
        SetConfigRequest set_request;
        SetConfigResponse set_response;
        Controller set_ctrl;
        set_request.set_partition_id(1);
        Config* cfg = set_request.mutable_config();
        cfg->mutable_evicter_config()->mutable_maxmemory()->set_value(3000);
        cfg->mutable_extend_config()->insert({"test_config", "test_value"});
        cfg->set_version(partition->GetConfig().version() + 1);
        SYNC_CALL(partition_manager->SetConfig, &set_ctrl, &set_request, &set_response);
        ASSERT_EQ(partition->options_.config.evicter_config().maxmemory().value(), 3000ul);
        ASSERT_EQ(partition->evicter_->config_.maxmemory().value(), 3000ul);
        ASSERT_EQ(partition->options_.config.extend_config().at("test_config"), "test_value");

        GetConfigRequest get_request;
        GetConfigResponse get_response;
        Controller get_ctrl;
        get_request.set_partition_id(1);
        SYNC_CALL(partition_manager->GetConfig, &get_ctrl, &get_request, &get_response);
        ASSERT_EQ(get_response.status().code(), Code::kOK);
    }

    {
        SetConfigRequest set_request;
        SetConfigResponse set_response;
        brpc::Controller set_ctrl;
        set_request.set_partition_id(1);
        set_request.mutable_config()->mutable_evicter_config()->mutable_maxmemory()->set_value(
            2000);
        set_request.mutable_config()->mutable_extend_config()->insert(
            {"test_config", "test_new_value"});
        set_request.mutable_config()->set_version(partition->GetConfig().version() + 1);
        server_stub_->SetConfig(&set_ctrl, &set_request, &set_response, NULL);
        ASSERT_EQ(partition->options_.config.evicter_config().maxmemory().value(), 2000ul);
        ASSERT_EQ(partition->evicter_->config_.maxmemory().value(), 2000ul);
        ASSERT_EQ(partition->options_.config.extend_config().at("test_config"), "test_new_value");

        GetConfigRequest get_request;
        GetConfigResponse get_response;
        brpc::Controller get_ctrl;
        get_request.set_partition_id(1);
        server_stub_->GetConfig(&get_ctrl, &get_request, &get_response, NULL);
        ASSERT_EQ(get_response.status().code(), Code::kOK);
    }

    Reload();
}

TEST_F(ServerTest, MetaTinkerTest) {
    {
        metaserver::ListServerPartitionResponse_Partition p;
        p.set_id(10000);  // not exists in local
        p.set_state(PartitionState::P_NORMAL);
        ms_query_service_->partitions.push_back(p);
    }

    PartitionManager* partition_manager = server_->partition_manager_.get();
    partition::Partition* partition = partition_manager->thread_infos_[1].partition_map[1].get();
    metaserver::ListServerPartitionResponse_Partition& p = ms_query_service_->partitions[0];
    *p.mutable_config() = partition->GetConfig();
    const std::string cfg_key = "test_config";
    for (int i = 0; i < 3; i++) {
        const std::string cfg_v = std::to_string(i);
        {
            std::lock_guard<std::mutex> _(ms_query_service_->mu);
            p.mutable_config()->mutable_extend_config()->erase(cfg_key);
            p.mutable_config()->mutable_extend_config()->insert({cfg_key, cfg_v});
            p.mutable_config()->set_version(p.config().version() + 1);
        }
        for (int j = 0; j < 10; j++) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            if (partition->GetConfig().extend_config().at(cfg_key) == cfg_v) {
                break;
            }
        }
        ASSERT_EQ(partition->GetConfig().extend_config().at(cfg_key), cfg_v)
            << partition->GetConfig().ShortDebugString();
    }

    Reload();
}

TEST_F(ServerTest, CommandTest) {
    ASSERT_EQ(Exec("ping")->reply(0).data(), "PONG");

    {
        ASSERT_EQ(Exec("slaveof 127.0.0.1 9999")->reply(0).data(), "OK");
        std::smatch match;
        std::string str = Exec("info")->reply(0).data().as_string();
        ASSERT_TRUE(std::regex_search(str, match, std::regex(R"(role:(\w+))")));
        ASSERT_EQ(match[1], "slave");
    }

    {
        ASSERT_EQ(Exec("slaveof no one")->reply(0).data(), "OK");
        std::smatch match;
        std::string str = Exec("info")->reply(0).data().as_string();
        ASSERT_TRUE(std::regex_search(str, match, std::regex(R"(role:(\w+))")));
        ASSERT_EQ(match[1], "master");
    }

    Restart();
}

}  // namespace server
}  // namespace bcache2
