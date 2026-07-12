// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <brpc/channel.h>
#include <byte/include/assert.h>
#include <byte/include/macros.h>
#include <gtest/gtest.h>
#include <stdio.h>
#include <string>

#include "client/client_impl.h"
#include "client/meta_syncer.h"
#include "client/neptune_syncer.h"
#include "client/server_pool.h"
#include "common/logging.h"
#include "protocol/feature_module.pb.h"
#include "protocol/master.pb.h"
#include "server/server.h"
#include "test/common/temp_dir.h"
#include "test/common/util.h"
#include "test/mini_cluster/mini_cluster.h"

DECLARE_uint64(feature_max_size);

namespace bcache2 {
DECLARE_string(metaserver_uri);
namespace client {

class ClientTest : public testing::Test {
 public:
    void SetUp() override {
        FLAGS_feature_max_size = UINT64_MAX;
        matrixobjectstore_init();
        bcache2::MiniCluster::Options options;
        options.work_dir = temp_dir.GetDir();
        options.server_count = 1;
        options.cluster_uri = "file://" + temp_dir.GetDir() + "/cluster/public";
        cluster_.Init(options);
        bcache2::Status internal_status = cluster_.Start();
        ASSERT_TRUE(internal_status.ok());

        bcache2::MasteWrapper* master = cluster_.GetMaster();

        master->CreateSimpleTable("hinata_ns", "hinata_table_name");
        ASSERT_TRUE(internal_status.ok());
        master_port = master->GetMasterPort();
        FLAGS_metaserver_uri = "127.0.0.1:" + std::to_string(master_port);
    }

    void TearDown() override {
        cluster_.Stop();
        matrixobjectstore_shutdown();
    }

    TempDir temp_dir;
    uint32_t master_port = 0;
    bcache2::MiniCluster cluster_;
};

TEST_F(ClientTest, client) {
    std::unique_ptr<Client> client;
    Client* temp_client;
    ClientOptions options;
    options.log_level = LogLevel::kAll;
    Status status;
    status = Client::Create(options, &temp_client);
    ASSERT_TRUE(!status.ok());

    options.master_consul = "toutiao.kv.configserver";
    status = Client::Create(options, &temp_client);
    ASSERT_TRUE(status.ok()) << status.ToString();
    client.reset(temp_client);

    options.master_addr = "127.0.0.1:" + std::to_string(master_port);
    options.master_consul.clear();
    status = Client::Create(options, &temp_client);
    ASSERT_TRUE(status.ok()) << status.ToString();
    client.reset(temp_client);

    std::unique_ptr<Table> table;
    std::unique_ptr<Pipeline> pipeline;
    TableOptions table_options;
    Table* temp_table;
    Pipeline* temp_pipeline;

    status = client->OpenTable("hinata_ns", "hinata_table_name", table_options, &temp_table);
    ASSERT_TRUE(status.ok()) << status.ToString();
    table.reset(temp_table);

    status = table->OpenPipeline(&temp_pipeline);
    ASSERT_TRUE(status.ok()) << status.ToString();
    pipeline.reset(temp_pipeline);

    // Test for client
    Controller ctlr;
    status = table->HSet("hinata_key", "hinata_field", "hinata_value");
    ASSERT_TRUE(status.ok()) << status.ToString();

    std::string value;
    status = table->HGet("hinata_key", "hinata_field", &value);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(value, "hinata_value");

    status = table->Set("hinata_key", "hinata_set_value");
    ASSERT_TRUE(status.IsUnmatched()) << status.ToString();

    status = table->Set("hinata_set_key", "hinata_set_value");
    ASSERT_TRUE(status.ok()) << status.ToString();

    status = table->Get("hinata_set_key", &value);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(value, "hinata_set_value");

    status = table->HDel("hinata_key", "hinata_field");
    ASSERT_TRUE(status.ok()) << status.ToString();
    status = table->HGet("hinata_key", "hinata_field", &value);
    ASSERT_TRUE(!status.ok()) << status.ToString();

    status = table->Set("hinata_set_key", "hinata_set_value");
    ASSERT_TRUE(status.ok()) << status.ToString();

    status = table->Get("hinata_set_key", &value);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(value, "hinata_set_value");

    status = table->SetEx("hinata_setex_key", "hinata_setex_value", 10 * 1000);
    ASSERT_TRUE(status.ok()) << status.ToString();

    status = table->Get("hinata_setex_key", &value);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_EQ(value, "hinata_setex_value");

    uint64_t ttl;
    status = table->Ttl("hinata_setex_key", &ttl);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_TRUE(ttl > 0 && ttl <= 10 * 1000) << ttl;

    status = table->Expire("hinata_setex_key", 5 * 1000);
    ASSERT_TRUE(status.ok()) << status.ToString();

    status = table->Ttl("hinata_setex_key", &ttl);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_TRUE(ttl > 0 && ttl <= 5 * 1000);

    status = table->Del("hinata_setex_key");
    ASSERT_TRUE(status.ok()) << status.ToString();

    status = table->Get("hinata_setex_key", &value);
    ASSERT_TRUE(!status.ok());

    // Test for pipeline
    std::vector<Status> statusArray;
    statusArray = pipeline->Sync();
    ASSERT_EQ(0, statusArray.size());

    pipeline->HSet("wuqi_key", "wuqi_field", "wuqi_value");
    statusArray = pipeline->Sync();
    ASSERT_EQ(1, statusArray.size());
    ASSERT_TRUE(statusArray[0].ok()) << statusArray[0].ToString();

    pipeline->HGet("wuqi_key", "wuqi_field", &value);
    statusArray = pipeline->Sync();
    ASSERT_EQ(1, statusArray.size());
    ASSERT_TRUE(statusArray[0].ok()) << statusArray[0].ToString();
    ASSERT_EQ("wuqi_value", value);

    pipeline->Set("wuqi_set_key", "wuqi_set_value");
    pipeline->Get("wuqi_set_key", &value);
    statusArray = pipeline->Sync();
    ASSERT_EQ(2, statusArray.size());
    ASSERT_TRUE(statusArray[0].ok()) << statusArray[0].ToString();
    ASSERT_TRUE(statusArray[1].ok()) << statusArray[1].ToString();
    ASSERT_EQ("wuqi_set_value", value);

    pipeline->HDel("wuqi_key", "wuqi_field");
    pipeline->HGet("wuqi_key", "wuqi_field", &value);
    statusArray = pipeline->Sync();
    ASSERT_EQ(2, statusArray.size());
    ASSERT_TRUE(statusArray[0].ok()) << statusArray[0].ToString();
    ASSERT_TRUE(!statusArray[1].ok()) << statusArray[1].ToString();
}

TEST_F(ClientTest, MetaSyncer) {
    Status status;
    ClientOptions opts;

    MetaSyncer::Options options;
    options.endpoint = "127.0.0.1:" + std::to_string(master_port);
    std::unique_ptr<MetaSyncer> meta_syncer(new MetaSyncer(options));
    status = meta_syncer->Init();
    ASSERT_TRUE(status.ok()) << status.ToString();

    NeptuneSyncer::Options neptune_options;
    neptune_options.timer_interval_ms = 1000 * 60;
    std::unique_ptr<NeptuneSyncer> neptune_syncer(new NeptuneSyncer(neptune_options,
        &opts));
    status = neptune_syncer->Init();
    ASSERT_TRUE(status.ok()) << status.ToString();

    TableOptions table_options;
    std::unique_ptr<TableImpl> table_impl(
        new TableImpl("hinata_ns", "hinata_table_name", table_options,
                &opts, meta_syncer.get(), neptune_syncer.get()));
    status = meta_syncer->OpenTable(table_impl.get());
    ASSERT_TRUE(status.ok()) << status.ToString();

    sleep(11);
    status = meta_syncer->StandaloneMode(table_impl.get());
    ASSERT_TRUE(status.ok()) << status.ToString();

    sleep(1);
    status = meta_syncer->CloseTable(table_impl.get());
    ASSERT_TRUE(status.ok()) << status.ToString();
}

class MasterMockService : public MasterService {
 public:
    void GetTableTopo(google::protobuf::RpcController* ctrl, const GetTableTopoRequest* request,
                      GetTableTopoResponse* response, google::protobuf::Closure* done) override {
        BYTE_DEFER({ done->Run(); });
        if (cntl_faild) {
            ctrl->SetFailed("test cntl failed");
        }
        response->mutable_status()->set_code(code);
        response->set_redirect_endpoint(redirect_endpoint);
        response->set_topo_version(topo_version);
    }
    void reset() {
        code = Code::kOK;
        redirect_endpoint.clear();
        topo_version = 1;
        cntl_faild = false;
    }
    Code code{Code::kOK};
    std::string redirect_endpoint;
    uint64_t topo_version{1};
    bool cntl_faild{false};
};

class MasterMockTest : public testing::Test {
 public:
    void SetUp() override {
        service.reset(new MasterMockService());
        server.reset(new brpc::Server());
        ASSERT_EQ(server->AddService(service.get(), brpc::SERVER_DOESNT_OWN_SERVICE), 0);

        brpc::ServerOptions options;
        ASSERT_EQ(server->Start(0, &options), 0);
        master_port = server->listen_address().port;
    }

    void TearDown() override {
        server->Stop(0);
        server->Join();
    }

    uint32_t master_port = 0;
    std::unique_ptr<MasterMockService> service;
    std::unique_ptr<brpc::Server> server;
};

TEST_F(MasterMockTest, MockMaster) {
    Status status;
    std::unique_ptr<Client> client;
    Client* temp_client;
    ClientOptions options;
    options.log_level = LogLevel::kAll;
    options.master_addr = "127.0.0.1:" + std::to_string(master_port);
    status = Client::Create(options, &temp_client);
    ASSERT_TRUE(status.ok()) << status.ToString();
    client.reset(temp_client);

    TableOptions table_options;
    Table* temp_table;

    service->cntl_faild = true;
    status = client->OpenTable("hinata_ns", "hinata_table_name", table_options, &temp_table);
    ASSERT_TRUE(!status.ok());
    service->reset();

    service->code = Code::kCancelled;
    status = client->OpenTable("hinata_ns", "hinata_table_name", table_options, &temp_table);
    ASSERT_TRUE(!status.ok());
    service->reset();

    service->redirect_endpoint = "127.0.0.1:" + std::to_string(master_port);
    status = client->OpenTable("hinata_ns", "hinata_table_name", table_options, &temp_table);
    ASSERT_TRUE(!status.ok());
    service->reset();

    service->topo_version = 0;
    status = client->OpenTable("hinata_ns", "hinata_table_name", table_options, &temp_table);
    ASSERT_TRUE(status.ok());
    service->reset();
    std::unique_ptr<Table> table;
    table.reset(temp_table);
    status = client->CloseTable(table.get());
    ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace client
}  // namespace bcache2
