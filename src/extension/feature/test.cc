// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "client/light_client.h"
#include "extension/feature/interface.pb.h"
#include "extension/modules.pb.h"
#include "test/smoketest/base_smoketest.h"

DECLARE_uint64(feature_max_size);

namespace bcache2 {
namespace swig {

class FeatureModuleTest : public ::testing::Test {
 public:
    void SetUp() override {
        MiniCluster::Options options;
        options.server_count = 1;
        options.cluster_uri = "file://" + temp_dir_.GetDir() + "/cluster/public";
        cluster_.Init(options);
        bcache2::Status internal_status = cluster_.Start();
        ASSERT_TRUE(internal_status.ok());

        MasteWrapper* master = cluster_.GetMaster();

        master->CreateSimpleTable("ns", "table");
        ASSERT_TRUE(internal_status.ok());
    }

    void TearDown() override { cluster_.Stop(); }

 protected:
    TempDir temp_dir_;
    MiniCluster cluster_;
};

uint32_t MakeCmdId(int module_id, int function_id) {
    return (static_cast<uint32_t>(module_id) << 16) | static_cast<uint32_t>(function_id);
}

TEST_F(FeatureModuleTest, SimpleTest) {
    // New client
    Client* tmp_client = nullptr;
    Status status = Client::Create(ClientOptions(), &tmp_client);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Client> client(tmp_client);
    // Open table
    Table* tmp_table = nullptr;
    status = client->OpenTable(
        "tcp://127.0.0.1:" + std::to_string(cluster_.GetMaster()->GetMasterPort()) + "/ns/table",
        TableOptions(), &tmp_table);
    ASSERT_TRUE(status.ok()) << status.message();
    std::unique_ptr<Table> table_release_gurad(tmp_table);
    LightTable table(tmp_table);

    uint64_t start_ts = 10000;
    uint64_t points_num = 100;
    uint64_t end_ts = start_ts + points_num;

    {
        feature2::AddRequest request;
        request.set_key("key1");
        request.set_format("protobuf");
        for (uint64_t i = 0; i < points_num; i++) {
            auto pt = request.add_point_list();
            pt->set_ts(start_ts + i);
            pt->set_value(std::to_string(start_ts + i));
        }

        feature2::AddResponse response;
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(FEATURE, feature2::ADD), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);
    }

    // feature query
    {
        feature2::QueryRequest request;
        feature2::QueryResponse response;
        request.set_key("key1");
        request.set_start_ts(start_ts - 10);
        request.set_end_ts(end_ts + 3);
        request.set_count(points_num + 3);
        request.set_format("protobuf");
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(FEATURE, feature2::QUERY), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);
        ASSERT_EQ(response.point_list_size(), points_num);
        for (uint64_t i = 0; i < points_num; i++) {
            auto point = response.point_list(i);
            ASSERT_EQ(point.value(), std::to_string(start_ts + i));
            ASSERT_EQ(point.ts(), start_ts + i);
        }
    }
}

TEST_F(FeatureModuleTest, TruncateTest) {
    // New client
    Client* tmp_client = nullptr;
    Status status = Client::Create(ClientOptions(), &tmp_client);
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Client> client(tmp_client);
    // Open table
    Table* tmp_table = nullptr;
    status = client->OpenTable(
        "tcp://127.0.0.1:" + std::to_string(cluster_.GetMaster()->GetMasterPort()) + "/ns/table",
        TableOptions(), &tmp_table);
    ASSERT_TRUE(status.ok()) << status.message();
    std::unique_ptr<Table> table_release_gurad(tmp_table);
    LightTable table(tmp_table);

    {
        feature2::AddRequest request;
        request.set_key("key1");
        request.set_format("protobuf");
        for (uint64_t ts = 0; ts < 2 * FLAGS_feature_max_size; ts++) {
            auto pt = request.add_point_list();
            pt->set_ts(ts);
            pt->set_value(std::to_string(ts));
        }
        feature2::AddResponse response;
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(FEATURE, feature2::ADD), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);
    }

    {
        feature2::QueryRequest request;
        feature2::QueryResponse response;
        request.set_key("key1");
        request.set_start_ts(0);
        request.set_end_ts(UINT32_MAX);
        request.set_count(UINT32_MAX);
        request.set_format("protobuf");
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(FEATURE, feature2::QUERY), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);
        ASSERT_EQ(response.point_list_size(), FLAGS_feature_max_size);
    }
}

}  // namespace swig
}  // namespace bcache2
