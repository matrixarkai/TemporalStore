// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "client/swig_client.h"

#include <gtest/gtest.h>

#include "test/common/temp_dir.h"
#include "test/mini_cluster/mini_cluster.h"

namespace bcache2 {
namespace swig {

class SwigClientTest : public ::testing::Test {
 public:
    SwigClientTest() {}
    virtual ~SwigClientTest() {}

    void SetUp() override {
        matrixobjectstore_init();

        MiniCluster::Options options;
        options.server_count = 1;
        options.cluster_uri = "file://" + temp_dir_.GetDir() + "/cluster/public";
        cluster_.Init(options);
        bcache2::Status internal_status = cluster_.Start();
        ASSERT_TRUE(internal_status.ok());

        MasteWrapper* master = cluster_.GetMaster();

        master->CreateSimpleTable("ns", "table");
        ASSERT_TRUE(internal_status.ok());

        ClientOptions client_options;
        client_options.idc = "vdc";
        client_options.log_level = LogLevel::kDebug;
        // client_options.log_console = true;
        Client* temp_client = nullptr;
        Status status = Client::Create(client_options, &temp_client);
        ASSERT_TRUE(status.ok());
        client_.reset(temp_client);

        TableOptions table_options;
        Table* temp_table = nullptr;
        status = client_->OpenTable(
            "tcp://127.0.0.1:" + std::to_string(cluster_.GetMaster()->GetMasterPort()) +
                "/ns/table",
            table_options, &temp_table);
        ASSERT_TRUE(status.ok());
        table_.reset(temp_table);
    }

    void TearDown() override {
        cluster_.Stop();
        matrixobjectstore_shutdown();
    }

 protected:
    TempDir temp_dir_;
    MiniCluster cluster_;

    std::unique_ptr<Client> client_;
    std::unique_ptr<Table> table_;
};

TEST_F(SwigClientTest, DISABLED_Simple) {
    {
        CmdRequest request;
        auto set_request = request.mutable_hash_request()->mutable_set_request();
        set_request->set_key("key1");
        set_request->set_field("field1");
        set_request->set_value("value1");
        std::string request_bytes;
        ASSERT_TRUE(request.SerializeToString(&request_bytes));
        Execution execution;
        execution.cmd = static_cast<int>(OpType::kOpHashSet);
        execution.request = Bytes(request_bytes.data(), request_bytes.size());
        execution.partition_key = Bytes("key1", 4);
        std::vector<Execution> executions = {std::move(execution)};
        Controller ctrl;
        table_->BatchExecute(&ctrl, &executions);
        ASSERT_EQ(ctrl.status.code(), 0);

        CmdResponse response;
        ASSERT_EQ(executions[0].status.code(), 0);
        ASSERT_TRUE(
            response.ParseFromArray(executions[0].response.data(), executions[0].response.size()));
        ASSERT_EQ(response.status().code(), 0);
        ASSERT_EQ(response.response_status().code(), 0);
    }

    {
        CmdRequest request;
        auto get_request = request.mutable_hash_request()->mutable_get_request();
        get_request->set_key("key1");
        get_request->set_field("field1");
        std::string request_bytes;
        ASSERT_TRUE(request.SerializeToString(&request_bytes));
        Execution execution;
        execution.cmd = static_cast<int>(OpType::kOpHashGet);
        execution.request = Bytes(request_bytes.data(), request_bytes.size());
        execution.partition_key = Bytes("key1", 4);
        std::vector<Execution> executions = {std::move(execution)};
        Controller ctrl;
        table_->BatchExecute(&ctrl, &executions);
        ASSERT_EQ(ctrl.status.code(), 0);

        CmdResponse response;
        ASSERT_EQ(executions[0].status.code(), 0);
        ASSERT_TRUE(
            response.ParseFromArray(executions[0].response.data(), executions[0].response.size()));
        ASSERT_EQ(response.status().code(), 0);
        ASSERT_EQ(response.response_status().code(), 0);
        ASSERT_EQ(response.hash_response().get_response().value(), "value1");
    }

    std::cout << "done";
}

}  // namespace swig
}  // namespace bcache2
