// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "client/light_client.h"
#include "extension/modules.pb.h"
#include "extension/set/interface.pb.h"
#include "test/smoketest/base_smoketest.h"

namespace bcache2 {
namespace swig {

class HashModuleTest : public ::testing::Test {
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

TEST_F(HashModuleTest, SimpleTest) {
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
    ASSERT_TRUE(status.ok());
    std::unique_ptr<Table> table_release_gurad(tmp_table);
    LightTable table(tmp_table);

    // Hash set
    {
        set::SAddRequest request;
        request.set_key("key1");
        request.add_members("field1");

        set::SAddResponse response;

        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(SET, set::SADD), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);
        ASSERT_EQ(response.added(), 1);
    }

    // Hash get
    {
        set::SMembersRequest request;
        request.set_key("key1");

        set::SMembersResponse response;

        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(SET, set::SMEMBERS), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);
        ASSERT_EQ(response.members_size(), 1);
    }

    // Set cardinality.
    {
        set::SCardRequest request;
        request.set_key("key1");

        set::SCardResponse response;

        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(SET, set::SCARD), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);
        ASSERT_EQ(response.len(), 1);
    }

    // Set membership.
    {
        set::SIsMemberRequest request;
        request.set_key("key1");
        request.set_member("field1");

        set::SIsMemberResponse response;

        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(SET, set::SISMEMBER), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);
        ASSERT_TRUE(response.exist());
    }

    // Set remove.
    {
        set::SRemRequest request;
        request.set_key("key1");
        request.add_members("field1");

        set::SRemResponse response;

        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(SET, set::SREM), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);
        ASSERT_EQ(response.removed(), 1);
    }
}

}  // namespace swig
}  // namespace bcache2
