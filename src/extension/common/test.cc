// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "client/light_client.h"
#include "extension/common/interface.pb.h"
#include "extension/hash/interface.pb.h"
#include "extension/modules.pb.h"
#include "extension/string/interface.pb.h"
#include "test/smoketest/base_smoketest.h"

namespace bcache2 {
namespace swig {

class CommonModuleTest : public ::testing::Test {
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

TEST_F(CommonModuleTest, SimpleTest) {
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
        hash2::SetRequest request;
        request.set_key("key1");
        request.set_field("field1");
        request.set_value("value1");

        hash2::SetResponse response;

        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(HASH, hash2::SET), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);
    }

    // Hash get
    {
        hash2::GetRequest request;
        request.set_key("key1");
        request.set_field("field1");

        hash2::GetResponse response;

        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(HASH, hash2::GET), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);
        ASSERT_EQ(response.value(), "value1");
    }

    // del object
    {
        common2::DelObjectRequest request;
        common2::DelObjectResponse response;
        request.set_key("key1");

        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(COMMON, common2::DEL_OBJECT), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);
    }

    // Hash get
    {
        hash2::GetRequest request;
        request.set_key("key1");
        request.set_field("field1");

        hash2::GetResponse response;

        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(HASH, hash2::GET), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), kNotFound);
    }

    // Hash set
    {
        hash2::SetRequest request;
        request.set_key("key1");
        request.set_field("field1");
        request.set_value("value1");

        hash2::SetResponse response;

        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(HASH, hash2::SET), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);
    }

    // Ttl
    {
        common2::TtlRequest request;
        request.set_key("key1");
        common2::TtlResponse response;

        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(COMMON, common2::TTL), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);
    }

    // Expire
    {
        common2::ExpireRequest request;
        request.set_key("key1");
        request.set_ttl_ms(1000);
        common2::ExpireResponse response;
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(COMMON, common2::EXPIRE), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);
    }

    // Ttl string setex
    {
        str2::SetexRequest request;
        str2::SetexResponse response;

        request.set_key("key_test");
        request.set_value("key_value");
        request.set_ttl_ms(3 * 1000);
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(STRING, str2::SETEX), "key_test", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);

        CoSleep(1 * 1000 * 1000);
        str2::GetRequest get_request;
        str2::GetResponse get_response;
        get_request.set_key("key_test");
        table.Execute(&ctrl, MakeCmdId(STRING, str2::GET), "key_test", get_request, &get_response);
        ASSERT_EQ(ctrl.status.code(), 0);
        ASSERT_EQ(get_response.value(), "key_value");

        CoSleep(3 * 1000 * 1000);
        get_request.set_key("key_test");
        table.Execute(&ctrl, MakeCmdId(STRING, str2::GET), "key_test", get_request, &get_response);
        ASSERT_EQ(ctrl.status.code(), kNotFound);
    }
}

}  // namespace swig
}  // namespace bcache2
