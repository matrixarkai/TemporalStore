// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "client/light_client.h"
#include "extension/modules.pb.h"
#include "extension/string/interface.pb.h"
#include "test/smoketest/base_smoketest.h"

namespace bcache2 {
namespace swig {

class StringModuleTest : public ::testing::Test {
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

TEST_F(StringModuleTest, SimpleTest) {
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

    // string set
    {
        str2::SetRequest request;
        request.set_key("key1");
        request.set_value("field1");
        str2::SetResponse response;

        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(STRING, str2::SET), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);
    }

    // string get
    {
        str2::GetRequest request;
        request.set_key("key1");

        str2::GetResponse response;

        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(STRING, str2::GET), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);
        ASSERT_EQ(response.value(), "field1");
    }

    // setnx
    {
        str2::SetRequest sexnx_req;
        sexnx_req.set_key("key1");
        sexnx_req.set_value("field2");
        sexnx_req.set_nx_flag(true);
        str2::SetResponse sexnx_rsp;
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(STRING, str2::SET), sexnx_req.key(), sexnx_req, &sexnx_rsp);
        ASSERT_EQ(ctrl.status.code(), 6);  // AlreadyExists

        sexnx_req.set_key("key2");
        table.Execute(&ctrl, MakeCmdId(STRING, str2::SET), sexnx_req.key(), sexnx_req, &sexnx_rsp);
        ASSERT_EQ(ctrl.status.code(), 0);

        str2::GetRequest get_req;
        get_req.set_key("key2");
        str2::GetResponse get_rsp;
        table.Execute(&ctrl, MakeCmdId(STRING, str2::GET), get_req.key(), get_req, &get_rsp);
        ASSERT_EQ(ctrl.status.code(), 0);
        ASSERT_EQ(get_rsp.value(), "field2");
    }

    // setxx
    {
        str2::SetRequest setxx_req;
        setxx_req.set_key("key1");
        setxx_req.set_value("field3");
        setxx_req.set_xx_flag(true);
        str2::SetResponse setxx_rsp;
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(STRING, str2::SET), setxx_req.key(), setxx_req, &setxx_rsp);
        ASSERT_EQ(ctrl.status.code(), 0);

        str2::GetRequest get_req;
        get_req.set_key("key1");
        str2::GetResponse get_rsp;
        table.Execute(&ctrl, MakeCmdId(STRING, str2::GET), get_req.key(), get_req, &get_rsp);
        ASSERT_EQ(ctrl.status.code(), 0);
        ASSERT_EQ(get_rsp.value(), "field3");

        setxx_req.set_key("key_not_exist");
        table.Execute(&ctrl, MakeCmdId(STRING, str2::SET), setxx_req.key(), setxx_req, &setxx_rsp);
        ASSERT_EQ(ctrl.status.code(), 5);  // NotFound
    }

    // both nx_flag and xx_flag
    {
        str2::SetRequest request;
        request.set_key("key1");
        request.set_value("field1");
        request.set_nx_flag(true);
        request.set_xx_flag(true);
        str2::SetResponse response;

        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(STRING, str2::SET), request.key(), request, &response);
        ASSERT_EQ(ctrl.status.code(), 3);  // InvalidArgument
    }
}

}  // namespace swig
}  // namespace bcache2
