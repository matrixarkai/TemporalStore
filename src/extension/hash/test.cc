// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "client/light_client.h"
#include "extension/hash/interface.pb.h"
#include "extension/modules.pb.h"
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
}

TEST_F(HashModuleTest, IncrBy) {
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

    {
        // simple test
        {
            hash2::IncrByRequest request;
            request.set_key("key1");
            request.set_field("field1");
            request.set_increment(1);


            hash2::IncrByResponse response;
            Controller ctrl;
            table.Execute(&ctrl, MakeCmdId(HASH, hash2::INCRBY), "key1", request, &response);
            ASSERT_EQ(ctrl.status.code(), 0);
            ASSERT_EQ(response.value(), 1);
        }
        {
            hash2::IncrByRequest request;
            request.set_key("key1");
            request.set_field("field1");
            request.set_increment(2);


            hash2::IncrByResponse response;
            Controller ctrl;
            table.Execute(&ctrl, MakeCmdId(HASH, hash2::INCRBY), "key1", request, &response);
            ASSERT_EQ(ctrl.status.code(), 0);
            ASSERT_EQ(response.value(), 3);
        }
    }
    {
        // hash value is not an integer
        {
            hash2::MSetRequest request;
            request.set_key("key1");
            request.add_fields("pure_alphabet");
            request.add_values("abc");
            request.add_fields("numerical_alphabet");
            request.add_values("123abc");

            hash2::MSetResponse response;
            Controller ctrl;
            table.Execute(&ctrl, MakeCmdId(HASH, hash2::MSET), "key1", request, &response);
            ASSERT_EQ(ctrl.status.code(), 0);
        }
        {
           hash2::IncrByRequest request;
           request.set_key("key1");
           request.set_field("pure_alphabet");
           request.set_increment(1);

           hash2::IncrByResponse response;
           Controller ctrl;
           table.Execute(&ctrl, MakeCmdId(HASH, hash2::INCRBY), "key1", request, &response);
           ASSERT_EQ(ctrl.status.code(), kUnmatched);
           ASSERT_EQ(response.value(), 0);
        }
        {
           hash2::IncrByRequest request;
           request.set_key("key1");
           request.set_field("numerical_alphabet");
           request.set_increment(1);

           hash2::IncrByResponse response;
           Controller ctrl;
           table.Execute(&ctrl, MakeCmdId(HASH, hash2::INCRBY), "key1", request, &response);
           ASSERT_EQ(ctrl.status.code(), kUnmatched);
           ASSERT_EQ(response.value(), 0);
        }
    }
    {
        // hash value is out of range
        {
            hash2::MSetRequest request;
            request.set_key("key1");
            request.add_fields("out_of_range");
            request.add_values(std::to_string(ULLONG_MAX));

            hash2::MSetResponse response;
            Controller ctrl;
            table.Execute(&ctrl, MakeCmdId(HASH, hash2::MSET), "key1", request, &response);
            ASSERT_EQ(ctrl.status.code(), 0);
        }
        {
               hash2::IncrByRequest request;
               request.set_key("key1");
               request.set_field("out_of_range");
               request.set_increment(1);

               hash2::IncrByResponse response;
               Controller ctrl;
               table.Execute(&ctrl, MakeCmdId(HASH, hash2::INCRBY), "key1", request, &response);
               ASSERT_EQ(ctrl.status.code(), kUnmatched);
               ASSERT_EQ(response.value(), 0);
        }
    }
    {
       // increment would overflow
       {
            hash2::IncrByRequest request;
            request.set_key("key1");
            request.set_field("increment_would_overflow");
            request.set_increment(LLONG_MAX);

            hash2::IncrByResponse response;
            Controller ctrl;
            table.Execute(&ctrl, MakeCmdId(HASH, hash2::INCRBY), "key1", request, &response);
            ASSERT_EQ(ctrl.status.code(), 0);
            ASSERT_EQ(response.value(), LLONG_MAX);
       }
       {
            hash2::IncrByRequest request;
            request.set_key("key1");
            request.set_field("increment_would_overflow");
            request.set_increment(1);

            hash2::IncrByResponse response;
            Controller ctrl;
            table.Execute(&ctrl, MakeCmdId(HASH, hash2::INCRBY), "key1", request, &response);
            ASSERT_EQ(ctrl.status.code(), kOutOfRange);
            ASSERT_EQ(response.value(), 0);
       }
    }
    {
        // decrement would overflow
        {
            hash2::IncrByRequest request;
            request.set_key("key1");
            request.set_field("decrement_would_overflow");
            request.set_increment(LLONG_MIN);

            hash2::IncrByResponse response;
            Controller ctrl;
            table.Execute(&ctrl, MakeCmdId(HASH, hash2::INCRBY), "key1", request, &response);
            ASSERT_EQ(ctrl.status.code(), 0);
            ASSERT_EQ(response.value(), LLONG_MIN);
        }
        {
            hash2::IncrByRequest request;
            request.set_key("key1");
            request.set_field("decrement_would_overflow");
            request.set_increment(-1);

            hash2::IncrByResponse response;
            Controller ctrl;
            table.Execute(&ctrl, MakeCmdId(HASH, hash2::INCRBY), "key1", request, &response);
            ASSERT_EQ(ctrl.status.code(), kOutOfRange);
            ASSERT_EQ(response.value(), 0);
        }
    }
}

TEST_F(HashModuleTest, HMGet) {
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

    {
        // key not found
        hash2::MGetRequest request;
        request.set_key("key1");
        *request.add_fields() = "field1";
        *request.add_fields() = "field2";

        hash2::MGetResponse response;
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(HASH, hash2::MGET), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), kNotFound);
    }

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

    {
        hash2::SetRequest request;
        request.set_key("key1");
        request.set_field("field2");
        request.set_value("value2");

        hash2::SetResponse response;
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(HASH, hash2::SET), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);
    }

    {
        // all fields not exist
        hash2::MGetRequest request;
        request.set_key("key1");
        *request.add_fields() = "field100";
        *request.add_fields() = "field200";

        hash2::MGetResponse response;
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(HASH, hash2::MGET), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);

        ASSERT_EQ(response.exists_size(), 2);
        ASSERT_EQ(response.values_size(), 2);
        for (int i = 0; i < response.exists_size(); ++i) {
            ASSERT_FALSE(response.exists(i));
        }
    }

    {
        // some fields exist
        hash2::MGetRequest request;
        request.set_key("key1");
        *request.add_fields() = "field100";
        *request.add_fields() = "field1";
        *request.add_fields() = "field200";

        hash2::MGetResponse response;
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(HASH, hash2::MGET), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);

        ASSERT_EQ(response.exists_size(), 3);
        ASSERT_EQ(response.values_size(), 3);
        for (int i = 0; i < response.exists_size(); ++i) {
            if (i == 1) {
                ASSERT_TRUE(response.exists(i));
                ASSERT_EQ(response.values(i), "value1");
            } else {
                ASSERT_FALSE(response.exists(i));
            }
        }
    }

    {
        // all fields exist
        hash2::MGetRequest request;
        request.set_key("key1");
        *request.add_fields() = "field1";
        *request.add_fields() = "field2";

        hash2::MGetResponse response;
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(HASH, hash2::MGET), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);

        ASSERT_EQ(response.exists_size(), 2);
        ASSERT_EQ(response.values_size(), 2);
        ASSERT_TRUE(response.exists(0));
        ASSERT_EQ(response.values(0), "value1");
        ASSERT_TRUE(response.exists(1));
        ASSERT_EQ(response.values(1), "value2");
    }
}

TEST_F(HashModuleTest, HMSet) {
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

    {
        // fields and values not match
        hash2::MSetRequest request;
        request.set_key("key1");
        *request.add_fields() = "field1";
        *request.add_values() = "value1";
        *request.add_values() = "value2";

        hash2::MSetResponse response;
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(HASH, hash2::MSET), "key1", request, &response);
        ASSERT_FALSE(ctrl.status.ok());
    }

    {
        // hmset
        hash2::MSetRequest request;
        request.set_key("key1");
        *request.add_fields() = "field111";
        *request.add_fields() = "field222";
        *request.add_values() = "value111";
        *request.add_values() = "value222";

        hash2::MSetResponse response;
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(HASH, hash2::MSET), "key1", request, &response);
        ASSERT_TRUE(ctrl.status.ok()) << ctrl.status.message();
    }

    hash2::GetAllRequest request;
    request.set_key("key1");

    hash2::GetAllResponse response;
    Controller ctrl;
    table.Execute(&ctrl, MakeCmdId(HASH, hash2::GETALL), "key1", request, &response);
    ASSERT_EQ(ctrl.status.code(), 0);

    ASSERT_EQ(response.fields_size(), 2);
    ASSERT_EQ(response.values_size(), 2);
    ASSERT_EQ(response.fields(0), "field111");
    ASSERT_EQ(response.values(0), "value111");
    ASSERT_EQ(response.fields(1), "field222");
    ASSERT_EQ(response.values(1), "value222");
}

TEST_F(HashModuleTest, HGetall) {
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

    {
        // empty
        hash2::GetAllRequest request;
        request.set_key("key1");

        hash2::GetAllResponse response;
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(HASH, hash2::GETALL), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), kNotFound);

        ASSERT_EQ(response.fields_size(), 0);
        ASSERT_EQ(response.values_size(), 0);
    }

    {
        // hmset
        hash2::MSetRequest request;
        request.set_key("key1");
        *request.add_fields() = "field111";
        *request.add_fields() = "field222";
        *request.add_values() = "value111";
        *request.add_values() = "value222";

        hash2::MSetResponse response;
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(HASH, hash2::MSET), "key1", request, &response);
        ASSERT_TRUE(ctrl.status.ok()) << ctrl.status.message();
    }

    {
        // hgetall
        hash2::GetAllRequest request;
        request.set_key("key1");

        hash2::GetAllResponse response;
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(HASH, hash2::GETALL), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0);

        ASSERT_EQ(response.fields_size(), 2);
        ASSERT_EQ(response.values_size(), 2);
        ASSERT_EQ(response.fields(0), "field111");
        ASSERT_EQ(response.values(0), "value111");
        ASSERT_EQ(response.fields(1), "field222");
        ASSERT_EQ(response.values(1), "value222");
    }
    {
        // hlen
        hash2::LenRequest request;
        request.set_key("key1");

        hash2::LenResponse response;
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(HASH, hash2::LEN), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), 0) << ctrl.status.message();

        ASSERT_EQ(response.len(), 2);
    }

    {
        // hdel
        hash2::DelRequest request;
        request.set_key("key1");
        request.set_field("field111");

        hash2::DelResponse response;
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(HASH, hash2::DEL), "key1", request, &response);
        ASSERT_TRUE(ctrl.status.ok()) << ctrl.status.message();
    }
    {
        // hdel
        hash2::DelRequest request;
        request.set_key("key1");
        request.set_field("field222");

        hash2::DelResponse response;
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(HASH, hash2::DEL), "key1", request, &response);
        ASSERT_TRUE(ctrl.status.ok()) << ctrl.status.message();
    }

    {
        // hgetall
        hash2::GetAllRequest request;
        request.set_key("key1");

        hash2::GetAllResponse response;
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(HASH, hash2::GETALL), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), kNotFound);
    }
    {
        // hlen
        hash2::LenRequest request;
        request.set_key("key1");

        hash2::LenResponse response;
        Controller ctrl;
        table.Execute(&ctrl, MakeCmdId(HASH, hash2::LEN), "key1", request, &response);
        ASSERT_EQ(ctrl.status.code(), kNotFound);
    }
}

}  // namespace swig
}  // namespace bcache2
