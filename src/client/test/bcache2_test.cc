// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "client/bcache2.h"

#include <assert.h>
#include <gtest/gtest.h>
#include <stdio.h>
#include <string.h>

#include "common/coclosure.h"
#include "protocol/server.pb.h"
#include "test/common/temp_dir.h"
#include "test/mini_cluster/mini_cluster.h"

class BCache2Test : public ::testing::Test {
 public:
    BCache2Test() {}
    virtual ~BCache2Test() {}

    void SetUp() override {
        matrixobjectstore_init();

        bcache2::MiniCluster::Options options;
        options.work_dir = temp_dir_.GetDir();
        options.server_count = 1;
        options.cluster_uri = "file://" + temp_dir_.GetDir() + "/cluster/public";
        cluster_.Init(options);
        bcache2::Status internal_status = cluster_.Start();
        ASSERT_TRUE(internal_status.ok());

        bcache2::MasteWrapper* master = cluster_.GetMaster();

        master->CreateSimpleTable("ns", "table");
        ASSERT_TRUE(internal_status.ok());
    }

    void TearDown() override {
        cluster_.Stop();
        matrixobjectstore_shutdown();
    }

 protected:
    static void OnExecuteDone(void* args) {
        Closure<void>* done = reinterpret_cast<Closure<void>*>(args);
        done->Run();
    }

    bcache2::TempDir temp_dir_;
    bcache2::MiniCluster cluster_;
};

TEST_F(BCache2Test, SimpleTest) {
    // init client
    bcache2_options_t* bcache2_options = bcache2_options_init();
    bcache2_options_set(bcache2_options, "idc", "vdc");
    bcache2_init(bcache2_options);
    bcache2_options_destory(bcache2_options);

    // open table
    bcache2_table_t* table = NULL;
    bcache2_table_options_t* table_options = bcache2_tableoptions_init();
    int code = bcache2_open((std::string("tcp://127.0.0.1:") +
                             std::to_string(cluster_.GetMaster()->GetMasterPort()) + "/ns/table")
                                .c_str(),
                            table_options, &table);
    ASSERT_EQ(code, BCACHE2_OK);
    bcache2_tableoptions_destory(table_options);

    // write
    {
        bcache2::hash::SetRequest request;
        request.set_key("key1");
        request.set_field("field1");
        request.set_value("value1");
        std::string request_bytes;
        ASSERT_TRUE(request.SerializeToString(&request_bytes));

        bcache2_execution_t* execution = bcache2_execution_init(0, 0);
        const char* key = "key1";
        data_t partition_key = {key, strlen(key)};
        data_t request_data = {request_bytes.data(), request_bytes.size()};
        bcache2_execution_add_request(execution, (11 << 16) | 0, partition_key, request_data);
        bcache2_execute(table, execution, NULL, NULL);
        code = bcache2_execution_get_status(execution, 0);
        printf("errorcode=%d\n", code);
        ASSERT_EQ(code, BCACHE2_OK);
        bcache2_execution_destory(execution);
    }

    {
        bcache2::hash::GetRequest request;
        request.set_key("key1");
        request.set_field("field1");
        std::string request_bytes;
        ASSERT_TRUE(request.SerializeToString(&request_bytes));

        bcache2_execution_t* execution = bcache2_execution_init(0, 0);
        const char* key = "key1";
        data_t partition_key = {key, strlen(key)};
        data_t request_data = {request_bytes.data(), request_bytes.size()};
        bcache2_execution_add_request(execution, (11 << 16) | 1, partition_key, request_data);
        bcache2::CoSyncClosure sync;
        bcache2_execute(table, execution, OnExecuteDone, &sync);
        sync.Wait();
        code = bcache2_execution_get_status(execution, 0);
        printf("errorcode=%d\n", code);
        ASSERT_EQ(code, BCACHE2_OK);
        data_t response_data = bcache2_execution_get_response(execution, 0);

        bcache2::hash::GetResponse response;
        ASSERT_TRUE(response.ParseFromArray(response_data.data, response_data.size));
        EXPECT_STREQ(response.value().c_str(), "value1");
        bcache2_execution_destory(execution);
    }

    // close table
    bcache2_close(table);

    // destory client
    bcache2_destory();
}
