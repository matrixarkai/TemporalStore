// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/macros.h>
#include <gtest/gtest.h>

#include <memory>

#include "client/client.h"
#include "test/common/temp_dir.h"
#include "test/common/util.h"
#include "test/mini_cluster/mini_cluster.h"

namespace bcache2 {

class BaseSmoketest : public ::testing::Test {
 public:
    BaseSmoketest() {}
    virtual ~BaseSmoketest() {}

    void SetUp() override {
        matrixobjectstore_init();

        MiniCluster::Options options;
        options.server_count = 1;
        options.cluster_uri = "file://" + temp_dir_.GetDir() + "/cluster/public";
        cluster_.Init(options);
        Status status = cluster_.Start();
        ASSERT_TRUE(status.ok());

        MasteWrapper* master = cluster_.GetMaster();

        name_space_ = "hinata_ns";
        table_name_ = "hinata_table_name";

        master->CreateSimpleTable(name_space_, table_name_);
        ASSERT_TRUE(status.ok());

        client_options_.master_addr =
            "127.0.0.1:" + std::to_string(cluster_.GetMaster()->GetMasterPort());
        client::Client* temp_client;
        status = client::Client::Create(client_options_, &temp_client);
        ASSERT_TRUE(status.ok());
        client_.reset(temp_client);

        table_options_.io_timeout_ms = 1000;
        table_options_.connect_timeout_ms = 1000;
        client::Table* temp_table;
        status = client_->OpenTable(name_space_, table_name_, table_options_, &temp_table);
        ASSERT_TRUE(status.ok());
        table_.reset(temp_table);

        test_keys_.emplace_back("hinata_key");
    }

    void TearDown() override {
        cluster_.Stop();
        matrixobjectstore_shutdown();
    }

    void ReloadServer() {
        Status status = cluster_.DropAllServer();
        ASSERT_TRUE(status.ok()) << status.ToString();
        int new_server_port = RandomPort();
        status = cluster_.AddServer(new_server_port);
        ASSERT_TRUE(status.ok()) << status.ToString();
        bthread_usleep(5 * 1000 * 1000);  // wait for auto register

        client::Client* temp_client;
        status = client::Client::Create(client_options_, &temp_client);
        ASSERT_TRUE(status.ok());
        client_.reset(temp_client);

        client::Table* temp_table;
        status = client_->OpenTable(name_space_, table_name_, table_options_, &temp_table);
        ASSERT_TRUE(status.ok()) << status;
        table_.reset(temp_table);
    }

 protected:
    MiniCluster cluster_;
    client::ClientOptions client_options_;
    std::unique_ptr<client::Client> client_;
    client::TableOptions table_options_;
    std::unique_ptr<client::Table> table_;
    TempDir temp_dir_;

    std::string name_space_;
    std::string table_name_;
    std::vector<std::string> test_keys_;

    DISALLOW_COPY_AND_ASSIGN(BaseSmoketest);
};

}  // namespace bcache2
