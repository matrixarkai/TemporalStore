// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <byte/include/macros.h>
#include <gtest/gtest.h>

#include <iostream>
#include <memory>

#include "model/hash_model.h"
#include "partition/storage/object_manager.h"
#include "protocol/server.pb.h"
#include "test/smoketest/base_smoketest.h"

namespace bcache2 {

class BasicSmoketest : public BaseSmoketest {
 public:
    BasicSmoketest() {}
    ~BasicSmoketest() {}

    void StringTest(const std::string& key) {
        std::string set_value = "test_value";
        std::string get_value;
        Status status = table_->Set(key, set_value);
        ASSERT_TRUE(status.ok()) << status;

        status = table_->Get(key, &get_value);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(get_value, set_value);

        set_value = "test_value_setex";
        status = table_->SetEx(key, set_value, 10 * 1000);
        ASSERT_TRUE(status.ok());

        status = table_->Get(key, &get_value);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(get_value, set_value);

        ReloadServer();

        status = table_->Get(key, &get_value);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(get_value, set_value);

        uint64_t ttl = 0;
        status = table_->Ttl(key, &ttl);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(ttl > 0);
        ASSERT_TRUE(ttl <= 10 * 1000);

        status = table_->Expire(key, 1000);
        CoSleep(1000 * 2000);

        status = table_->Get(key, &get_value);
        ASSERT_TRUE(status.IsNotFound()) << status.ToString();

        ReloadServer();

        status = table_->Get(key, &get_value);
        ASSERT_TRUE(status.IsNotFound());

        set_value = "test_value_setex";
        status = table_->SetEx(key, set_value, 10 * 1000);
        ASSERT_TRUE(status.ok());

        table_->Del(key);
        ASSERT_TRUE(status.ok());

        status = table_->Get(key, &get_value);
        ASSERT_TRUE(status.IsNotFound());
    }

    void HashTest(const std::string& key) {
        std::string set_value = "test_value";
        std::string set_field = "test_field";
        std::string get_value;

        Status status = table_->HSet(key, set_field, set_value);
        ASSERT_TRUE(status.ok());

        status = table_->HGet(key, set_field, &get_value);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(get_value, set_value);

        status = table_->HDel(key, set_field);
        ASSERT_TRUE(status.ok());

        status = table_->HGet(key, set_field, &get_value);
        ASSERT_TRUE(status.IsNotFound());

        status = table_->Expire(key, 10 * 1000);
        ASSERT_TRUE(status.ok());

        set_value = "test_value_1";
        status = table_->HSet(key, set_field, set_value);
        ASSERT_TRUE(status.ok());

        ReloadServer();

        status = table_->HGet(key, set_field, &get_value);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(get_value, set_value);

        status = table_->Del(key);
        ASSERT_TRUE(status.ok());

        status = table_->HGet(key, set_field, &get_value);
        ASSERT_TRUE(status.IsNotFound());
    }

    DISALLOW_COPY_AND_ASSIGN(BasicSmoketest);
};

TEST_F(BasicSmoketest, BasicTest) {
    Status status;

    Controller ctlr;
    status = table_->HSet("hinata_key", "hinata_field", "hinata_value");
    ASSERT_TRUE(status.ok());

    std::string value;
    status = table_->HGet("hinata_key", "hinata_field", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, "hinata_value");

    status = table_->Set("hinata_set_key", "hinata_set_value");
    ASSERT_TRUE(status.ok());

    status = table_->Get("hinata_set_key", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, "hinata_set_value");
}

TEST_F(BasicSmoketest, ReloadTest) {
    Status status;

    Controller ctlr;
    status = table_->HSet("hinata_key", "hinata_field", "hinata_value");
    ASSERT_TRUE(status.ok());

    std::string value;
    status = table_->HGet("hinata_key", "hinata_field", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, "hinata_value");

    ReloadServer();

    status = table_->HGet("hinata_key", "hinata_field", &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, "hinata_value");
}

TEST_F(BasicSmoketest, ModuleTest) {
    for (size_t i = 0; i < test_keys_.size(); ++i) {
        StringTest(test_keys_[i]);
        HashTest(test_keys_[i]);
    }
}

}  // namespace bcache2
