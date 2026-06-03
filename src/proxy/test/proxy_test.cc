// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <brpc/channel.h>
#include <brpc/thrift_message.h>
#include <gtest/gtest.h>

#include "proxy/flags.h"
#include "test/common/temp_dir.h"
#include "test/mini_cluster/mini_cluster.h"
#include "thrift/server_types.h"

namespace bcache2 {
namespace proxy {
namespace test {

class ProxyTest : public ::testing::Test {
 public:
    void SetUp() override {
        MiniCluster::Options options;
        options.server_count = 1;
        options.proxy_count = 1;
        options.cluster_uri = "file://" + temp_dir_.GetDir() + "/cluster/public";
        cluster_.Init(options);
        Status status = cluster_.Start();
        ASSERT_TRUE(status.ok()) << status;

        MasteWrapper* master = cluster_.GetMaster();

        master->CreateSimpleTable("ns", "table");
        ASSERT_TRUE(status.ok()) << status;

        {
            brpc::ChannelOptions options;
            options.protocol = brpc::PROTOCOL_THRIFT;
            ASSERT_EQ(channel_.Init("127.0.0.1", cluster_.PickProxyPort(), &options), 0);
        }
    }

    void TearDown() override { cluster_.Stop(); }

 protected:
    TempDir temp_dir_;
    MiniCluster cluster_;
    brpc::Channel channel_;
};

TEST_F(ProxyTest, FeatureAdd_FeatureQuery) {
    {
        thrift::FeatureAddRequest request;
        thrift::FeatureAddResponse response;
        request.__set_namespace_name("ns");
        request.__set_table_name("table");
        request.__set_key("key");
        request.__set_format("json");
        request.__set_policy(thrift::WritePolicy::UPSERT);
        std::vector<thrift::Point> points;
        thrift::Point point;
        point.__set_ts(100);
        point.__set_value(R"X({"key":"value"})X");
        points.emplace_back(point);
        request.__set_point_list(points);

        brpc::ThriftStub stub(&channel_);
        brpc::Controller ctrl;
        stub.CallMethod("FeatureAdd", &ctrl, &request, &response, NULL);
        ASSERT_FALSE(ctrl.Failed());
        ASSERT_EQ(response.status.code, kOK) << response.status.message;
    }

    {
        thrift::FeatureQueryRequest request;
        thrift::FeatureQueryResponse response;
        request.__set_namespace_name("ns");
        request.__set_table_name("table");
        request.__set_key("key");
        request.__set_format("json");
        request.__set_count(1);
        request.__set_start_ts(100);
        request.__set_end_ts(200);

        brpc::ThriftStub stub(&channel_);
        brpc::Controller ctrl;
        stub.CallMethod("FeatureQuery", &ctrl, &request, &response, NULL);
        ASSERT_FALSE(ctrl.Failed());
        ASSERT_EQ(response.status.code, kOK) << response.status.message;
        ASSERT_EQ(response.point_list.size(), 1);
    }
}

TEST_F(ProxyTest, GetSet) {
    {
        thrift::SetRequest request;
        thrift::SetResponse response;
        request.__set_namespace_name("ns");
        request.__set_table_name("table");
        request.__set_key("key");
        request.__set_value("value");

        brpc::ThriftStub stub(&channel_);
        brpc::Controller ctrl;
        stub.CallMethod("Set", &ctrl, &request, &response, NULL);
        ASSERT_FALSE(ctrl.Failed());
        ASSERT_EQ(response.status.code, kOK) << response.status.message;
    }

    {
        thrift::GetRequest request;
        thrift::GetResponse response;
        request.__set_namespace_name("ns");
        request.__set_table_name("table");
        request.__set_key("key");

        brpc::ThriftStub stub(&channel_);
        brpc::Controller ctrl;
        stub.CallMethod("Get", &ctrl, &request, &response, NULL);
        ASSERT_FALSE(ctrl.Failed());
        ASSERT_EQ(response.status.code, kOK) << response.status.message;
        ASSERT_EQ(response.value, "value");
    }
}

TEST_F(ProxyTest, RiskHset) {
    thrift::RiskHsetRequest request;
    thrift::RiskCommonResponse response;
    request.__set_namespace_name("ns");
    request.__set_table_name("table");
    request.__set_key("key");
    request.__set_value("222");
    request.__set_occur_time(123);

    brpc::ThriftStub stub(&channel_);
    brpc::Controller ctrl;
    stub.CallMethod("RiskHset", &ctrl, &request, &response, NULL);
    ASSERT_FALSE(ctrl.Failed());
    ASSERT_EQ(response.status.code, kOK) << response.status.message;
}

TEST_F(ProxyTest, RiskHquery) {
    thrift::RiskHqueryRequest request;
    thrift::RiskHqueryResponse response;
    request.__set_namespace_name("ns");
    request.__set_table_name("table");
    request.__set_key("key");

    brpc::ThriftStub stub(&channel_);
    brpc::Controller ctrl;
    stub.CallMethod("RiskHquery", &ctrl, &request, &response, NULL);
    ASSERT_FALSE(ctrl.Failed());
    ASSERT_TRUE(response.status.code == kOK || response.status.code == kNotFound)
        << response.status.message;
}

TEST_F(ProxyTest, RiskCPCSet) {
    thrift::RiskCPCSetRequest request;
    thrift::RiskCommonResponse response;
    request.__set_namespace_name("ns");
    request.__set_table_name("table");
    request.__set_key("cpckey");
    request.__set_occur_time(123);
    request.__set_precision(thrift::RiskPrecision::FiveSeconds);
    std::vector<std::string> values;
    values.emplace_back("test");
    request.__set_values(values);

    brpc::ThriftStub stub(&channel_);
    brpc::Controller ctrl;
    stub.CallMethod("RiskCPCSet", &ctrl, &request, &response, NULL);
    ASSERT_FALSE(ctrl.Failed());
    ASSERT_EQ(response.status.code, kOK) << response.status.message;
}

TEST_F(ProxyTest, RiskCPCQuery) {
    thrift::RiskCPCQueryRequest request;
    thrift::RiskCPCQueryResponse response;
    request.__set_namespace_name("ns");
    request.__set_table_name("table");
    request.__set_key("cpckey");
    std::vector<thrift::RiskWindow> windows;
    thrift::RiskWindow window;
    window.__set_end_offset(1);
    window.__set_end_offset(2);
    window.__set_unit(thrift::RiskWindowUnit::Day);
    windows.emplace_back(window);
    request.__set_windows(windows);

    brpc::ThriftStub stub(&channel_);
    brpc::Controller ctrl;
    stub.CallMethod("RiskCPCQuery", &ctrl, &request, &response, NULL);
    ASSERT_FALSE(ctrl.Failed());
    ASSERT_TRUE(response.status.code == kOK || response.status.code == kNotFound)
        << response.status.message;
}

TEST_F(ProxyTest, RiskFolSet) {
    thrift::RiskFolSetRequest request;
    thrift::RiskCommonResponse response;
    request.__set_namespace_name("ns");
    request.__set_table_name("table");
    request.__set_key("key");

    brpc::ThriftStub stub(&channel_);
    brpc::Controller ctrl;
    stub.CallMethod("RiskFolSet", &ctrl, &request, &response, NULL);
    ASSERT_FALSE(ctrl.Failed());
    ASSERT_TRUE(response.status.code == kOK || response.status.code == kNotFound)
        << response.status.message;
}

TEST_F(ProxyTest, RiskFolQuery) {
    thrift::RiskFolQueryRequest request;
    thrift::RiskFolQueryResponse response;
    request.__set_namespace_name("ns");
    request.__set_table_name("table");
    request.__set_key("key");

    brpc::ThriftStub stub(&channel_);
    brpc::Controller ctrl;
    stub.CallMethod("RiskFolQuery", &ctrl, &request, &response, NULL);
    ASSERT_FALSE(ctrl.Failed());
    ASSERT_TRUE(response.status.code == kOK || response.status.code == kNotFound)
        << response.status.message;
}

TEST_F(ProxyTest, RiskManager) {
    thrift::RiskManagerRequest request;
    thrift::RiskManagerResponse response;
    request.__set_namespace_name("ns");
    request.__set_table_name("table");
    request.__set_key("key");
    std::vector<thrift::RiskKvPair> field_values;
    thrift::RiskKvPair pair;
    pair.__set_key("111");
    pair.__set_value("222");
    field_values.emplace_back(pair);
    request.__set_field_list(field_values);

    brpc::ThriftStub stub(&channel_);
    brpc::Controller ctrl;
    stub.CallMethod("RiskManager", &ctrl, &request, &response, NULL);
    ASSERT_FALSE(ctrl.Failed());
    ASSERT_TRUE(response.status.code == kOK || response.status.code == kNotFound)
        << response.status.message;
}

TEST_F(ProxyTest, HMSet_HMGet_HGetall_HLen) {
    {
        thrift::HMSetRequest request;
        thrift::HMSetResponse response;
        request.__set_namespace_name("ns");
        request.__set_table_name("table");
        request.__set_key("key");
        std::vector<std::string> fields{"field1", "field2"};
        std::vector<std::string> values{"value1", "value2"};
        request.__set_fields(fields);
        request.__set_values(values);

        brpc::ThriftStub stub(&channel_);
        brpc::Controller ctrl;
        stub.CallMethod("HMSet", &ctrl, &request, &response, NULL);
        ASSERT_FALSE(ctrl.Failed());
        ASSERT_EQ(response.status.code, kOK) << response.status.message;
    }

    {
        thrift::HMGetRequest request;
        thrift::HMGetResponse response;
        request.__set_namespace_name("ns");
        request.__set_table_name("table");
        request.__set_key("key");
        std::vector<std::string> fields{"field1", "field2"};
        request.__set_fields(fields);

        brpc::ThriftStub stub(&channel_);
        brpc::Controller ctrl;
        stub.CallMethod("HMGet", &ctrl, &request, &response, NULL);
        ASSERT_FALSE(ctrl.Failed());
        ASSERT_EQ(response.status.code, kOK) << response.status.message;
        ASSERT_EQ(response.exists.size(), 2);
        ASSERT_EQ(response.values.size(), 2);
        ASSERT_EQ(response.exists[0], true);
        ASSERT_EQ(response.exists[1], true);
        ASSERT_EQ(response.values[0], "value1");
        ASSERT_EQ(response.values[1], "value2");
    }

    {
        thrift::HGetAllRequest request;
        thrift::HGetAllResponse response;
        request.__set_namespace_name("ns");
        request.__set_table_name("table");
        request.__set_key("key");

        brpc::ThriftStub stub(&channel_);
        brpc::Controller ctrl;
        stub.CallMethod("HGetAll", &ctrl, &request, &response, NULL);
        ASSERT_FALSE(ctrl.Failed());
        ASSERT_EQ(response.status.code, kOK) << response.status.message;
        ASSERT_EQ(response.fields.size(), 2);
        ASSERT_EQ(response.values.size(), 2);
        ASSERT_EQ(response.fields[0], "field1");
        ASSERT_EQ(response.fields[1], "field2");
        ASSERT_EQ(response.values[0], "value1");
        ASSERT_EQ(response.values[1], "value2");
    }

    {
        thrift::HLenRequest request;
        thrift::HLenResponse response;
        request.__set_namespace_name("ns");
        request.__set_table_name("table");
        request.__set_key("key");

        brpc::ThriftStub stub(&channel_);
        brpc::Controller ctrl;
        stub.CallMethod("HLen", &ctrl, &request, &response, NULL);
        ASSERT_FALSE(ctrl.Failed());
        ASSERT_EQ(response.status.code, kOK) << response.status.message;
        ASSERT_EQ(response.len, 2);
    }
}

}  // namespace test
}  // namespace proxy
}  // namespace bcache2
