// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <gtest/gtest.h>

#include "common/status.h"
#include "metaserver_v2/meta/namespace.h"
#include "metaserver_v2/meta/proxy.h"
#include "protocol/metaserver.pb.h"
#include "proxy/proxy.h"
#include "test/common/temp_dir.h"
#include "test/common/util.h"
#include "test/mini_cluster/mini_cluster.h"

namespace bcache2::metaserver::test {

class ProxyCalibrateTest : public testing::Test {
 public:
    void SetUp() override {
        MiniCluster::Options options;
        options.cluster_name = cluster_name;
        options.work_dir = temp_dir.GetDir();
        options.server_count = 1;
        options.proxy_count = proxy_count;
        options.cluster_uri = "file://" + temp_dir.GetDir() + "/cluster/public";
        cluster.Init(options);
        Status status = cluster.Start();
        ASSERT_TRUE(status.ok()) << status;
    }

    void TearDown() override { cluster.Stop(); }

    void InitRequestId(metaserver::RequestId* id) {
        id->set_timestamp(butil::gettimeofday_s());
        id->set_cluster_name(cluster_name);
        id->set_operator_name("proxy_calibration");
    }

    const int proxy_count = 10;
    const std::string cluster_name{"proxy_calibration"};
    TempDir temp_dir;
    MiniCluster cluster;
};

TEST_F(ProxyCalibrateTest, OneNamespaceTest) {
    const std::string ns = "one";
    const int instance_num = 100;
    const int expect_instance_num = std::min(proxy_count, instance_num);
    const std::string consul_name = "dev.bcache2_proxy_calibration." + ns + "_" + temp_dir.GetDir();
    auto master = cluster.GetMaster();
    auto query_stub = master->GetQueryStub();
    auto manage_stub = master->GetManageStub();

    Status status = master->CreateNamespace(ns);
    ASSERT_TRUE(status.ok());
    metaserver::PutProxyGroupRequest request;
    InitRequestId(request.mutable_id());
    auto info = request.mutable_info();
    info->set_namespace_name(ns);
    *(info->mutable_placement()) = MockLocation();
    info->set_instance_num(instance_num);
    info->mutable_config()->add_consul_names(consul_name);
    info->mutable_config()->add_consul_names(consul_name + "2");
    AckResponse response;
    brpc::Controller cntl;
    manage_stub->PutProxyGroup(&cntl, &request, &response, NULL);
    ASSERT_TRUE(!cntl.Failed());
    status = Status::FromRpcStatus(response.status());
    ASSERT_TRUE(status.ok());

    bool y = false;
    for (int i = 0; i < 20; i++) {
        bthread_usleep(100 * 1000);  // 2 sec total
        ListProxyGroupRequest request;
        request.mutable_id()->set_cluster_name(cluster_name);
        ListProxyGroupResponse response;
        brpc::Controller cntl;
        query_stub->ListProxyGroup(&cntl, &request, &response, nullptr);
        if (cntl.Failed() || response.status().code() != 0) {
            LOG_WARNING("query metaserver failed")
                .put("is_cntl_failed", cntl.Failed())
                .put("cnt_msg", cntl.ErrorText())
                .put("code", response.status().code());
            continue;
        }
        y = true;
        ASSERT_EQ(response.groups_size(), 1);
        const auto& info = response.groups(0);
        ASSERT_EQ(info.namespace_name(), ns);
        ASSERT_EQ(info.instance_num(), instance_num);
        ASSERT_EQ(info.config().consul_names_size(), 2);
        for (const auto& name : info.config().consul_names()) {
            ASSERT_EQ(name.find(consul_name), 0);
        }
        break;
    }
    ASSERT_TRUE(y);
    y = false;
    for (int i = 0; i < 20; i++) {
        bthread_usleep(100 * 1000);  // 2 sec total
        ListProxyRequest request;
        InitRequestId(request.mutable_id());
        ListProxyResponse response;
        brpc::Controller cntl;
        query_stub->ListProxy(&cntl, &request, &response, nullptr);
        if (cntl.Failed() || response.status().code() != 0) {
            LOG_WARNING("query metaserver failed")
                .put("is_cntl_failed", cntl.Failed())
                .put("cnt_msg", cntl.ErrorText())
                .put("code", response.status().code());
            continue;
        }
        ASSERT_EQ(response.proxies_size(), proxy_count);
        int count = 0;
        for (auto& proxy : response.proxies()) {
            if (proxy.namespace_name() == ns) {
                count++;
                ASSERT_EQ(proxy.config().consul_names_size(), 2);
                for (const auto& name : proxy.config().consul_names()) {
                    ASSERT_EQ(name.find(consul_name), 0);
                }
            }
        }
        if (count == expect_instance_num) {
            y = true;
            break;
        }
    }
    ASSERT_TRUE(y);
    y = false;
    auto proxies = cluster.GetAllProxies();
    ASSERT_EQ(proxies.size(), proxy_count);
    for (int i = 0; i < 20 && !y; i++) {
        bthread_usleep(100 * 1000);  // 2 sec total
        int count = 0;
        for (auto proxy : proxies) {
            auto config = proxy->GetServer()->GetConfig();
            if (config.namespace_name == ns) {
                count++;
                ASSERT_EQ(config.consul_names.size(), 2);
            }
            if (count == expect_instance_num) {
                y = true;
                break;
            }
        }
    }
    ASSERT_TRUE(y);
}

TEST(ConsulCalibrateTest, SimpleTest) {
    ConsulMap consul_map;

    NamespaceInfo info;
    info.set_id(1);
    info.set_name("1");
    auto ns = std::make_shared<Namespace>(info);
    auto cluster = ns->GetProxyCluster();
    Status status;
    for (int i = 0; i < 3; i++) {
        ProxyGroupInfo pg_info;
        pg_info.set_namespace_name(info.name());
        pg_info.mutable_placement()->set_vregion("vregion");
        pg_info.mutable_placement()->set_vdc(fmt::format("vdc-{}", i));
        pg_info.set_instance_num(2);
        pg_info.mutable_config()->add_consul_names("a.b.c");
        pg_info.mutable_config()->add_consul_names("a.b.c2");
        pg_info.mutable_config()->add_consul_names("a.b.c3");
        for (const std::string& name : pg_info.config().consul_names()) {
            status = consul_map.Validate(cluster, name);
            ASSERT_TRUE(status.ok());
        }
        status = cluster->CreateOrUpdateProxyGroup(pg_info);
        ASSERT_TRUE(status.ok());
        status = consul_map.Calibrate(cluster);
        ASSERT_TRUE(status.ok());
    }
    NamespaceInfo info2;
    info2.set_id(2);
    info2.set_name("2");
    auto ns2 = std::make_shared<Namespace>(info2);
    auto cluster2 = ns2->GetProxyCluster();
    for (int i = 0; i < 3; i++) {
        ProxyGroupInfo pg_info;
        pg_info.set_namespace_name(info.name());
        pg_info.mutable_placement()->set_vregion("vregion");
        pg_info.mutable_placement()->set_vdc(fmt::format("vdc-{}", i));
        pg_info.set_instance_num(2);
        pg_info.mutable_config()->add_consul_names("a.b.c");
        pg_info.mutable_config()->add_consul_names("a.b.c2");
        pg_info.mutable_config()->add_consul_names("a.b.c3");
        for (const std::string& name : pg_info.config().consul_names()) {
            status = consul_map.Validate(cluster2, name);
            ASSERT_FALSE(status.ok());
        }
        pg_info.mutable_config()->clear_consul_names();
        pg_info.mutable_config()->add_consul_names("a.x.c");
        pg_info.mutable_config()->add_consul_names("a.x.c2");
        pg_info.mutable_config()->add_consul_names("a.x.c3");
        for (const std::string& name : pg_info.config().consul_names()) {
            status = consul_map.Validate(cluster2, name);
            ASSERT_TRUE(status.ok());
        }
        status = cluster2->CreateOrUpdateProxyGroup(pg_info);
        ASSERT_TRUE(status.ok());
        status = consul_map.Calibrate(cluster2);
        ASSERT_TRUE(status.ok());
    }
    ASSERT_EQ(consul_map.ns_to_consul_map_.size(), 2);
    for (auto p : consul_map.ns_to_consul_map_) {
        ASSERT_EQ(p.second.size(), 3);
    }
    ASSERT_EQ(consul_map.consul_to_ns_map_.size(), 6);

    status = consul_map.Remove(cluster);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(consul_map.ns_to_consul_map_.size(), 1);
    for (auto p : consul_map.ns_to_consul_map_) {
        ASSERT_EQ(p.second.size(), 3);
    }
    ASSERT_EQ(consul_map.consul_to_ns_map_.size(), 3);

    for (int i = 0; i < 3; i++) {
        ProxyGroupInfo pg_info;
        pg_info.set_namespace_name(info.name());
        pg_info.mutable_placement()->set_vregion("vregion");
        pg_info.mutable_placement()->set_vdc(fmt::format("vdc-{}", i));
        pg_info.set_instance_num(2);
        pg_info.mutable_config()->add_consul_names("a.b.c");
        pg_info.mutable_config()->add_consul_names("a.b.c2");
        pg_info.mutable_config()->add_consul_names("a.b.c3");
        for (const std::string& name : pg_info.config().consul_names()) {
            status = consul_map.Validate(cluster2, name);
            ASSERT_TRUE(status.ok());
        }
        status = cluster2->CreateOrUpdateProxyGroup(pg_info);
        ASSERT_TRUE(status.ok());
        status = consul_map.Calibrate(cluster2);
        ASSERT_TRUE(status.ok());
    }
    ASSERT_EQ(consul_map.ns_to_consul_map_.size(), 1);
    for (auto p : consul_map.ns_to_consul_map_) {
        ASSERT_EQ(p.second.size(), 3);
    }
    ASSERT_EQ(consul_map.consul_to_ns_map_.size(), 3);
}

}  // namespace bcache2::metaserver::test

