// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "gtest/gtest.h"

#include "common/status.h"
#include "metaserver_v2/flags.h"
#include "protocol/metaserver.pb.h"
#include "proxy/proxy.h"
#include "test/common/temp_dir.h"
#include "test/common/util.h"
#include "test/mini_cluster/mini_cluster.h"

namespace bcache2::metaserver::test {

class ProxyHATest : public testing::Test {
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
        id->set_operator_name("proxy_ha");
    }

    const int proxy_count = 10;
    const std::string cluster_name{"proxy_ha"};
    TempDir temp_dir;
    MiniCluster cluster;
};

TEST_F(ProxyHATest, SimpleTest) {
    FLAGS_metaserver_convict_safe_mode_warning_ratio = 100;
    FLAGS_metaserver_convict_safe_mode_critical_ratio = 100;
    FLAGS_metaserver_convict_routine_interval_ms = 100;
    FLAGS_metaserver_phi_failure_threshold = 1.2;

    const std::string ns = "one";
    const int instance_num = 3;
    const int expect_instance_num = std::min(proxy_count, instance_num);
    const std::string consul_name = "dev.bcache2_proxy_ha." + ns + "_" + temp_dir.GetDir();
    auto master = cluster.GetMaster();
    // auto query_stub = master->GetQueryStub();
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
    AckResponse response;
    brpc::Controller cntl;
    manage_stub->PutProxyGroup(&cntl, &request, &response, NULL);
    ASSERT_TRUE(!cntl.Failed());
    status = Status::FromRpcStatus(response.status());
    ASSERT_TRUE(status.ok());

    bool y = false;
    auto proxies = cluster.GetAllProxies();
    std::vector<ProxyWrapper*> related;
    ASSERT_EQ(proxies.size(), proxy_count);
    for (int i = 0; i < 20 && !y; i++) {
        bthread_usleep(100 * 1000);  // 2 sec total
        int count = 0;
        for (auto proxy : proxies) {
            auto config = proxy->GetServer()->GetConfig();
            if (config.namespace_name == ns) {
                count++;
                related.push_back(proxy);
            }
            if (count == expect_instance_num) {
                y = true;
                break;
            }
        }
    }
    ASSERT_TRUE(y);

    LOG_WARNING("stop related proxy");
    for (auto proxy : related) {
        proxy->Stop();
    }

    LOG_WARNING("wait ha done");
    y = false;
    std::vector<ProxyWrapper*> new_related;
    for (int i = 0; i < 20 && !y; i++) {
        bthread_usleep(1000 * 1000);  // 20 sec total
        int count = 0;
        for (auto proxy : proxies) {
            bool kicked = false;
            for (auto old : related) {
                if (old == proxy) {
                    kicked = true;
                    break;
                }
            }
            if (kicked) {
                continue;
            }

            auto config = proxy->GetServer()->GetConfig();
            if (config.namespace_name == ns) {
                count++;
                new_related.push_back(proxy);
            }
            if (count == expect_instance_num) {
                y = true;
                break;
            }
        }
    }
    ASSERT_TRUE(y);

    LOG_WARNING("stop new_related proxy");
    for (auto proxy : new_related) {
        // TODO(wuzhenyu) too tricky
        proxy->GetServer()->heart_beat_->started_ = false;
    }

    LOG_WARNING("wait ha done");
    y = false;
    std::vector<ProxyWrapper*> new_related2;
    for (int i = 0; i < 20 && !y; i++) {
        bthread_usleep(1000 * 1000);  // 20 sec total
        int count = 0;
        for (auto proxy : proxies) {
            bool kicked = false;
            for (auto old : related) {
                if (old == proxy) {
                    kicked = true;
                    break;
                }
            }
            if (kicked) {
                continue;
            }
            for (auto old : new_related) {
                if (old == proxy) {
                    kicked = true;
                    break;
                }
            }
            if (kicked) {
                continue;
            }

            auto config = proxy->GetServer()->GetConfig();
            if (config.namespace_name == ns) {
                count++;
                new_related2.push_back(proxy);
            }
            if (count == expect_instance_num) {
                y = true;
                break;
            }
        }
    }
    ASSERT_TRUE(y);
}

}  // namespace bcache2::metaserver::test

