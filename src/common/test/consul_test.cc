// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "common/consul.h"

#include <gtest/gtest.h>

#include <list>

#include "brpc/server.h"

namespace bcache2 {
namespace service_discovery {
namespace test {

class ConsulTest : public testing::Test {
 public:
    void SetUp() override { std::srand(time(nullptr)); }

    void TearDown() override {}

 protected:
    Consul consul_;
};

TEST_F(ConsulTest, AgentDown) {
    Consul down_consul("192.168.1.5", 2913);

    std::vector<Endpoint> endpoints;
    Status status = down_consul.Lookup("inf.sd.proxy", Consul::AddrFamily::DualStack, &endpoints);
    ASSERT_FALSE(status.ok());

    status = down_consul.Register("bytedance.bcache2.unittest", 1000 + std::rand() % 9000, 1000);
    ASSERT_FALSE(status.ok());
}

TEST_F(ConsulTest, Lookup) {
    std::vector<Endpoint> endpoints;
    Status status = consul_.Lookup("inf.sd.proxy", Consul::AddrFamily::DualStack, &endpoints);
    ASSERT_TRUE(status.ok());
    ASSERT_GE(endpoints.size(), 0);
    std::cout << "dusl-stack endpoints num: " << endpoints.size() << "\n";
    for (const Endpoint& ep : endpoints) {
        std::cout << ep.host << ":" << ep.port << "\n";
    }

    status = consul_.Lookup("inf.sd.proxy", Consul::AddrFamily::V4, &endpoints);
    ASSERT_TRUE(status.ok());
    ASSERT_GE(endpoints.size(), 0);
    std::cout << "v4 endpoints num: " << endpoints.size() << "\n";
    for (const Endpoint& ep : endpoints) {
        std::cout << ep.host << ":" << ep.port << "\n";
    }

    status = consul_.Lookup("inf.sd.proxy", Consul::AddrFamily::V6, &endpoints);
    ASSERT_TRUE(status.ok());
    ASSERT_GE(endpoints.size(), 0);
    std::cout << "v6 endpoints num: " << endpoints.size() << "\n";
    for (const Endpoint& ep : endpoints) {
        std::cout << ep.host << ":" << ep.port << "\n";
    }

    status = consul_.Lookup("inf.sd.proxy", Consul::AddrFamily::Auto, &endpoints);
    ASSERT_TRUE(status.ok());
    ASSERT_GE(endpoints.size(), 0);
    std::cout << "auto endpoints num: " << endpoints.size() << "\n";
    for (const Endpoint& ep : endpoints) {
        std::cout << ep.host << ":" << ep.port << "\n";
    }

    status = consul_.Lookup("inf.sd.non_exist", Consul::AddrFamily::V6, &endpoints);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(endpoints.size(), 0);
}

TEST_F(ConsulTest, DISABLED_Register) {
    int ttl = 20;
    int port = 1000 + std::rand() % 9000;

    std::string service = "bytedance.bcache2.unittest_" + std::to_string(port);
    Status status = consul_.Register(service, port, ttl);
    ASSERT_TRUE(status.ok());

    BYTE_DEFER({ consul_.DeRegister(service, port); });

    std::vector<Endpoint> endpoints;
    int try_times = 0;
    for (try_times = 0; try_times < 10; ++try_times) {
        Status status = consul_.Lookup(service, Consul::AddrFamily::DualStack, &endpoints);
        if (status.ok() && endpoints.size() > 0) {
            break;
        }
        sleep(1);
    }
    ASSERT_GT(endpoints.size(), 0);

    // waiting for service critical
    sleep(20);

    try_times = 0;
    for (try_times = 0; try_times < 10; ++try_times) {
        Status status = consul_.Lookup(service, Consul::AddrFamily::DualStack, &endpoints);
        if (status.ok() && endpoints.size() == 0) {
            break;
        }
        sleep(1);
    }
    ASSERT_EQ(endpoints.size(), 0);
}

TEST_F(ConsulTest, DISABLED_DeRegister) {
    int ttl = 120;
    int port = 1000 + std::rand() % 9000;

    std::string service = "bytedance.bcache2.unittest_" + std::to_string(port);
    Status status = consul_.Register(service, port, ttl);
    ASSERT_TRUE(status.ok());

    std::vector<Endpoint> endpoints;
    int try_times = 0;
    for (try_times = 0; try_times < 10; ++try_times) {
        Status status = consul_.Lookup(service, Consul::AddrFamily::DualStack, &endpoints);
        if (status.ok() && endpoints.size() > 0) {
            break;
        }
        sleep(1);
    }
    ASSERT_GT(endpoints.size(), 0);

    // waiting for service critical
    status = consul_.DeRegister(service, port);
    ASSERT_TRUE(status.ok());

    try_times = 0;
    for (try_times = 0; try_times < 10; ++try_times) {
        Status status = consul_.Lookup(service, Consul::AddrFamily::DualStack, &endpoints);
        if (status.ok() && endpoints.size() == 0) {
            break;
        }
        sleep(1);
    }
    ASSERT_EQ(endpoints.size(), 0);
}

}  // namespace test
}  // namespace service_discovery
}  // namespace bcache2
