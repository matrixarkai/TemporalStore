// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "butil/fast_rand.h"
#include "gtest/gtest.h"

#include "metaserver_v2/meta/location.h"
#include "metaserver_v2/meta/server.h"

namespace bcache2::metaserver::test {

/// make me happy
class LocationTest : public testing::Test {};

ServerInfo MockServerInfo() {
    ServerInfo normal_info;
    auto ep = normal_info.mutable_endpoint();
    ep->set_addr_family(Endpoint::ADDR_DUAL_STACK);
    ep->set_ip4("10.37.144.143");
    ep->set_ip6("fdbd:dc03:ff:1:1:37:144:143");
    ep->set_port(5000);
    auto loc = normal_info.mutable_location();
    loc->set_vregion("vregion");
    loc->set_vdc("vdc");
    loc->set_vau("vau");
    for (int i = 0; i < 4; i++) {
        auto node = normal_info.add_numa_nodes();
        node->set_cpu_list(std::to_string(i));
        node->set_memory_size_mb(512);
    }
    return normal_info;
}

TEST_F(LocationTest, CurdTest) {
    Status status;
    auto lm = std::make_unique<LocationManager<Server>>();
    ServerInfo info = MockServerInfo();
    {
        auto server = std::make_shared<Server>(info);
        // add
        status = lm->Add(server);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(server->GetId(), 1);
        // add dup
        status = lm->Add(server);
        ASSERT_FALSE(status.ok());

        // get
        ServerPtr server2 = lm->Get(server->GetId());
        ASSERT_TRUE(server2 != nullptr);
        ASSERT_EQ(server.get(), server2.get());
        server2 = lm->Get(info.endpoint());
        ASSERT_TRUE(server2 != nullptr);
        ASSERT_EQ(server.get(), server2.get());
        auto servers = lm->List(info.location(), [](auto s) { return true; });
        ASSERT_EQ(servers.size(), 1);
        ASSERT_EQ(servers[0].get(), server.get());

        // remove than get
        status = lm->Remove(server);
        ASSERT_TRUE(status.ok());
        server2 = lm->Get(server->GetId());
        ASSERT_TRUE(server2 == nullptr);
        server2.reset();
        server2 = lm->Get(info.endpoint());
        ASSERT_TRUE(server2 == nullptr);
        servers = lm->ListAll();
        ASSERT_EQ(servers.size(), 0);
        servers = lm->List(info.location(), [](auto s) { return true; });
        ASSERT_EQ(servers.size(), 0);
    }
}

TEST_F(LocationTest, FilterQueryTest) {
    Status status;
    auto lm = std::make_unique<LocationManager<Server>>();
    std::vector<std::string> vdcs{"vdc1", "vdc2", "vdc3"};
    std::vector<std::string> vaus{"vau1", "vau2", "vau3"};
    std::map<std::string, std::vector<ServerPtr>> data;
    size_t total = butil::fast_rand() % 1000 + 329;
    auto base_info = MockServerInfo();
    for (size_t i = 0; i < total; i++) {
        const auto& vdc = vdcs[butil::fast_rand() % vdcs.size()];
        const auto& vau = vaus[butil::fast_rand() % vaus.size()];
        auto info = base_info;
        info.mutable_endpoint()->set_port(i + 5000);
        info.mutable_location()->set_vdc(vdc);
        info.mutable_location()->set_vau(vau);
        auto server = std::make_shared<Server>(info);
        status = lm->Add(server);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(server->GetId(), i + 1);
        data[vdc].push_back(server);
        data[vdc + vau].push_back(server);
    }
    auto servers = lm->ListAll();
    ASSERT_EQ(servers.size(), total);
    Location empty_loc;
    servers = lm->List(empty_loc, [](auto s) { return true; });
    ASSERT_EQ(servers.size(), total);
    for (const auto& vdc : vdcs) {
        Location loc = base_info.location();
        loc.set_vdc(vdc);
        loc.clear_vau();
        servers = lm->List(loc, [](auto s) { return true; });
        ASSERT_EQ(servers.size(), data[vdc].size());
        for (const auto& vau : vaus) {
            loc.set_vau(vau);
            servers = lm->List(loc, [](auto s) { return true; });
            ASSERT_EQ(servers.size(), data[vdc + vau].size());
        }
    }
    for (size_t i = 0; i < total; i++) {
        uint64_t id = i + 1;
        ServerPtr server = lm->Get(id);
        ASSERT_TRUE(server != nullptr);
        Endpoint ep = server->GetEndpoint();
        server = lm->Get(ep);
        ASSERT_TRUE(server != nullptr);

        Location loc = server->GetLocation();
        auto servers = lm->List(loc, [](auto s) { return true; });
        status = lm->Remove(server);
        ASSERT_TRUE(status.ok());

        // get after remove
        ServerPtr server2 = lm->Get(ep);
        ASSERT_TRUE(server2 == nullptr);
        server2 = lm->Get(id);
        ASSERT_TRUE(server2 == nullptr);

        auto servers2 = lm->List(loc, [](auto s) { return true; });
        ASSERT_EQ(servers.size(), servers2.size() + 1);
    }

    // now empty
    Location loc = base_info.location();
    loc.clear_vdc();
    loc.clear_vau();
    servers = lm->List(loc, [](auto s) { return true; });
    ASSERT_TRUE(servers.empty());
    servers = lm->ListAll();
    ASSERT_TRUE(servers.empty());
}

}  // namespace bcache2::metaserver::test

