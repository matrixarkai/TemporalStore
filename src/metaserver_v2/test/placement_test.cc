// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "butil/fast_rand.h"
#include "butil/logging.h"
#include "gtest/gtest.h"
#include "spdlog/fmt/fmt.h"

#include "common/logging.h"
#include "common/partition_id_type.h"
#include "common/proto_enhance.h"
#include "metaserver_v2/flags.h"
#include "metaserver_v2/meta/metabase.h"
#include "metaserver_v2/meta/server.h"
#include "metaserver_v2/metrics.h"
#include "metaserver_v2/resource_placement/placement_manager.h"

namespace bcache2::metaserver::test {

static Location MockLocation(int vdc_idx, int vau_idx) {
    Location loc;
    loc.set_vregion("vregion");
    loc.set_vdc(fmt::format("vdc{}", vdc_idx));
    loc.set_vau(fmt::format("vau{}-{}", vdc_idx, vau_idx));
    return loc;
}

static std::vector<ServerPtr> MockServers(int node_count_per_server, int server_count_per_vau,
                                          int vdc_count, int vau_count_per_vdc) {
    std::vector<ServerPtr> servers;
    int idx = 0;
    for (int vdc_idx = 0; vdc_idx < vdc_count; vdc_idx++) {
        for (int vau_idx = 0; vau_idx < vau_count_per_vdc; vau_idx++) {
            for (int i = 0; i < server_count_per_vau; i++) {
                ServerInfo info;
                Endpoint* ep = info.mutable_endpoint();
                ep->set_ip4("10.1.1." + std::to_string((++idx) % 3));
                ep->set_port(idx);
                *info.mutable_location() = MockLocation(vdc_idx, vau_idx);
                for (int j = 0; j < node_count_per_server; j++) {
                    NUMANode* node = info.add_numa_nodes();
                    node->set_cpu_list(fmt::format("{}-{}", j * 48, (j + 1) * 48 - 1));
                    node->set_memory_size_mb(512 * 1024);
                }
                info.set_state(ServerState::SERVER_NORMAL);
                ServerPtr server = std::make_shared<Server>(std::move(info));
                servers.push_back(server);
            }  // server
        }      // vau
    }
    return servers;
}

static TablePtr MockTable(const std::string& name, int pset_count,
                          const std::vector<Location>& locations) {
    TableInfo info;
    info.set_name(name);
    info.set_namespace_name("ns1");
    info.set_partition_set_num(pset_count);
    auto unit = info.add_partition_units();
    unit->set_partition_num(locations.size());
    for (auto loc : locations) {
        *unit->add_placement_set() = loc;
    }
    unit->set_storage_pool_uri("local://data00/x");
    auto quota = info.mutable_quota();
    quota->set_ops_read(10'000);
    quota->set_ops_write(10'000);

    return std::make_shared<Table>(info);
}

static Status AddMockTable(NamespaceManager* ns_mgr, std::string name, int pset_count,
                           const std::vector<Location>& locations) {
    NamespaceInfo ns_info;
    ns_info.set_name("ns1");
    NamespacePtr ns = std::make_shared<Namespace>(ns_info);
    Status status = ns_mgr->Add(ns);
    if (!status.ok()) {
        return status;
    }
    return ns_mgr->AddTable(MockTable(name, pset_count, locations));
}

TEST(PlacementTest, SimpleTest) {
    InitMetrics("dev.bcache2.ut", {});
    BYTE_DEFER({ QuitMetrics(); });
    auto metabase = std::make_shared<Metabase>();
    metabase->Init();
    auto loc_mgr = metabase->GetServerLocationManager();
    auto ns_mgr = metabase->GetNamespaceManager();
    auto pm = std::make_unique<PlacementManager>(metabase.get());

    // mock resource
    const int node_count_per_server = 4;
    const int server_count_per_vau = 10;
    const int vdc_count = 2;
    const int vau_count_per_vdc = 3;
    auto servers =
        MockServers(node_count_per_server, server_count_per_vau, vdc_count, vau_count_per_vdc);
    const size_t total_server_count = servers.size();
    int64_t now = butil::gettimeofday_us();
    for (auto& server : servers) {
        Status status = loc_mgr->Add(server);
        ASSERT_TRUE(status.ok()) << status;
        server->MutableRealtimeStats()->SetLastHeartbeatTimeUs(now);
    }

    // mock table
    std::string name = "table1";
    std::vector<Location> locations{
        MockLocation(0, 0),
        MockLocation(0, 1),
        MockLocation(1, 0),
        MockLocation(1, 1),
    };
    const int pset_count = 4096;
    Status status = AddMockTable(ns_mgr, name, pset_count, locations);
    ASSERT_TRUE(status.ok()) << status;
    NamespacePtr ns;
    status = ns_mgr->Get("ns1", &ns);
    ASSERT_TRUE(status.ok()) << status;
    TablePtr table;
    status = ns->Get(name, &table);
    ASSERT_TRUE(status.ok()) << status;

    // place
    pm->SetDefaultRules();
    FLAGS_metaserver_placement_host_deduplicate = false;
    auto partitions = table->GetAllPartitions();
    std::map<uint64_t, uint64_t> p_node_map;
    for (auto p : partitions) {
        NodePtr node;
        Status status = pm->PlacePartition(p, &node);
        ASSERT_TRUE(status.ok()) << status;

        // commit in no time
        Server* server = node->GetServer();
        PlacementSpec place;
        *place.mutable_node() = node->GetInfo();
        *place.mutable_location() = server->GetLocation();
        *place.mutable_server() = server->GetEndpoint();
        p->SetPlacementActual(std::move(place), node);
        p->FinishCreating(butil::gettimeofday_s());
        node->CommitIntentPartition(p->GetId());
    }

    // aggr and validate
    Location no_partition_loc = MockLocation(0, 2);
    servers = loc_mgr->List(no_partition_loc, [](const auto&) { return true; });
    const size_t no_partition_server_count = servers.size();
    ASSERT_FALSE(servers.empty());
    for (auto& server : servers) {
        auto nodes = server->GetNodes();
        ASSERT_FALSE(nodes.empty());
        for (auto& node : nodes) {
            ASSERT_EQ(node->GetPartitionCount(), 0);
        }
    }
    size_t sum1 = 0;
    size_t sum2 = 0;
    size_t releated_node_count = 0;
    for (auto& loc : locations) {
        servers = loc_mgr->List(loc, [](const auto&) { return true; });
        ASSERT_FALSE(servers.empty());
        for (auto& server : servers) {
            auto nodes = server->GetNodes();
            ASSERT_FALSE(nodes.empty());
            std::unordered_set<uint64_t> pset_ids;
            for (auto& node : nodes) {
                size_t count = node->GetPartitionCount();
                ASSERT_GT(count, 0);
                sum1 += count;
                sum2 += count * count;
                releated_node_count++;

                for (uint64_t id : node->GetPartitionIds()) {
                    partition_id_t pid(id);
                    uint64_t pset_id = pid.GetPartitionSetId();
                    ASSERT_EQ(pset_ids.count(pset_id), 0);
                    pset_ids.insert(pset_id);
                }
            }
        }
    }
    ASSERT_GT(releated_node_count, 0);
    ASSERT_EQ(pset_count * locations.size(), sum1);
    ASSERT_EQ(releated_node_count,
              (total_server_count - 2 * no_partition_server_count) * node_count_per_server);
    double sigma = sqrt(1. * releated_node_count * sum2 - 1. * sum1 * sum1) / releated_node_count;
    double mean = 1. * sum1 / releated_node_count;
    ASSERT_LT(sigma, mean * 0.01) << sigma << " " << mean;
}

TEST(PlacementTest, EmptyPlacementExpectationReturnsError) {
    InitMetrics("dev.bcache2.ut", {});
    BYTE_DEFER({ QuitMetrics(); });
    auto metabase = std::make_shared<Metabase>();
    metabase->Init();
    auto ns_mgr = metabase->GetNamespaceManager();
    auto pm = std::make_unique<PlacementManager>(metabase.get());
    pm->SetDefaultRules();

    std::vector<Location> locations{Location{}};
    Status status = AddMockTable(ns_mgr, "table_empty_placement", 1, locations);
    ASSERT_TRUE(status.ok()) << status;

    NamespacePtr ns;
    status = ns_mgr->Get("ns1", &ns);
    ASSERT_TRUE(status.ok()) << status;
    TablePtr table;
    status = ns->Get("table_empty_placement", &table);
    ASSERT_TRUE(status.ok()) << status;
    auto partitions = table->GetAllPartitions();
    ASSERT_EQ(1, partitions.size());

    NodePtr node;
    status = pm->PlacePartition(partitions[0], &node);
    ASSERT_TRUE(status.IsInvalidArgument()) << status;
    ASSERT_EQ(nullptr, node);
}

}  // namespace bcache2::metaserver::test
