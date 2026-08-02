// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "butil/fast_rand.h"
#include "gtest/gtest.h"

#include "metaserver_v2/meta/metabase.h"
#include "metaserver_v2/metrics.h"
#include "metaserver_v2/resource_placement/location_placement_rule.h"
#include "metaserver_v2/resource_placement/placement_manager.h"
#include "test/common/temp_dir.h"
#include "test/mini_cluster/mini_cluster.h"

namespace bcache2::metaserver::test {

class MetaSnapshotTest : public testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}
};

static Location MockDefaultLoc() {
    Location loc;
    loc.set_vregion("vregion");
    loc.set_vdc("vdc");
    loc.set_vau("vau");
    return loc;
}

static ProxyInfo MockProxyInfo() {
    ProxyInfo info;
    auto ep = info.mutable_endpoint();
    ep->set_addr_family(Endpoint::ADDR_DUAL_STACK);
    uint32_t suffix = butil::fast_rand() % 256;
    uint32_t suffix2 = butil::fast_rand() % 256;
    ep->set_ip4(fmt::format("10.0.{}.{}", suffix, suffix2));
    ep->set_ip6(fmt::format("fdbd:dc03:ff:1:1:37:{}:{}", suffix, suffix2));
    ep->set_port(butil::fast_rand() % 10'000 + 1000);
    *info.mutable_location() = MockDefaultLoc();
    return info;
}

static ServerInfo MockServerInfo() {
    ServerInfo normal_info;
    auto ep = normal_info.mutable_endpoint();
    ep->set_addr_family(Endpoint::ADDR_DUAL_STACK);
    uint32_t suffix = butil::fast_rand() % 256;
    uint32_t suffix2 = butil::fast_rand() % 256;
    ep->set_ip4(fmt::format("10.0.{}.{}", suffix, suffix2));
    ep->set_ip6(fmt::format("fdbd:dc03:ff:1:1:37:{}:{}", suffix, suffix2));
    ep->set_port(butil::fast_rand() % 10'000 + 1000);
    *normal_info.mutable_location() = MockDefaultLoc();
    for (int i = 0; i < 2; i++) {
        auto node = normal_info.add_numa_nodes();
        node->set_cpu_list(std::to_string(i));
        node->set_memory_size_mb(512);
    }
    return normal_info;
}

TableInfo MockTableInfo() {
    TableInfo info;
    uint32_t table_suffix = butil::fast_rand() % 100'000;
    info.set_name(fmt::format("table_{}", table_suffix));
    uint32_t ns_suffix = butil::fast_rand() % 3;
    info.set_namespace_name(fmt::format("ns_{}", ns_suffix));
    info.set_partition_set_num(32);
    for (int i = 0; i < 2; i++) {
        auto unit = info.add_partition_units();
        const int cnt = 2;
        unit->set_partition_num(cnt);
        for (int j = 0; j < cnt; j++) {
            *unit->add_placement_set() = MockDefaultLoc();
        }
        unit->set_storage_pool_uri("local://data00/x");
    }
    auto quota = info.mutable_quota();
    quota->set_ops_read(10'000);
    quota->set_ops_write(10'000);

    return info;
}

ProxyGroupInfo MockProxyGroupInfo(const std::string& ns_name) {
    ProxyGroupInfo info;
    info.set_namespace_name(ns_name);
    *info.mutable_placement() = MockDefaultLoc();
    if (butil::fast_rand() % 2 == 0) {
        info.mutable_placement()->clear_vau();
    }
    info.set_instance_num(butil::fast_rand() % 10 + 1);
    auto config = info.mutable_config();
    config->add_consul_names(fmt::format("a.b.{}.{}", ns_name, info.instance_num()));
    return info;
}

TEST_F(MetaSnapshotTest, SimpleTest) {
    InitMetrics("dev.bcache2.ut", {});
    BYTE_DEFER({ QuitMetrics(); });

    Status status;
    Metabase mb;
    mb.Init();
    const int64_t now = butil::gettimeofday_s();

    ManagementInfo info;
    info.add_reserved_consul_name_list("asdf");
    mb.UpdateManageInfo(info);

    auto server_loc_mgr = mb.GetServerLocationManager();
    for (int i = 0; i < 100; i++) {
        auto server = std::make_shared<Server>(MockServerInfo());
        server->SetState(ServerState::SERVER_NORMAL);
        status = server_loc_mgr->Add(server);
        ASSERT_TRUE(status.ok()) << status;
        server->MutableRealtimeStats()->SetLastHeartbeatTimeUs(now);
    }

    auto ns_mgr = mb.GetNamespaceManager();
    PlacementManager place_mgr(&mb);
    {
        std::vector<std::shared_ptr<PlacementRule>> result;
        result.emplace_back(new LocationPlacementRule(server_loc_mgr));
        place_mgr.UpdateRules(result);
    }
    for (int i = 0; i < 20; i++) {
        TableInfo table_info = MockTableInfo();
        const std::string ns_name = table_info.namespace_name();
        {
            NamespaceInfo ns_info;
            ns_info.set_name(ns_name);
            NamespacePtr ns = std::make_shared<Namespace>(ns_info);
            ns_mgr->Add(ns);
        }
        TablePtr table = std::make_shared<Table>(table_info);
        status = ns_mgr->AddTable(table);
        ASSERT_TRUE(status.ok()) << status;

        for (auto& p : table->GetAllPartitions()) {
            NodePtr node;
            status = place_mgr.PlacePartition(p, &node);
            ASSERT_TRUE(status.ok()) << status;
            node->CommitIntentPartition(p->GetId());
            Server* server = node->GetServer();

            PlacementSpec place;
            *place.mutable_node() = node->GetInfo();
            *place.mutable_location() = server->GetLocation();
            *place.mutable_server() = server->GetEndpoint();
            p->SetPlacementActual(std::move(place), node);
            p->FinishCreating(now);
            if (butil::fast_rand() % 100 < 95) {
                p->FinishLoading();
                PartitionSet* pset = p->GetPartitionSet();
                pset->FinishCreatingPartition(p);
                if (pset->GetState() == PartitionSetState::PSET_NORMAL) {
                    table->FinishCreatingPartitionSet(pset->GetId());
                }
            }
        }
        for (int round = 0; round < 3; round++) {
            // inject chaos
            auto psets = table->GetAllPartitionSets();
            for (auto& pset : psets) {
                auto partitions = pset->GetAllPartitions();
                size_t cnt = partitions.size();
                for (size_t i = 0; i < cnt; i++) {
                    auto partition = partitions[butil::fast_rand() % partitions.size()];
                    auto state = partition->GetState();
                    if (state == PartitionState::P_FREEZING) {
                        pset->SetFrozen(partition, now);
                    } else if (state != PartitionState::P_FROZEN) {
                        if (state == PartitionState::P_CREATING && butil::fast_rand() % 2 == 0) {
                            partition->FinishCreating(now);
                            partition->FinishLoading();
                            pset->FinishCreatingPartition(partition);
                            if (pset->GetState() == PartitionSetState::PSET_NORMAL) {
                                table->FinishCreatingPartitionSet(pset->GetId());
                            }
                        } else {
                            PartitionPtr derived;
                            status = pset->DerivePartition(partition, &derived, now,
                                                           PartitionBornObjective::PBO_RECOVER);
                            ASSERT_TRUE(status.ok()) << status;

                            derived->Attach(pset.get());
                            status = table->AddPartition(derived);
                            ASSERT_TRUE(status.ok()) << status;
                            status = pset->Freeze(partition, table->GetElectionPolicy());
                            ASSERT_TRUE(status.ok()) << status;
                        }
                    }
                    partitions = pset->GetAllPartitions();
                }  // for cnt
            }      // psets

            // update partition num
            if (butil::fast_rand() % 3 == 0) {
                TableInfo table_info = table->GetInfo();
                PartitionUnit unit_info =
                    table->GetInfo().partition_units(round % table_info.partition_units_size());
                unit_info.set_id(round % table_info.partition_units_size());
                unit_info.mutable_placement_set(0)->set_tag("tag" + std::to_string(round));
                *unit_info.add_placement_set() =
                    unit_info.placement_set(butil::fast_rand() % unit_info.placement_set_size());
                unit_info.set_partition_num(unit_info.placement_set_size());
                Status status = table->UpdatePartitionUnit(unit_info);
                ASSERT_TRUE(status.ok()) << status << " " << round;
            }
        }  // round
    }

    auto proxy_loc_mgr = mb.GetProxyLocationManager();
    for (int i = 0; i < 100; i++) {
        auto proxy = std::make_shared<Proxy>(MockProxyInfo());
        proxy->SetState(ProxyState::PROXY_IDLE);
        status = proxy_loc_mgr->Add(proxy);
        ASSERT_TRUE(status.ok()) << status;
        proxy->MutableRealtimeStats()->SetLastHeartbeatTimeUs(now);
    }
    for (auto& ns : ns_mgr->List()) {
        ProxyClusterPtr cluster = ns->GetProxyCluster();
        ProxyGroupInfo info = MockProxyGroupInfo(ns->GetName());
        status = cluster->CreateOrUpdateProxyGroup(info);
        ASSERT_TRUE(status.ok()) << status;
        status = ns_mgr->GetConsulMap()->Calibrate(cluster);
        ASSERT_TRUE(status.ok()) << status;
        auto proxies = proxy_loc_mgr->List(info.placement(), [](const auto& proxy) -> bool {
            return proxy->GetState() == ProxyState::PROXY_IDLE;
        });
        ASSERT_GT(proxies.size(), 0);
        ProxyGroupPtr group;
        status = ns->GetProxyCluster()->GetProxyGroup(info.placement(), &group);
        ASSERT_TRUE(status.ok()) << status;
        for (uint32_t i = 0; i < info.instance_num() && i < proxies.size(); i++) {
            group->AddProxy(proxies[i]);
        }
    }

    std::cout << "mock done" << std::endl;

    Metabase mb_cp;
    mb_cp.Init();
    mb.DeepCopyTo(&mb_cp);
    std::cout << "cp1" << std::endl;
    ASSERT_TRUE(mb.Equal(&mb_cp));
    std::cout << "equal1" << std::endl;
    ASSERT_TRUE(mb_cp.Equal(&mb));
    std::cout << "equal2" << std::endl;
    {
        Metabase mb_cp2;
        mb_cp2.Init();

        {
            Metabase mb_cp_tmp;
            mb_cp_tmp.Init();

            mb_cp.DeepCopyTo(&mb_cp_tmp);
            mb_cp_tmp.DeepCopyTo(&mb_cp2);
        }
        ASSERT_TRUE(mb.Equal(&mb_cp2));
        ASSERT_TRUE(mb_cp2.Equal(&mb));
    }
    {
        Metabase mb_cp2;
        mb_cp2.Init();
        mb_cp.DeepCopyTo(&mb_cp2);
        std::cout << "cp2" << std::endl;
        ASSERT_TRUE(mb.Equal(&mb_cp2));
        ASSERT_TRUE(mb_cp2.Equal(&mb));
    }

    std::cout << "cp done" << std::endl;
    TempDir temp_dir;
    const std::string dir = temp_dir.GetDir() + "/snapshot";
    std::cout << "dumping " << dir << std::endl;
    status = mb_cp.DumpSnapshot(dir);
    ASSERT_TRUE(status.ok()) << status;
    std::cout << "loading " << dir << std::endl;
    Metabase mb_load;
    status = mb_load.LoadSnapshot(dir);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(mb_load.Equal(&mb));
    ASSERT_TRUE(mb.Equal(&mb_load));
}

}  // namespace bcache2::metaserver::test

