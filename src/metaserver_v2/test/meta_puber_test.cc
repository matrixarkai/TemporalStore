// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "butil/fast_rand.h"
#include "butil/logging.h"
#include "gtest/gtest.h"
#include "spdlog/fmt/fmt.h"

#include "common/logging.h"
#include "common/proto_enhance.h"
#include "metaserver_v2/meta_publisher.h"
#include "metaserver_v2/metrics.h"

namespace bcache2::metaserver::test {

Endpoint MockEndpoint(uint64_t i) {
    Endpoint ep;
    uint64_t a = i / 256 % 255 + 1;
    uint64_t b = i % 255 + 1;
    ep.set_addr_family(Endpoint::ADDR_DUAL_STACK);
    ep.set_port((i + 1) % 65536);
    ep.set_ip4(fmt::format("192.168.{}.{}", a, b));
    ep.set_ip6(fmt::format("fdbd:dc99:11:{}::{}", a, b));
    return ep;
}

TEST(MetaPuberTest, OneUnitTest) {
    InitMetrics("dev.bcache2.ut", {});
    BYTE_DEFER({ QuitMetrics(); });

    const uint32_t table_id = 1;
    const uint64_t pset_cnt = 32768;
    MetaPublisher puber;
    TableInfo table_info;
    table_info.set_id(table_id);
    table_info.set_namespace_name("ns");
    table_info.set_name("table");
    auto table_unit = table_info.add_partition_units();
    table_unit->set_id(1);
    table_unit->set_partition_num(3);
    for (uint32_t i = 0; i < table_unit->partition_num(); i++) {
        auto loc = table_unit->add_placement_set();
        std::string s = std::to_string(i);
        loc->set_vregion("region" + s);
        loc->set_vdc("vdc" + s);
        loc->set_vau("vau" + s);
    }
    Status status = puber.AddTable(table_info);
    ASSERT_TRUE(status.ok()) << status.ToString();

    for (uint64_t i = 0; i < pset_cnt; i++) {
        PartitionSetInfo info;
        info.set_id(i + 1);
        info.set_slot_range_begin(i + 1);
        info.set_slot_range_end(i + 2);
        auto ms = info.mutable_membership();
        ms->set_partition_set_version(i);
        auto unit = ms->add_units();
        unit->set_partition_unit_id(1);
        for (uint64_t j = 0; j < 3; j++) {
            const uint64_t pid = ((i + 1) << 20) + j;
            unit->add_active_id_list(pid);
            unit->set_primary_id(pid);
            auto loc = ms->add_placements();
            loc->set_id(pid);
            auto place = loc->mutable_placement();
            *place->mutable_location() = table_unit->placement_set(j);
            *place->mutable_server() = MockEndpoint(i);
        }

        Status status = puber.UpdatePartitionSet(table_id, info);
        ASSERT_TRUE(status.ok()) << status.ToString();
    }

    {
        GetTableTopoResponse response;
        Status status = puber.Query(0, "ns", "table", "region", false, &response);
        ASSERT_TRUE(status.ok()) << status.ToString();
    }
    {
        GetTableTopoResponse response;
        Status status = puber.Query(1ULL << 30, "ns", "table", "vdc1", false, &response);
        ASSERT_TRUE(!status.ok()) << status.ToString();
    }
    {
        GetTableTopoResponse response;
        Status status = puber.Query(1, "ns", "table", "vdc1", false, &response);
        std::cout << response.ByteSize() << std::endl;
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_EQ(response.partitions_size(), pset_cnt);
        for (uint64_t i = 0; i < pset_cnt; i++) {
            const auto& pset = response.partitions(i);
            ASSERT_EQ(pset.partition_id(), i + 1);
            ASSERT_GT(pset.start_slot(), 0);
            ASSERT_GT(pset.end_slot(), pset.start_slot());

            ASSERT_EQ(pset.load_infos_size(), 3);
            uint64_t primary_pid = 0;
            for (auto& p : pset.load_infos()) {
                // ASSERT_GT(p.load_version(), 0);
                ASSERT_EQ(p.partition_id() >> 20, i + 1) << p.ShortDebugString();
                if (p.is_master()) {
                    ASSERT_EQ(primary_pid, 0);
                    primary_pid = p.partition_id();
                }
                auto exp_ep = MockEndpoint(i);
                ASSERT_EQ(p.endpoint().port(), exp_ep.port());
                ASSERT_EQ(p.endpoint().ip4(), exp_ep.ip4());
                ASSERT_EQ(p.endpoint().ip6(), exp_ep.ip6());
                ASSERT_TRUE(Validate(p.location()));
            }
        }
    }
    {  // compress
        GetTableTopoResponse response;
        Status status = puber.Query(1, "ns", "table", "vdc1", true, &response);
        ASSERT_TRUE(status.ok()) << status.ToString();
        std::cout << response.ByteSize() << std::endl;
        ASSERT_EQ(response.partitions_size(), 0);
        ASSERT_FALSE(response.serialized_topo().empty());

        GetTableTopoResponse response2;
        bool y = response2.ParseFromString(response.serialized_topo());
        ASSERT_TRUE(y);
        ASSERT_EQ(response2.location_map_size(), 3);
        std::unordered_map<uint32_t, Location> id2loc;
        for (auto& loc : response2.location_map()) {
            ASSERT_TRUE(Validate(loc.location()));
            id2loc[loc.id()] = loc.location();
        }
        std::unordered_map<uint32_t, size_t> id2cnt;
        for (uint64_t i = 0; i < pset_cnt; i++) {
            const auto& pset = response2.partitions(i);
            ASSERT_EQ(pset.partition_id(), i + 1);
            ASSERT_GT(pset.start_slot(), 0);
            ASSERT_GT(pset.end_slot(), pset.start_slot());
            ASSERT_EQ(pset.compressed_load_infos_size(), 3);
            uint64_t primary_pid = 0;
            for (auto& p : pset.compressed_load_infos()) {
                // ASSERT_GT(p.load_version(), 0);
                ASSERT_EQ(p.partition_id() >> 20, i + 1) << p.ShortDebugString();
                if (p.is_master()) {
                    ASSERT_EQ(primary_pid, 0);
                    primary_pid = p.partition_id();
                }
                auto exp_ep = MockEndpoint(i);
                Endpoint got_ep;
                Status status = ToEndpoint(p.endpoint2(), &got_ep);
                ASSERT_TRUE(status.ok()) << status;
                ASSERT_EQ(got_ep.port(), exp_ep.port());
                ASSERT_EQ(got_ep.ip4(), exp_ep.ip4());
                ASSERT_EQ(got_ep.ip6(), exp_ep.ip6());
                auto iter = id2loc.find(p.location_wrapper_id());
                ASSERT_TRUE(iter != id2loc.end());
                id2cnt[p.location_wrapper_id()]++;
            }
        }
        ASSERT_EQ(id2cnt.size(), 3);
    }
}

}  // namespace bcache2::metaserver::test
