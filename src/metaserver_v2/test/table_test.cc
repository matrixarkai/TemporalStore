// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "butil/fast_rand.h"
#include "butil/logging.h"
#include "gtest/gtest.h"

#include "common/logging.h"
#include "metaserver_v2/meta/namespace.h"
#include "metaserver_v2/meta/partition.h"
#include "metaserver_v2/meta/table.h"

namespace logging {
DECLARE_bool(crash_on_fatal_log);
}

namespace bcache2::metaserver::test {

/// make me happy
class TableTest : public testing::Test {};

static TableInfo MockTableInfo() {
    TableInfo info;
    info.set_name("table1");
    info.set_namespace_name("ns1");
    info.set_partition_set_num(8192);
    for (int i = 0; i < 3; i++) {
        auto unit = info.add_partition_units();
        const int cnt = 3;
        unit->set_partition_num(cnt);
        for (int j = 0; j < cnt; j++) {
            auto loc = unit->add_placement_set();
            loc->set_vregion("vregion" + std::to_string(i));
            loc->set_vdc("vdc" + std::to_string(j / 4));
            loc->set_vau("vau" + std::to_string(j / 2));
        }
        unit->set_storage_pool_uri("local://data00/x");
    }
    auto quota = info.mutable_quota();
    quota->set_ops_read(10'000);
    quota->set_ops_write(10'000);

    return info;
}

std::unique_ptr<NamespaceManager> PrepareNsMgr() {
    auto ns_mgr = std::make_unique<NamespaceManager>();
    NamespaceInfo info;
    info.set_name("ns1");
    auto consul_info = info.add_consul_info();
    consul_info->set_name("dev.a.b.c");
    consul_info->set_vdc("vdc1");
    NamespacePtr ns = std::make_shared<Namespace>(info);
    Status status = ns_mgr->Add(ns);
    if (!status.ok()) {
        return nullptr;
    }
    return ns_mgr;
}

TEST_F(TableTest, ValidateInfoTest) {
    Status status;
    auto mgr = PrepareNsMgr();
    ASSERT_TRUE(mgr.get() != nullptr);
    {
        TableInfo info;
        status = mgr->ValidateTableInfo(info);
        ASSERT_FALSE(status.ok());
    }
    {
        TableInfo info = MockTableInfo();
        status = mgr->ValidateTableInfo(info);
        ASSERT_TRUE(status.ok()) << status;
    }
    {
        TableInfo info = MockTableInfo();
        info.clear_name();
        status = mgr->ValidateTableInfo(info);
        ASSERT_FALSE(status.ok());
    }
    {
        TableInfo info = MockTableInfo();
        info.clear_namespace_name();
        status = mgr->ValidateTableInfo(info);
        ASSERT_FALSE(status.ok());
    }
    {
        TableInfo info = MockTableInfo();
        info.clear_quota();
        status = mgr->ValidateTableInfo(info);
        ASSERT_FALSE(status.ok());
    }
    {
        TableInfo info = MockTableInfo();
        auto unit = info.mutable_partition_units(0);
        unit->clear_placement_set();
        status = mgr->ValidateTableInfo(info);
        ASSERT_FALSE(status.ok());
    }
    {
        TableInfo info = MockTableInfo();
        auto unit = info.mutable_partition_units(0);
        unit->clear_storage_pool_uri();
        status = mgr->ValidateTableInfo(info);
        ASSERT_FALSE(status.ok());
    }
}

TEST_F(TableTest, CurdTest) {
    int64_t now = butil::gettimeofday_s();
    Status status;
    auto mgr = PrepareNsMgr();
    ASSERT_TRUE(mgr.get() != nullptr);
    TableInfo info = MockTableInfo();
    TablePtr table = std::make_shared<Table>(info);
    {  // create
        status = mgr->AddTable(table);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(table->GetId(), 1);
        ASSERT_NE(table->GetNamespace(), nullptr);
        ASSERT_EQ(table->GetState(), TableState::TABLE_CREATING);
        ASSERT_EQ(table->GetPartitionSetNum(), info.partition_set_num());

        size_t partition_cnt_per_set_exp = 0;
        for (auto unit : info.partition_units()) {
            partition_cnt_per_set_exp += unit.partition_num();
        }
        size_t total_partition_cnt_exp = info.partition_set_num() * partition_cnt_per_set_exp;

        std::vector<PartitionSetPtr> psets = table->GetAllPartitionSets();
        ASSERT_EQ(psets.size(), info.partition_set_num());
        for (auto& pset : psets) {
            ASSERT_EQ(pset->GetTable(), table.get());
            ASSERT_EQ(pset->GetState(), PartitionSetState::PSET_CREATING);
            std::vector<PartitionPtr> partitions = pset->GetAllPartitions();
            ASSERT_EQ(partitions.size(), partition_cnt_per_set_exp);
            std::set<uint64_t> primary_ids;
            for (auto p : partitions) {
                if (p->IsPrimary()) {
                    primary_ids.insert(p->GetId());
                }
                auto primary = pset->GetPrimary(p);
                ASSERT_NE(primary.get(), nullptr);
                auto silblings = pset->GetSilblings(p);
                ASSERT_GT(silblings.size(), 1);
            }

            ASSERT_EQ(primary_ids.size(), info.partition_units_size());
        }

        std::vector<PartitionPtr> partitions = table->GetAllPartitions();
        ASSERT_EQ(partitions.size(), total_partition_cnt_exp);
    }
    {  // finish create
        std::vector<PartitionPtr> partitions = table->GetAllPartitions();
        for (auto partition : partitions) {
            partition->FinishCreating(now);
            partition->FinishLoading();
            auto pset = partition->GetPartitionSet();
            pset->FinishCreatingPartition(partition);
            if (pset->GetState() == PartitionSetState::PSET_NORMAL) {
                Table* table = pset->GetTable();
                table->FinishCreatingPartitionSet(pset->GetId());
            }
        }
        ASSERT_EQ(table->GetState(), TableState::TABLE_NORMAL);
        std::vector<PartitionSetPtr> psets = table->GetAllPartitionSets();
        ASSERT_EQ(psets.size(), info.partition_set_num());
        for (auto& pset : psets) {
            ASSERT_EQ(pset->GetState(), PartitionSetState::PSET_NORMAL);
            for (auto p : pset->GetAllPartitions()) {
                ASSERT_EQ(p->GetState(), PartitionState::P_NORMAL);
            }
        }
    }
    {  // recover
        auto psets = table->GetAllPartitionSets();
        PartitionSetPtr pset = psets[butil::fast_rand() % psets.size()];
        auto partitions = pset->GetAllPartitions();
        size_t cnt = partitions.size() * 10;
        for (size_t i = 0; i < cnt; i++) {
            auto partition = partitions[butil::fast_rand() % partitions.size()];
            auto state = partition->GetState();
            if (state == PartitionState::P_FREEZING) {
                pset->SetFrozen(partition, now);
            } else {
                if (state == PartitionState::P_CREATING && butil::fast_rand() % 2 == 0) {
                    partition->FinishCreating(now);
                    partition->FinishLoading();
                    pset->FinishCreatingPartition(partition);
                    if (pset->GetState() == PartitionSetState::PSET_NORMAL) {
                        Table* table = pset->GetTable();
                        table->FinishCreatingPartitionSet(pset->GetId());
                    }
                } else if (state != PartitionState::P_FROZEN) {
                    PartitionPtr derived;
                    status = pset->DerivePartition(partition, &derived, now,
                                                   PartitionBornObjective::PBO_RECOVER);
                    ASSERT_TRUE(status.ok()) << status;

                    derived->Attach(pset.get());
                    Table* table = pset->GetTable();
                    status = table->AddPartition(derived);
                    ASSERT_TRUE(status.ok()) << status;
                    status = pset->Freeze(partition, table->GetElectionPolicy());
                    ASSERT_TRUE(status.ok()) << status;
                }
            }
            partitions = pset->GetAllPartitions();
        }  // for cnt
        // clean up
        for (auto partition : pset->GetAllPartitions()) {
            auto state = partition->GetState();
            if (state == PartitionState::P_FROZEN) {
                continue;
            }
            if (state == PartitionState::P_FREEZING) {
                pset->SetFrozen(partition, now);
            } else if (state == PartitionState::P_CREATING) {
                partition->FinishCreating(now);
                partition->FinishLoading();
                pset->FinishCreatingPartition(partition);
                if (pset->GetState() == PartitionSetState::PSET_NORMAL) {
                    Table* table = pset->GetTable();
                    table->FinishCreatingPartitionSet(pset->GetId());
                }
            } else {
                ASSERT_EQ(state, PartitionState::P_NORMAL) << PartitionState_Name(state);
            }
        }
        for (auto partition : pset->GetAllFrozenPartitions()) {
            auto state = partition->GetState();
            ASSERT_EQ(state, PartitionState::P_FROZEN);
            pset->Drop(partition);
        }
        ASSERT_TRUE(pset->GetAllFrozenPartitions().empty());

        std::set<uint64_t> pids;
        partitions = pset->GetAllPartitions();
        for (auto p : partitions) {
            pids.insert(p->GetId());
        }
        MembershipInfo mship = pset->GetMembershipInfo();
        for (int i = 0; i < info.partition_units_size(); i++) {
            auto unit = info.partition_units(i);
            auto munit = mship.units(i);
            ASSERT_EQ(munit.active_id_list_size(), unit.partition_num());
            ASSERT_EQ(munit.frozen_id_list_size(), 0);
            for (auto id : munit.active_id_list()) {
                ASSERT_EQ(pids.count(id), 1) << id;
            }
            ASSERT_EQ(pids.count(munit.primary_id()), 1) << munit.primary_id();
        }
    }
}

TEST_F(TableTest, PartitionLocMutPlanTest) {
    const size_t count = 3;
    PartitionUnit unit;
    unit.set_id(1);
    unit.set_partition_num(count);
    for (size_t i = 0; i < count; i++) {
        auto loc = unit.add_placement_set();
        loc->set_vregion("vregion");
        loc->set_vdc("vdc" + std::to_string(i));
        loc->set_vau("vau" + std::to_string(i));
    }

    {  // only update tag
        PartitionUnit update = unit;
        update.mutable_placement_set(0)->set_tag("tag");
        auto plan = Table::GeneratePartitionLocMutPlan(unit, update);
        ASSERT_EQ(plan.updates.size(), count);
        for (auto upd : plan.updates) {
            ASSERT_TRUE(InSameVdc(upd.first, upd.second));
        }
    }
    {  // update tag + change vdc
        PartitionUnit update = unit;
        update.mutable_placement_set(0)->set_tag("tag");
        update.mutable_placement_set(1)->set_vdc("vdc");
        auto plan = Table::GeneratePartitionLocMutPlan(unit, update);
        ASSERT_EQ(plan.updates.size(), 2);  // include 1 not changing
        ASSERT_EQ(plan.addes.size(), 1);
        ASSERT_EQ(plan.removes.size(), 1);
        for (auto& upd : plan.updates) {
            ASSERT_TRUE(InSameVdc(upd.first, upd.second));
        }
        ASSERT_EQ(plan.addes[0].vdc(), "vdc");
        ASSERT_EQ(plan.removes[0].vdc(), "vdc1");
    }
    {  // update tag + change vdc + add partition
        PartitionUnit update = unit;
        update.set_partition_num(count + 1);
        update.mutable_placement_set(0)->set_tag("tag");
        update.mutable_placement_set(1)->set_vdc("vdc");
        auto added = update.add_placement_set();
        added->set_vregion("vregion");
        added->set_vdc("vdc_new");
        auto plan = Table::GeneratePartitionLocMutPlan(unit, update);
        ASSERT_EQ(plan.updates.size(), 2);  // include 1 not changing
        ASSERT_EQ(plan.addes.size(), 2);    // include 1 new
        ASSERT_EQ(plan.removes.size(), 1);
        for (auto& upd : plan.updates) {
            ASSERT_TRUE(InSameVdc(upd.first, upd.second));
        }
        size_t exp_added_count = 0;
        for (auto& add : plan.addes) {
            if (add.vdc() == "vdc" || add.vdc() == "vdc_new") {
                exp_added_count++;
            }
        }
        ASSERT_EQ(exp_added_count, 2);
        ASSERT_EQ(plan.removes[0].vdc(), "vdc1");
    }
    {  // update vau + change vdc + remove partition
        PartitionUnit update = unit;
        update.set_partition_num(count - 1);
        auto iter = update.placement_set().begin();
        update.mutable_placement_set()->erase(iter);
        update.mutable_placement_set(0)->set_vdc("vdc");
        update.mutable_placement_set(1)->clear_vau();
        auto plan = Table::GeneratePartitionLocMutPlan(unit, update);
        ASSERT_EQ(plan.updates.size(), 1);
        ASSERT_EQ(plan.addes.size(), 1);
        ASSERT_EQ(plan.removes.size(), 2);
        for (auto& upd : plan.updates) {
            ASSERT_TRUE(InSameVdc(upd.first, upd.second));
        }
        ASSERT_EQ(plan.addes[0].vdc(), "vdc");
        size_t exp_removed_count = 0;
        for (auto& r : plan.removes) {
            if (r.vdc() == "vdc0" || r.vdc() == "vdc1") {
                exp_removed_count++;
            }
        }
        ASSERT_EQ(exp_removed_count, 2);
    }
}

TEST_F(TableTest, ChangePartitionNumSimpleReduce) {
    int64_t now = butil::gettimeofday_s();
    Status status;
    auto mgr = PrepareNsMgr();
    ASSERT_TRUE(mgr.get() != nullptr);
    TableInfo info;
    info.set_name("table1");
    info.set_namespace_name("ns1");
    info.set_partition_set_num(2);
    auto unit = info.add_partition_units();
    unit->set_partition_num(4);
    for (int j = 0; j < 4; j++) {
        auto loc = unit->add_placement_set();
        loc->set_vregion("vregionx");
        loc->set_vdc("vdc" + std::to_string(j / 2));
    }
    unit->set_storage_pool_uri("local://data00/x");
    auto quota = info.mutable_quota();
    quota->set_ops_read(10'000);

    TablePtr table = std::make_shared<Table>(info);
    status = mgr->AddTable(table);
    ASSERT_TRUE(status.ok());

    {  // finish create
        std::vector<PartitionPtr> partitions = table->GetAllPartitions();
        for (auto partition : partitions) {
            partition->FinishCreating(now);
            partition->FinishLoading();
            auto pset = partition->GetPartitionSet();
            pset->FinishCreatingPartition(partition);
            if (pset->GetState() == PartitionSetState::PSET_NORMAL) {
                Table* table = pset->GetTable();
                table->FinishCreatingPartitionSet(pset->GetId());
            }
        }
    }
    {
        // update partition num
        TableInfo table_info = table->GetInfo();
        PartitionUnit unit_info = table->GetInfo().partition_units(0);
        auto iter = unit_info.placement_set().begin();
        unit_info.mutable_placement_set()->erase(iter);
        unit_info.set_partition_num(3);
        Status status = table->UpdatePartitionUnit(unit_info);
        ASSERT_TRUE(status.ok()) << status;
        // validate
        table_info = table->GetInfo();
        for (auto& unit_info : table_info.partition_units()) {
            auto psets = table->GetAllPartitionSets();
            for (auto& pset : psets) {
                for (auto partition : pset->GetPartitions(unit_info.id())) {
                    auto state = partition->GetState();
                    if (state == PartitionState::P_FREEZING) {
                        pset->SetFrozen(partition, now);
                    } else if (state == PartitionState::P_CREATING) {
                        partition->FinishCreating(now);
                        partition->FinishLoading();
                    } else {
                        ASSERT_EQ(state, PartitionState::P_NORMAL) << PartitionState_Name(state);
                    }
                }
            }
            psets = table->GetAllPartitionSets();
            for (auto& pset : psets) {
                std::map<uint32_t, Location> idx2loc;
                size_t count = 0;
                for (auto partition : pset->GetPartitions(unit_info.id())) {
                    auto state = partition->GetState();
                    if (state == PartitionState::P_FROZEN) {
                        continue;
                    }
                    ASSERT_EQ(state, PartitionState::P_NORMAL);
                    partition_id_t pid(partition->GetId());
                    ASSERT_EQ(idx2loc.count(pid.GetPartitionIndex()), 0);
                    idx2loc[pid.GetPartitionIndex()] = partition->GetPlacementExpect();
                    count++;
                }
                ASSERT_EQ(count, 3);
                std::cout << count << std::endl;
                for (auto pair : idx2loc) {
                    std::cout << pair.first << " " << to_string(pair.second) << std::endl;
                }
            }
        }
    }
    {
        // update partition num
        TableInfo table_info = table->GetInfo();
        PartitionUnit unit_info = table->GetInfo().partition_units(0);
        auto iter = unit_info.placement_set().begin();
        unit_info.mutable_placement_set()->erase(iter);
        unit_info.set_partition_num(2);
        logging::FLAGS_crash_on_fatal_log = false;
        Status status = table->UpdatePartitionUnit(unit_info);
        ASSERT_TRUE(!status.ok()) << status;
        logging::FLAGS_crash_on_fatal_log = true;
    }
}

TEST_F(TableTest, ChangePartitionNum) {
    int64_t now = butil::gettimeofday_s();
    Status status;
    auto mgr = PrepareNsMgr();
    ASSERT_TRUE(mgr.get() != nullptr);
    TableInfo info = MockTableInfo();
    info.set_partition_set_num(2);
    TablePtr table = std::make_shared<Table>(info);
    status = mgr->AddTable(table);
    ASSERT_TRUE(status.ok());

    {  // finish create
        std::vector<PartitionPtr> partitions = table->GetAllPartitions();
        for (auto partition : partitions) {
            partition->FinishCreating(now);
            partition->FinishLoading();
            auto pset = partition->GetPartitionSet();
            pset->FinishCreatingPartition(partition);
            if (pset->GetState() == PartitionSetState::PSET_NORMAL) {
                Table* table = pset->GetTable();
                table->FinishCreatingPartitionSet(pset->GetId());
            }
        }
    }
    auto psets = table->GetAllPartitionSets();
    for (auto& pset : psets) {
        auto partitions = pset->GetAllPartitions();
        std::map<uint32_t, Location> idx2loc;
        for (auto& p : partitions) {
            partition_id_t pid(p->GetId());
            ASSERT_EQ(idx2loc.count(pid.GetPartitionIndex()), 0);
            idx2loc[pid.GetPartitionIndex()] = p->GetPlacementExpect();
        }
        for (auto pair : idx2loc) {
            std::cout << pair.first << " " << to_string(pair.second) << std::endl;
        }
    }
    for (int round = 0; round < 10; round++) {
        // inject chaos
        std::cout << "inject chaos " << round << std::endl;
        auto psets = table->GetAllPartitionSets();
        for (auto& pset : psets) {
            auto partitions = pset->GetAllPartitions();
            size_t cnt = 300;
            for (size_t i = 0; i < cnt; i++) {
                auto partition = partitions[butil::fast_rand() % partitions.size()];
                auto state = partition->GetState();
                if (state == PartitionState::P_FREEZING) {
                    pset->SetFrozen(partition, now);
                } else {
                    if (state == PartitionState::P_CREATING && butil::fast_rand() % 2 == 0) {
                        partition->FinishCreating(now);
                        partition->FinishLoading();
                    } else if (state != PartitionState::P_FROZEN) {
                        PartitionPtr derived;
                        status = pset->DerivePartition(partition, &derived, now,
                                                       PartitionBornObjective::PBO_RECOVER);
                        ASSERT_TRUE(status.ok()) << status;

                        derived->Attach(pset.get());
                        Table* table = pset->GetTable();
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
        TableInfo table_info = table->GetInfo();
        PartitionUnit unit_info =
            table->GetInfo().partition_units(round % table_info.partition_units_size());
        unit_info.set_id(round % table_info.partition_units_size());
        unit_info.mutable_placement_set(0)->set_tag("tag" + std::to_string(round));
        unit_info.mutable_placement_set(2)->set_vdc("vdcx" + std::to_string(round));
        *unit_info.add_placement_set() =
            unit_info.placement_set(butil::fast_rand() % unit_info.placement_set_size());
        unit_info.set_partition_num(unit_info.placement_set_size());
        Status status = table->UpdatePartitionUnit(unit_info);
        ASSERT_TRUE(status.ok()) << status << " " << round;
    }  // round
    // validate
    TableInfo table_info = table->GetInfo();
    for (auto& unit_info : table_info.partition_units()) {
        for (auto& pset : psets) {
            for (auto partition : pset->GetPartitions(unit_info.id())) {
                auto state = partition->GetState();
                if (state == PartitionState::P_FROZEN) {
                    continue;
                }
                if (state == PartitionState::P_FREEZING) {
                    pset->SetFrozen(partition, now);
                } else if (state == PartitionState::P_CREATING) {
                    partition->FinishCreating(now);
                    partition->FinishLoading();
                } else {
                    ASSERT_EQ(state, PartitionState::P_NORMAL) << PartitionState_Name(state);
                }
            }
        }
        for (auto& pset : psets) {
            std::map<uint32_t, Location> idx2loc;
            size_t count = 0;
            for (auto partition : pset->GetPartitions(unit_info.id())) {
                auto state = partition->GetState();
                if (state == PartitionState::P_FROZEN) {
                    continue;
                }
                ASSERT_EQ(state, PartitionState::P_NORMAL);
                partition_id_t pid(partition->GetId());
                ASSERT_EQ(idx2loc.count(pid.GetPartitionIndex()), 0);
                idx2loc[pid.GetPartitionIndex()] = partition->GetPlacementExpect();
                count++;
            }
            ASSERT_GT(count, 3);
            std::cout << count << std::endl;
            for (auto pair : idx2loc) {
                std::cout << pair.first << " " << to_string(pair.second) << std::endl;
            }
        }
    }
}

}  // namespace bcache2::metaserver::test
