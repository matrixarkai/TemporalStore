// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/meta/table.h"

#include <algorithm>
#include <limits>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/logging.h"
#include "metaserver_v2/meta/namespace.h"

namespace bcache2 {
namespace metaserver {

Table::Table(TableInfo info) : info_(std::move(info)) {
    for (int i = 0; i < info_.partition_units_size(); i++) {
        auto unit = info_.mutable_partition_units(i);
        unit->set_id(i);
    }
}

std::string Table::GetName() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.name();
}

std::string Table::GetNamespaceName() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.namespace_name();
}

std::string Table::GetFullName() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return GetTableFullName(info_.namespace_name(), info_.name());
}

uint64_t Table::GetId() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.id();
}

void Table::SetId(uint64_t id) {
    std::lock_guard<bthread::Mutex> _(mu_);
    info_.set_id(id);
}

Namespace* Table::GetNamespace() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return ns_;
}

void Table::Attach(Namespace* ns) {
    std::lock_guard<bthread::Mutex> _(mu_);
    ns_ = ns;
}

void Table::Detach() {
    std::lock_guard<bthread::Mutex> _(mu_);
    ns_ = nullptr;
}

int64_t Table::GetFrozenTimeSec() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.frozen_at();
}

TableState Table::GetState() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.state();
}

void Table::SetState(TableState s) {
    std::lock_guard<bthread::Mutex> _(mu_);
    info_.set_state(s);
}

void Table::SetCreatingState() {
    std::lock_guard<bthread::Mutex> _(mu_);
    info_.set_state(TableState::TABLE_CREATING);
}

void Table::SetFrozenState(int64_t ts) {
    // TODO(wuzhenyu) check stats
    std::lock_guard<bthread::Mutex> _(mu_);
    info_.set_frozen_at(ts);
    info_.set_state(TableState::TABLE_FROZEN);
}

Status Table::GetPartitionSet(uint64_t pset_id, PartitionSetPtr* pset) {
    std::lock_guard<bthread::Mutex> _(mu_);
    auto iter = partition_set_map_.find(pset_id);
    if (iter == partition_set_map_.end()) {
        return Status::NotFound("");
    }
    *pset = iter->second;
    return Status::OK();
}

std::vector<PartitionSetPtr> Table::GetAllPartitionSets() {
    std::vector<PartitionSetPtr> result;
    std::lock_guard<bthread::Mutex> _(mu_);
    result.reserve(partition_set_map_.size());
    for (auto pair : partition_set_map_) {
        result.push_back(pair.second);
    }
    return result;
}

std::vector<PartitionPtr> Table::GetAllPartitions() {
    std::vector<PartitionPtr> result;
    std::lock_guard<bthread::Mutex> _(mu_);
    result.reserve(flat_partition_map_.size());
    for (auto pair : flat_partition_map_) {
        result.push_back(pair.second);
    }
    return result;
}

std::vector<PartitionPtr> Table::ListFrozenPartitions(int64_t timestamp_before) {
    std::vector<PartitionPtr> result;
    std::lock_guard<bthread::Mutex> _(mu_);
    for (auto pair : flat_partition_map_) {
        PartitionPtr partition = pair.second;
        if (partition->GetState() != PartitionState::P_FROZEN) {
            continue;
        }
        if (timestamp_before == 0 || partition->GetFrozenTimeSec() < timestamp_before) {
            result.push_back(partition);
        }
    }
    return result;
}

Status Table::GetPartition(uint64_t id, PartitionPtr* partition) {
    std::lock_guard<bthread::Mutex> _(mu_);
    auto iter = flat_partition_map_.find(id);
    if (iter == flat_partition_map_.end()) {
        return Status::NotFound("");
    }
    *partition = iter->second;
    return Status::OK();
}

Status Table::AddPartition(PartitionPtr partition) {
    const uint64_t id = partition->GetId();
    std::lock_guard<bthread::Mutex> _(mu_);
    PartitionSet* pset = partition->GetPartitionSet();
    Status status = pset->Add(partition);
    if (!status.ok()) {
        return status;
    }
    auto pair = flat_partition_map_.emplace(id, std::move(partition));
    return pair.second ? Status::OK() : Status::AlreadyExists("already exists");
}

Status Table::DropPartition(const PartitionPtr& partition) {
    CHECK(partition->GetState() == PartitionState::P_FROZEN);
    std::lock_guard<bthread::Mutex> _(mu_);
    auto pset = partition->GetPartitionSet();
    CHECK(pset);
    Status status = pset->Drop(partition);
    CHECK(status.ok()) << this;
    size_t c = flat_partition_map_.erase(partition->GetId());
    return c > 0 ? Status::OK() : Status::NotFound("not found");
}

size_t Table::GetPartitionSetNum() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.partition_set_num();
}

TableInfo Table::GetInfo() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_;
}

ElectionPolicy Table::GetElectionPolicy() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.election_policy();
}

void Table::UpdateConfig(const Config& config) {
    std::lock_guard<bthread::Mutex> _(mu_);
    Config* original = info_.mutable_config();
    const uint64_t v = original->version();
    original->MergeFrom(config);
    CHECK_LT(v, std::numeric_limits<uint64_t>::max() - 1);
    original->set_version(v + 1);
}

Config Table::GetConfig() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.config();
}

void Table::FinishCreatingPartitionSet(uint64_t pset_id) {
    std::lock_guard<bthread::Mutex> _(mu_);
    if (info_.state() == TableState::TABLE_CREATING && --creating_pset_count_ == 0) {
        info_.set_state(TableState::TABLE_NORMAL);
    }
}

Status Table::SpawnPartitionSets() {
    std::lock_guard<bthread::Mutex> _(mu_);
    uint32_t table_id = info_.id();
    for (auto pset : PartitionSetGenerator(table_id, info_.partition_set_num())) {
        pset->Attach(this);
        partition_id_t base_pid(0);
        base_pid.SetPartitionSetId(pset->GetId());
        LOG_INFO("spawn pset").put("base_pid", base_pid);
        uint32_t pidx = 0;
        for (const auto& unit : info_.partition_units()) {
            int placement_size = unit.placement_set_size();
            for (uint32_t i = 0; i < unit.partition_num(); i++, pidx++) {
                partition_id_t pid = base_pid;
                // NOTE version is from 0
                pid.SetPartitionIndex(pidx);
                PartitionInfo info;
                info.set_id(pid.id);
                *info.mutable_placement_expect() = unit.placement_set(i % placement_size);
                *info.mutable_partition_unit() = unit;
                CHECK_EQ(info.version_u64(), static_cast<uint64_t>(pid.GetPartitionVersion()));
                auto partition = std::make_shared<Partition>(std::move(info));
                partition->SetCreatingState(PartitionBornObjective::PBO_NORMAL);
                partition->Attach(pset.get());
                Status status = pset->Add(partition);
                CHECK(status.ok());
                auto pair = flat_partition_map_.emplace(pid.id, partition);
                CHECK(pair.second);
            }
        }  // units
        Status status = pset->InitMembership();
        CHECK(status.ok());

        auto pair = partition_set_map_.emplace(base_pid.GetPartitionSetId(), pset);
        CHECK(pair.second);
        ++creating_pset_count_;
    }  // partition set loop
    return Status::OK();
}

Table::PartitionLocMutPlan Table::GeneratePartitionLocMutPlan(const PartitionUnit& origin,
                                                              const PartitionUnit& update) {
    Table::PartitionLocMutPlan result;
    std::unordered_map<std::string, std::vector<Location>> origin_vdc2loc;
    for (uint32_t i = 0; i < origin.partition_num(); i++) {
        const Location& loc = origin.placement_set(i % origin.placement_set_size());
        origin_vdc2loc[loc.vregion() + loc.vdc()].push_back(loc);
    }

    std::unordered_map<std::string, std::vector<Location>> update_vdc2loc;
    for (uint32_t i = 0; i < update.partition_num(); i++) {
        const Location& loc = update.placement_set(i % update.placement_set_size());
        update_vdc2loc[loc.vregion() + loc.vdc()].push_back(loc);
    }

    for (auto& pair : update_vdc2loc) {
        const std::string& vdc = pair.first;
        size_t need = pair.second.size();
        auto iter = origin_vdc2loc.find(vdc);
        if (iter == origin_vdc2loc.end()) {
            // add
            for (auto& loc : pair.second) {
                LOG_INFO("LocMutPlan: add loc").put("loc", loc);
                result.addes.push_back(loc);
            }
        } else {
            auto& origin_vec = iter->second;
            const size_t update_count = std::min(iter->second.size(), need);
            size_t i = 0;
            for (; i < update_count; i++) {
                LOG_INFO("LocMutPlan: update loc")
                    .put("from", origin_vec[i])
                    .put("to", pair.second[i]);
                // include same
                result.updates.push_back(std::make_pair(origin_vec[i], pair.second[i]));
            }
            if (need > update_count) {
                // add
                for (; i < need; i++) {
                    LOG_INFO("LocMutPlan: add loc").put("loc", pair.second[i]);
                    result.addes.push_back(pair.second[i]);
                }
            } else {
                // remove
                for (; i < origin_vec.size(); i++) {
                    LOG_INFO("LocMutPlan: remove loc").put("loc", origin_vec[i]);
                    result.removes.push_back(origin_vec[i]);
                }
            }
        }
    }
    for (auto& pair : origin_vdc2loc) {
        const std::string& vdc = pair.first;
        auto iter = update_vdc2loc.find(vdc);
        if (iter == update_vdc2loc.end()) {
            // remove
            for (auto& loc : pair.second) {
                LOG_INFO("LocMutPlan: remove loc").put("loc", loc);
                result.removes.push_back(loc);
            }
        }
    }
    CHECK_EQ(update.partition_num(), result.updates.size() + result.addes.size());
    return result;
}

Status Table::UpdatePartitionUnit(const PartitionUnit& unit) {
    std::lock_guard<bthread::Mutex> _(mu_);
    PartitionUnit* unit_ptr = nullptr;
    for (int i = 0; i < info_.partition_units_size(); i++) {
        PartitionUnit* u = info_.mutable_partition_units(i);
        if (u->id() == unit.id()) {
            unit_ptr = u;
            break;
        }
    }
    if (unit_ptr == nullptr) {
        return Status::FailedPrecondition("unit not found");
    }

    PartitionUnit original_unit = *unit_ptr;
    *unit_ptr = unit;
    // Note: be careful for immutable fields
    unit_ptr->set_storage_pool_uri(original_unit.storage_pool_uri());
    PartitionLocMutPlan loc_mut_plan = GeneratePartitionLocMutPlan(original_unit, unit);
    for (auto iter : partition_set_map_) {
        auto pset = iter.second;
        std::unordered_map<Location, std::list<PartitionPtr>, LocationHash> loc2p;
        // Note: this container includes all freezing and creating partitions
        std::vector<PartitionPtr> partitions = pset->GetPartitions(unit.id());
        for (auto& p : partitions) {
            loc2p[p->GetPlacementExpect()].push_back(p);
        }
        for (auto& pair : loc_mut_plan.updates) {
            auto& candidates = loc2p[pair.first];
            LOG_INFO("try to update partition loc")
                .put("from", to_string(pair.first))
                .put("to", to_string(pair.second))
                .put("candidates", candidates.size());
            PartitionPtr p = nullptr;
            while (!candidates.empty()) {
                p = candidates.front();
                candidates.pop_front();

                // all partition need to be updated to avoid
                // deriving legacy placement info
                p->SetPartitionUnitInfo(*unit_ptr);

                PartitionState state = p->GetState();
                if (state != PartitionState::P_FREEZING && state != PartitionState::P_FROZEN) {
                    break;
                }
                p.reset();
            }
            CHECK(p.get() != nullptr);
            if (!p) {
                return Status::Internal("update partition not found");
            }
            if (!IsSame(p->GetPlacementExpect(), pair.second)) {
                LOG_INFO("update partition loc success")
                    .put("partition", *p)
                    .put("from", to_string(p->GetPlacementExpect()))
                    .put("to", to_string(pair.second));
                p->SetPlacementExpect(pair.second);
            }
        }

        // TODO(wuzhenyu): FIXME partition index has ABA problem
        partition_id_t base_pid(0);
        base_pid.SetPartitionSetId(pset->GetId());
        for (auto& loc : loc_mut_plan.addes) {
            uint32_t pidx = 0;
            Status status = pset->AcquireNewPartitionIndex(&pidx);
            CHECK(status.ok()) << this << status;
            if (!status.ok()) {
                LOG_ERROR("failed to acquire partition index").put("result", status);
                return status;
            }
            partition_id_t pid = base_pid;
            pid.SetPartitionIndex(pidx);
            PartitionInfo info;
            info.set_id(pid.id);
            *info.mutable_placement_expect() = loc;
            *info.mutable_partition_unit() = *unit_ptr;
            CHECK_EQ(info.version_u64(), static_cast<uint64_t>(pid.GetPartitionVersion()));
            auto partition = std::make_shared<Partition>(std::move(info));
            LOG_INFO("add partition").put("partition", *partition).put("loc", to_string(loc));
            partition->SetCreatingState(PartitionBornObjective::PBO_NORMAL);
            partition->Attach(pset.get());
            status = pset->Add(partition);
            CHECK(status.ok()) << this << status;
            if (!status.ok()) {
                LOG_ERROR("failed to add partition").put("result", status);
                return status;
            }
            auto pair = flat_partition_map_.emplace(pid.id, partition);
            CHECK(pair.second);
        }
        for (auto& loc : loc_mut_plan.removes) {
            auto& candidates = loc2p[loc];
            LOG_INFO("try to remove partition")
                .put("candidates", candidates.size())
                .put("loc", to_string(loc));
            bool y = false;
            while (!candidates.empty() && !y) {
                auto p = candidates.front();
                candidates.pop_front();
                PartitionState state = p->GetState();
                if (state != PartitionState::P_CREATING && state != PartitionState::P_LOADING &&
                    state != PartitionState::P_NORMAL) {
                    continue;
                }
                if (p->IsPrimary() && info_.election_policy() == ElectionPolicy::PROMOTE_DERIVED) {
                    LOG_INFO("skip to remove partition due to primary role")
                        .put("partition", *p)
                        .put("loc", to_string(loc));
                    continue;
                }

                LOG_INFO("freeze partition").put("partition", *p).put("loc", to_string(loc));
                Status status = pset->Freeze(p, info_.election_policy());
                CHECK(status.ok()) << this << status;
                if (!status.ok()) {
                    LOG_ERROR("failed to freeze partition").put("result", status);
                    return status;
                }

                y = true;
            }  // while candidates
            CHECK(y) << this;
            if (!y) {
                return Status::Internal("remove partition failed");
            }
        }  // remove
    }
    return Status::OK();
}

Status Table::FreezeAllPartitions() {
    std::lock_guard<bthread::Mutex> _(mu_);
    uint32_t table_id = info_.id();
    CHECK(info_.state() == TableState::TABLE_FROZEN);

    LOG_WARNING("start to drop all partition").put("table_id", table_id);
    for (auto iter : partition_set_map_) {
        auto pset = iter.second;
        pset->SetState(PartitionSetState::PSET_FROZEN);
        Status status = pset->FreezeAllPartitions();
        CHECK(status.ok()) << this;
        pset->IncrMembershipGlobalVersion();
    }
    return Status::OK();
}

Status Table::DropAllPartitions() {
    std::lock_guard<bthread::Mutex> _(mu_);
    uint32_t table_id = info_.id();
    CHECK(info_.state() == TableState::TABLE_FROZEN);
    LOG_WARNING("start to drop all partition").put("table_id", table_id);
    for (auto iter : partition_set_map_) {
        auto pset = iter.second;
        Status status = pset->DropAllPartitions();
        CHECK(status.ok()) << this;
        pset->Detach();
    }
    partition_set_map_.clear();
    flat_partition_map_.clear();
    return Status::OK();
}

bool Table::CanFreezePartitionSafely(uint64_t id) {
    PartitionPtr partition;
    Status status = GetPartition(id, &partition);
    if (!status.ok() || partition->GetState() != PartitionState::P_NORMAL) {
        return true;
    }
    PartitionSet* pset = partition->GetPartitionSet();
    if (pset == nullptr) {
        return true;
    }
    std::vector<PartitionPtr> silbings = pset->GetSilblings(partition);
    if (silbings.size() <= 1) {
        // it's safe by design
        return true;
    }
    size_t normal_count = 0;
    for (auto& silbing : silbings) {
        if (silbing->GetId() == id) {
            continue;
        }
        if (silbing->GetState() == PartitionState::P_NORMAL && ++normal_count > 0) {
            return true;
        }
    }
    return false;
}

bool Table::Equal(DeepCopy* rhs_base) {
    if (rhs_base == nullptr) {
        return false;
    }
    auto rhs = static_cast<Table*>(rhs_base);
    std::lock_guard<bthread::Mutex> _(mu_);
    if (!ProtoEqual(info_, rhs->info_)) {
        LOG_INFO("proto info not equal");
        return false;
    }
    if (creating_pset_count_ != rhs->creating_pset_count_) {
        LOG_INFO("creating pset count not equal")
            .put("mine", creating_pset_count_)
            .put("rhs", rhs->creating_pset_count_);
        return false;
    }
    if (ns_ != nullptr && rhs->ns_ == nullptr) {
        LOG_INFO("ns attach not equal");
        return false;
    }
    if (!MapEqual(partition_set_map_, rhs->partition_set_map_)) {
        LOG_INFO("pset map not equal");
        return false;
    }
    if (!MapEqual(flat_partition_map_, rhs->flat_partition_map_)) {
        LOG_INFO("flat partition not equal");
        return false;
    }
    return true;
}

void Table::DeepCopyTo(DeepCopy* rhs_base) {
    auto rhs = static_cast<Table*>(rhs_base);
    std::lock_guard<bthread::Mutex> _(mu_);
    rhs->info_ = info_;
    rhs->creating_pset_count_ = creating_pset_count_;
    for (auto& pair : partition_set_map_) {
        PartitionSetPtr pset = std::make_shared<PartitionSet>(pair.first);
        pair.second->DeepCopyTo(pset.get());
        pset->Attach(rhs);
        rhs->partition_set_map_[pair.first] = std::move(pset);
    }
    rhs->ConstructFlatPartitionMap();
}

void Table::ConstructFlatPartitionMap() {
    // Note: no lock protection here
    for (auto& pair : partition_set_map_) {
        for (auto& p : pair.second->GetAllPartitions()) {
            flat_partition_map_[p->GetId()] = p;
        }
    }
}

Status Table::DumpSnapshot(google::protobuf::io::FileOutputStream* stream) {
    Status status = PackToStream(stream);
    RETURN_IF_STATUS_ERROR(status);

    LOG_INFO("start to dump all pset");
    std::vector<PartitionSetPtr> psets = GetAllPartitionSets();
    for (auto& pset : psets) {
        LOG_INFO("start to dump pset").put("pset_id", pset->GetId());
        status = pset->DumpSnapshot(stream);
        RETURN_IF_STATUS_ERROR(status);
    }
    return Status::OK();
}

Status Table::LoadSnapshot(google::protobuf::io::FileInputStream* stream) {
    Status status = UnPackFromStream(stream);
    RETURN_IF_STATUS_ERROR(status);
    TableInfo info = GetInfo();
    LOG_INFO("loading table snapshot").put("table_id", info.id());
    int creating_count = 0;
    for (uint32_t i = 0; i < info.partition_set_num(); i++) {
        auto pset = std::make_shared<PartitionSet>(0);
        status = pset->LoadSnapshot(stream);
        RETURN_IF_STATUS_ERROR(status);
        pset->Attach(this);
        LOG_INFO("loaded partition set snapshot").put("pset_id", pset->GetId());
        PartitionSetState state = pset->GetState();
        if (state == PartitionSetState::PSET_CREATING) {
            creating_count++;
        }

        std::lock_guard<bthread::Mutex> _(mu_);
        partition_set_map_[pset->GetId()] = pset;
        for (auto& p : pset->GetAllPartitions()) {
            flat_partition_map_[p->GetId()] = p;
        }
    }

    LOG_INFO("loaded table snapshot").put("table_id", info.id());
    std::lock_guard<bthread::Mutex> _(mu_);
    if (info_.state() == TableState::TABLE_CREATING) {
        creating_pset_count_ = creating_count;
    }
    return Status::OK();
}

}  // namespace metaserver
}  // namespace bcache2

