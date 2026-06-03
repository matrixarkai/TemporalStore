// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/meta/partition.h"

#include <algorithm>
#include <limits>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "metaserver_v2/meta/table.h"
#include "metaserver_v2/utils.h"

namespace bcache2 {
namespace metaserver {

Partition::Partition(PartitionInfo info) : info_(std::move(info)) {}
Partition::~Partition() {
    auto node = node_.lock();
    if (node) {
        node->RemovePartition(info_.id());
    }
}

uint64_t Partition::GetId() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.id();
}

void Partition::Attach(PartitionSet* pset) {
    std::lock_guard<bthread::Mutex> _(mu_);
    partition_set_ = pset;
}

void Partition::Detach() {
    std::lock_guard<bthread::Mutex> _(mu_);
    partition_set_ = nullptr;
}

PartitionSet* Partition::GetPartitionSet() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return partition_set_;
}

void Partition::SetCreatingState(PartitionBornObjective pbo) {
    std::lock_guard<bthread::Mutex> _(mu_);
    info_.set_state(PartitionState::P_CREATING);
    info_.set_born_objective(pbo);
}

void Partition::SetState(PartitionState s) {
    std::lock_guard<bthread::Mutex> _(mu_);
    info_.set_state(s);
}

void Partition::SetFrozenState(int64_t ts) {
    std::lock_guard<bthread::Mutex> _(mu_);
    info_.set_frozen_at(ts);
    info_.set_state(PartitionState::P_FROZEN);
}

int64_t Partition::GetFrozenTimeSec() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.frozen_at();
}

void Partition::FinishCreating(int64_t ts) {
    std::lock_guard<bthread::Mutex> _(mu_);
    CHECK(info_.state() == PartitionState::P_CREATING) << this;
    info_.set_created_at(ts);
    info_.set_state(PartitionState::P_LOADING);
}

void Partition::FinishLoading() {
    std::lock_guard<bthread::Mutex> _(mu_);
    CHECK(info_.state() == PartitionState::P_LOADING) << this;
    info_.set_state(PartitionState::P_NORMAL);
}

PartitionState Partition::GetState() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.state();
}

PartitionBornObjective Partition::GetBornObjective() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.born_objective();
}

PartitionInfo Partition::GetInfo() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_;
}

Location Partition::GetPlacementExpect() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.placement_expect();
}

void Partition::SetPlacementExpect(Location loc) {
    std::lock_guard<bthread::Mutex> _(mu_);
    *info_.mutable_placement_expect() = std::move(loc);
}

void Partition::SetPlacementActual(PlacementSpec spec, NodePtr node) {
    std::lock_guard<bthread::Mutex> _(mu_);
    *info_.mutable_placement_actual() = std::move(spec);
    node_ = std::move(node);
}

const PlacementSpec& Partition::GetPlacementActual() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.placement_actual();
}

NodePtr Partition::GetNode() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return node_.lock();
}

PartitionUnit Partition::GetPartitionUnitInfo() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.partition_unit();
}

void Partition::SetPartitionUnitInfo(PartitionUnit unit) {
    std::lock_guard<bthread::Mutex> _(mu_);
    *info_.mutable_partition_unit() = std::move(unit);
}

bool Partition::IsPrimary() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.role() == PartitionRole::PARTITION_ROLE_PRIMARY;
}

PartitionRole Partition::GetRole() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.role();
}

void Partition::SetRole(PartitionRole role) {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.set_role(role);
}

void Partition::SetDerived(PartitionPtr p) {
    std::lock_guard<bthread::Mutex> _(mu_);
    derived_ = p;
    if (p) {
        info_.set_derived_id(p->GetId());
    }
}

PartitionPtr Partition::GetDerived() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return derived_;
}

void Partition::SetInherited(PartitionPtr p) {
    std::lock_guard<bthread::Mutex> _(mu_);
    inherited_ = p;
    if (p) {
        info_.set_inherited_id(p->GetId());
    }
}

PartitionPtr Partition::GetInherited() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return inherited_;
}

Status SerializeToLoadRequest(const PartitionPtr& partition, bool async_load, LoadRequest* req) {
    CHECK(req != nullptr);

    PartitionInfo info = partition->GetInfo();
    PartitionSet* pset = partition->GetPartitionSet();
    CHECK(pset);
    Table* table = pset->GetTable();
    CHECK(table);

    partition_id_t pid(info.id());
    const auto& partition_unit_info = info.partition_unit();
    auto pair = pset->GetSlotRange();
    req->set_partition_id(pid.id);
    req->set_load_version(info.version_u64());
    req->set_start_slot(pair.first);
    req->set_end_slot(pair.second);

    // TODO(wuzhenyu) uri 最好持久化
    std::string uri = GeneratePartitionSetUri(partition_unit_info.storage_pool_uri(),
                                              table->GetNamespaceName(), pid.GetPartitionSetId());
    req->set_partition_uri(std::move(uri));
    req->set_table_name(table->GetFullName());
    *req->mutable_config() = table->GetInfo().config();
    req->set_sync(!async_load);
    bool is_primary = info.role() == PartitionRole::PARTITION_ROLE_PRIMARY;
    req->set_readonly(!is_primary);
    *req->mutable_membership() = pset->GetMembershipInfo();
    return Status::OK();
}

PartitionStats Partition::GetRealTimeStats() const {
    std::lock_guard<bthread::Mutex> _(stats_mu_);
    return real_time_stats_;
}

void Partition::SetRealTimeStats(const PartitionStats& stats) {
    std::lock_guard<bthread::Mutex> _(stats_mu_);
    real_time_stats_ = stats;
}

Status Partition::LoadSnapshot(google::protobuf::io::FileInputStream* stream) {
    return UnPackFromStream(stream);
}

bool Partition::Equal(DeepCopy* rhs_base) {
    if (rhs_base == nullptr) {
        return false;
    }
    auto rhs = static_cast<Partition*>(rhs_base);
    std::lock_guard<bthread::Mutex> _(mu_);
    if (!ProtoEqual(info_, rhs->info_)) {
        LOG_INFO("proto info not equal");
        return false;
    }
    if (node_.lock() && !rhs->node_.lock()) {
        LOG_INFO("node attach not equal");
        return false;
    }
    if (partition_set_ && !rhs->partition_set_) {
        LOG_INFO("pset attach not equal");
        return false;
    }
    if (derived_ && !rhs->derived_) {
        LOG_INFO("derived not equal");
        return false;
    }
    if (inherited_ && !rhs->inherited_) {
        LOG_INFO("inherited not equal");
        return false;
    }
    return true;
}

void Partition::DeepCopyTo(DeepCopy* rhs_base) {
    auto rhs = static_cast<Partition*>(rhs_base);
    std::lock_guard<bthread::Mutex> _(mu_);
    rhs->info_ = info_;
}

Status Partition::DumpSnapshot(google::protobuf::io::FileOutputStream* stream) {
    return PackToStream(stream);
}

std::ostream& Partition::ToString(std::ostream& os) const {
    std::lock_guard<bthread::Mutex> _(mu_);
    return os << this << "|" << partition_id_t(info_.id()) << "|"
              << PartitionState_Name(info_.state());
}

/////////

PartitionSet::PartitionSet(uint64_t pset_id) { info_.set_id(pset_id); }

uint64_t PartitionSet::GetId() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.id();
}

void PartitionSet::SetSlotRange(uint32_t start, uint32_t end) {
    std::lock_guard<bthread::Mutex> _(mu_);
    info_.set_slot_range_begin(start);
    info_.set_slot_range_end(end);
}

std::pair<uint32_t, uint32_t> PartitionSet::GetSlotRange() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return std::make_pair(info_.slot_range_begin(), info_.slot_range_end());
}

void PartitionSet::Attach(Table* table) {
    std::lock_guard<bthread::Mutex> _(mu_);
    table_ = table;
}

void PartitionSet::Detach() {
    std::lock_guard<bthread::Mutex> _(mu_);
    table_ = nullptr;
}

Table* PartitionSet::GetTable() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return table_;
}

PartitionSetInfo PartitionSet::GetInfo() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_;
}

MembershipInfo PartitionSet::GetMembershipInfo() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.membership();
}

void PartitionSet::SetCreatingState() {
    std::lock_guard<bthread::Mutex> _(mu_);
    info_.set_state(PartitionSetState::PSET_CREATING);
}

void PartitionSet::SetState(PartitionSetState s) {
    std::lock_guard<bthread::Mutex> _(mu_);
    info_.set_state(s);
}

PartitionSetState PartitionSet::GetState() {
    std::lock_guard<bthread::Mutex> _(mu_);
    return info_.state();
}

static void IncrMembershipGlobalVersionInternal(MembershipInfo* mship) {
    const uint64_t curr = mship->partition_set_version();
    CHECK_LT(curr, std::numeric_limits<uint64_t>::max() - 1);
    mship->set_partition_set_version(curr + 1);
}

static void IncrMembershipUnitVersionInternal(MembershipInfo_Unit* unit) {
    const uint64_t curr = unit->version();
    CHECK_LT(curr, std::numeric_limits<uint64_t>::max() - 1);
    unit->set_version(curr + 1);
}

void PartitionSet::IncrMembershipGlobalVersion() {
    std::lock_guard<bthread::Mutex> _(mu_);
    auto mship = info_.mutable_membership();
    IncrMembershipGlobalVersionInternal(mship);
}

Status PartitionSet::InitMembership() {
    std::lock_guard<bthread::Mutex> _(mu_);
    CHECK(info_.state() == PartitionSetState::PSET_CREATING);
    int cnt = 0;
    auto mship = info_.mutable_membership();
    mship->set_partition_set_version(1);
    mship->clear_units();
    for (auto iter : units_) {
        auto& unit = iter.second;
        CHECK(!unit.empty());

        auto munit = mship->add_units();
        munit->set_partition_unit_id(iter.first);
        // TODO(wuzhenyu) select loction prefer
        PartitionPtr primary = unit.front();
        munit->set_primary_id(primary->GetId());
        primary->SetRole(PartitionRole::PARTITION_ROLE_PRIMARY);
        munit->set_election_term(1);
        for (auto& partition : unit) {
            cnt++;
            munit->add_active_id_list(partition->GetId());
            if (partition.get() != primary.get()) {
                partition->SetRole(PartitionRole::PARTITION_ROLE_SECONDARY);
            }
        }
    }
    return Status::OK();
}

void PartitionSet::FinishCreatingPartition(const PartitionPtr& p) {
    std::lock_guard<bthread::Mutex> _(mu_);
    --creating_partition_count_;
    CHECK_GE(creating_partition_count_, 0);
    if (info_.state() == PartitionSetState::PSET_CREATING && creating_partition_count_ == 0) {
        info_.set_state(PartitionSetState::PSET_NORMAL);
    }
    auto mship = info_.mutable_membership();
    PartitionPlacementInfo* pp = mship->add_placements();
    pp->set_id(p->GetId());
    *pp->mutable_placement() = p->GetPlacementActual();

    const PartitionUnit& unit_info = p->GetPartitionUnitInfo();
    for (int i = 0; i < mship->units_size(); i++) {
        auto munit = mship->mutable_units(i);
        if (munit->partition_unit_id() != unit_info.id()) {
            continue;
        }
        IncrMembershipUnitVersionInternal(munit);
        break;
    }
}

Status PartitionSet::Add(PartitionPtr partition) {
    const uint64_t id = partition->GetId();
    std::lock_guard<bthread::Mutex> _(mu_);
    auto pair = partitions_.emplace(id, partition);
    if (!pair.second) {
        return Status::AlreadyExists("");
    }

    if (partition->GetState() == PartitionState::P_CREATING) {
        ++creating_partition_count_;
    }

    const PartitionUnit& unit_info = partition->GetPartitionUnitInfo();
    units_[unit_info.id()].push_back(partition);
    auto mship = info_.mutable_membership();
    for (int i = 0; i < mship->units_size(); i++) {
        auto munit = mship->mutable_units(i);
        if (munit->partition_unit_id() != unit_info.id()) {
            continue;
        }
        munit->add_active_id_list(id);
        IncrMembershipUnitVersionInternal(munit);
        break;
    }
    return Status::OK();
}

Status PartitionSet::Freeze(const PartitionPtr& partition, ElectionPolicy election_policy) {
    const uint64_t id = partition->GetId();

    std::lock_guard<bthread::Mutex> _(mu_);
    if (partition->GetState() == PartitionState::P_CREATING ||
        partition->GetState() == PartitionState::P_LOADING) {
        --creating_partition_count_;
    }
    partition->SetState(PartitionState::P_FREEZING);
    if (!partition->IsPrimary()) {
        return Status::OK();
    }
    const PartitionUnit& unit_info = partition->GetPartitionUnitInfo();
    MembershipInfo* mship = info_.mutable_membership();
    MembershipInfo_Unit* munit = nullptr;
    for (int i = 0; i < mship->units_size(); i++) {
        MembershipInfo_Unit* unit = mship->mutable_units(i);
        if (unit->partition_unit_id() == unit_info.id()) {
            munit = unit;
            break;
        }
    }
    if (munit && munit->primary_id() == id) {
        munit->clear_primary_id();
    }
    partition->SetRole(PartitionRole::PARTITION_ROLE_SECONDARY);
    return PromotePrimaryWithLock(id, unit_info.id(), election_policy);
}

Status PartitionSet::SetFrozen(const PartitionPtr& partition, int64_t ts) {
    const uint64_t id = partition->GetId();
    std::lock_guard<bthread::Mutex> _(mu_);
    partition->SetFrozenState(ts);
    PartitionPtr child = partition->GetDerived();
    if (child) {
        child->SetInherited(nullptr);
        partition->SetDerived(nullptr);
    }
    PartitionPtr parent = partition->GetInherited();
    if (parent) {
        parent->SetDerived(nullptr);
        partition->SetInherited(nullptr);
    }
    partitions_.erase(id);
    const PartitionUnit& unit_info = partition->GetPartitionUnitInfo();
    auto& l = units_[unit_info.id()];
    for (auto iter = l.begin(); iter != l.end(); iter++) {
        if ((*iter)->GetId() == id) {
            l.erase(iter);
            break;
        }
    }
    frozen_partitions_.emplace(id, partition);

    auto mship = info_.mutable_membership();
    MembershipInfo_Unit* munit = nullptr;
    for (int i = 0; i < mship->units_size(); i++) {
        MembershipInfo_Unit* unit = mship->mutable_units(i);
        if (unit->partition_unit_id() == unit_info.id()) {
            munit = unit;
            break;
        }
    }
    CHECK(munit) << this;

    for (auto iter = munit->active_id_list().begin(); iter != munit->active_id_list().end();
         iter++) {
        if (*iter == id) {
            munit->mutable_active_id_list()->erase(iter);
            break;
        }
    }
    munit->add_frozen_id_list(id);
    IncrMembershipUnitVersionInternal(munit);
    return Status::OK();
}

Status PartitionSet::Drop(const PartitionPtr& partition) {
    uint64_t id = partition->GetId();
    CHECK(partition->GetState() == PartitionState::P_FROZEN);
    std::lock_guard<bthread::Mutex> _(mu_);
    partition->Detach();
    frozen_partitions_.erase(id);
    const PartitionUnit& unit_info = partition->GetPartitionUnitInfo();
    auto mship = info_.mutable_membership();
    MembershipInfo_Unit* munit = nullptr;
    for (int i = 0; i < mship->units_size(); i++) {
        MembershipInfo_Unit* unit = mship->mutable_units(i);
        if (unit->partition_unit_id() == unit_info.id()) {
            munit = unit;
            break;
        }
    }
    CHECK(munit) << this;
    for (auto iter = munit->frozen_id_list().begin(); iter != munit->frozen_id_list().end();
         iter++) {
        if (*iter == id) {
            munit->mutable_frozen_id_list()->erase(iter);
            break;
        }
    }
    for (auto iter = mship->placements().begin(); iter != mship->placements().end(); iter++) {
        if (iter->id() == id) {
            mship->mutable_placements()->erase(iter);
            break;
        }
    }
    IncrMembershipUnitVersionInternal(munit);
    return Status::OK();
}

Status PartitionSet::UpdatePlacementActual(const PartitionPtr& partition, PlacementSpec loc) {
    std::lock_guard<bthread::Mutex> _(mu_);

    const uint64_t id = partition->GetId();
    partition->SetPlacementActual(loc, partition->GetNode());

    const PartitionUnit& unit_info = partition->GetPartitionUnitInfo();
    auto mship = info_.mutable_membership();
    MembershipInfo_Unit* munit = nullptr;
    for (int i = 0; i < mship->units_size(); i++) {
        MembershipInfo_Unit* unit = mship->mutable_units(i);
        if (unit->partition_unit_id() == unit_info.id()) {
            munit = unit;
            break;
        }
    }
    CHECK(munit) << this;
    for (int i = 0; i < mship->placements_size(); i++) {
        auto place_with_id = mship->mutable_placements(i);
        if (place_with_id->id() == id) {
            *(place_with_id->mutable_placement()) = std::move(loc);
        }
    }

    IncrMembershipUnitVersionInternal(munit);
    return Status::OK();
}

PartitionPtr PartitionSet::GetPrimary(uint32_t unit_id) {
    std::lock_guard<bthread::Mutex> _(mu_);
    auto iter = units_.find(unit_id);
    if (iter == units_.end()) {
        return nullptr;
    }

    const auto& partitions = iter->second;
    for (auto& p : partitions) {
        if (p->IsPrimary()) {
            return p;
        }
    }
    return nullptr;
}

PartitionPtr PartitionSet::GetPrimary(const PartitionPtr& p) {
    const PartitionUnit& unit_info = p->GetPartitionUnitInfo();
    return GetPrimary(unit_info.id());
}

Status PartitionSet::SwitchPrimary(uint32_t unit_id) {
    CHECK(false);
    // TODO(wuzhenyu) impl
    return Status::OK();
}

Status PartitionSet::PromotePrimaryWithLock(uint64_t legacy_primary_id, uint32_t unit_id,
                                            ElectionPolicy policy) {
    partition_id_t legacy_pid(legacy_primary_id);
    PartitionPtr new_primary;
    if (policy == ElectionPolicy::PROMOTE_DERIVED) {
        CHECK_GT(legacy_primary_id, 0ULL);
        uint64_t idx = legacy_pid.GetPartitionIndex();
        for (auto& partition : units_[unit_id]) {
            if (partition->GetState() != PartitionState::P_CREATING) {
                continue;
            }
            partition_id_t pid(partition->GetId());
            if (idx == pid.GetPartitionIndex()) {
                new_primary = partition;
                break;
            }
        }
        CHECK(new_primary) << this;
    } else {
        // TODO(wuzhenyu) impl
        CHECK(false) << this << ElectionPolicy_Name(policy);
    }
    CHECK(new_primary.get() != nullptr);
    new_primary->SetRole(PartitionRole::PARTITION_ROLE_PRIMARY);

    const PartitionUnit& unit_info = new_primary->GetPartitionUnitInfo();
    MembershipInfo* mship = info_.mutable_membership();
    MembershipInfo_Unit* munit = nullptr;
    for (int i = 0; i < mship->units_size(); i++) {
        MembershipInfo_Unit* unit = mship->mutable_units(i);
        if (unit->partition_unit_id() == unit_info.id()) {
            munit = unit;
            break;
        }
    }
    CHECK(munit) << this;
    munit->set_primary_id(new_primary->GetId());
    munit->set_election_term(munit->election_term() + 1);
    IncrMembershipUnitVersionInternal(munit);
    return Status::OK();
}

std::vector<PartitionPtr> PartitionSet::GetPartitions(uint32_t unit_id) {
    std::vector<PartitionPtr> result;
    std::lock_guard<bthread::Mutex> _(mu_);
    auto iter = units_.find(unit_id);
    if (iter == units_.end()) {
        return result;
    }

    const auto& partitions = iter->second;
    for (auto& p : partitions) {
        result.push_back(p);
    }
    return result;
}

std::vector<PartitionPtr> PartitionSet::GetAllPartitions() {
    std::vector<PartitionPtr> result;
    std::lock_guard<bthread::Mutex> _(mu_);
    for (auto iter : partitions_) {
        result.push_back(iter.second);
    }
    for (auto iter : frozen_partitions_) {
        result.push_back(iter.second);
    }
    return result;
}
std::vector<PartitionPtr> PartitionSet::GetAllFrozenPartitions() {
    std::vector<PartitionPtr> result;
    std::lock_guard<bthread::Mutex> _(mu_);
    for (auto iter : frozen_partitions_) {
        result.push_back(iter.second);
    }
    return result;
}

std::vector<PartitionPtr> PartitionSet::GetSilblings(const PartitionPtr& p) {
    std::vector<PartitionPtr> result;
    const PartitionUnit& unit_info = p->GetPartitionUnitInfo();
    std::lock_guard<bthread::Mutex> _(mu_);
    const auto& l = units_[unit_info.id()];
    for (auto iter : l) {
        result.push_back(iter);
    }
    return result;
}

Status PartitionSet::FreezeAllPartitions() {
    std::lock_guard<bthread::Mutex> _(mu_);
    CHECK(info_.state() == PartitionSetState::PSET_FROZEN);
    for (auto iter : partitions_) {
        auto p = iter.second;
        // skip freezing state
        p->SetState(PartitionState::P_FROZEN);
        p->SetDerived(nullptr);
        p->SetInherited(nullptr);
        frozen_partitions_.emplace(p->GetId(), p);
    }
    partitions_.clear();
    units_.clear();
    auto mship = info_.mutable_membership();
    for (int i = 0; i < mship->units_size(); i++) {
        auto munit = mship->mutable_units(i);
        for (const uint64_t active_id : munit->active_id_list()) {
            munit->add_frozen_id_list(active_id);
        }
        munit->clear_active_id_list();
        IncrMembershipUnitVersionInternal(munit);
    }
    IncrMembershipGlobalVersionInternal(mship);
    return Status::OK();
}

Status PartitionSet::DropAllPartitions() {
    std::lock_guard<bthread::Mutex> _(mu_);
    CHECK(info_.state() == PartitionSetState::PSET_FROZEN);
    CHECK(partitions_.empty());
    for (auto& iter : frozen_partitions_) {
        iter.second->Detach();
    }
    frozen_partitions_.clear();
    // never mind of info.membership
    return Status::OK();
}

Status PartitionSet::DerivePartition(const PartitionPtr& inherited, PartitionPtr* derived,
                                     int64_t ts, PartitionBornObjective pbo) {
    std::lock_guard<bthread::Mutex> _(mu_);
    PartitionPtr result;
    if (result = inherited->GetDerived()) {
        LOG_WARNING("partition already has derived partition")
            .put("inherited", *inherited)
            .put("derived", *result);
        return Status::Internal("already has derived partition");
    }
    PartitionInfo info = inherited->GetInfo();  // copy
    partition_id_t pid(inherited->GetId());
    uint32_t new_version = (pid.GetPartitionVersion() + 1) % kPartitionVersionMask;
    uint64_t new_version_u64 = info.version_u64() + 1;
    pid.SetPartitionVersion(new_version);
    info.set_id(pid.id);
    info.clear_placement_actual();
    info.clear_role();
    info.set_version_u64(new_version_u64);
    result = std::make_shared<Partition>(std::move(info));
    result->SetCreatingState(pbo);

    inherited->SetDerived(result);
    result->SetInherited(inherited);
    *derived = result;
    LOG_INFO("derive partition")
        .put("inherited_partition_id", inherited->GetId())
        .put("derived_partition_id", pid)
        .put("version_u64", new_version_u64);
    return Status::OK();
}

Status PartitionSet::Metabolize(const PartitionPtr& derived) {
    CHECK(false);
    return Status::OK();
}

Status PartitionSet::AcquireNewPartitionIndex(uint32_t* result) {
    uint32_t max_idx = 0;
    std::unordered_set<uint32_t> current_set;
    std::lock_guard<bthread::Mutex> _(mu_);
    for (auto& pair : partitions_) {
        partition_id_t pid(pair.first);
        uint32_t idx = pid.GetPartitionIndex();
        current_set.insert(idx);
        max_idx = std::max(max_idx, idx);
    }
    for (auto& pair : frozen_partitions_) {
        // Consider: after reducing partition_num
        partition_id_t pid(pair.first);
        uint32_t idx = pid.GetPartitionIndex();
        current_set.insert(idx);
        max_idx = std::max(max_idx, idx);
    }
    if (current_set.size() >= static_cast<size_t>(kPartitionIndexMask + 1)) {
        return Status::Internal("partition index overflow");
    }
    for (uint32_t i = 0; i <= kPartitionIndexMask; i++) {
        uint32_t idx = (++max_idx) & kPartitionIndexMask;
        if (current_set.count(idx) == 0) {
            *result = idx;
            return Status::OK();
        }
    }
    return Status::Internal("partition index overflow");
}

Status PartitionSet::SerializeToString(std::string* output) {
    PartitionSetSnapshot snap;
    {
        std::lock_guard<bthread::Mutex> _(mu_);
        *snap.mutable_info() = info_;
        for (auto& pair : partitions_) {
            snap.add_partition_id_list(pair.first);
            auto derived = pair.second->GetDerived();
            if (derived) {
                LOG_INFO("partition has derived")
                    .put("pid", pair.first)
                    .put("derived", derived->GetId());
                auto l = snap.add_lineages();
                l->set_parent_id(pair.first);
                l->set_child_id(derived->GetId());
            }
        }
        for (auto& pair : frozen_partitions_) {
            snap.add_partition_id_list(pair.first);
            auto derived = pair.second->GetDerived();
            if (derived) {
                LOG_INFO("frozen partition has derived")
                    .put("pid", pair.first)
                    .put("derived", derived->GetId());
                auto l = snap.add_lineages();
                l->set_parent_id(pair.first);
                l->set_child_id(derived->GetId());
            }
        }
    }
    if (!snap.SerializeToString(output)) {
        return Status::Internal("Serialize Error");
    }
    return Status::OK();
}

Status PartitionSet::ParseFromString(const std::string& input) {
    PartitionSetSnapshot snap;
    if (!snap.ParseFromString(input)) {
        return Status::Internal("Parse Error");
    }
    std::lock_guard<bthread::Mutex> _(mu_);
    snap_cache_ = snap;
    info_ = snap.info();
    return Status::OK();
}

bool PartitionSet::Equal(DeepCopy* rhs_base) {
    if (rhs_base == nullptr) {
        return false;
    }
    auto rhs = static_cast<PartitionSet*>(rhs_base);
    std::lock_guard<bthread::Mutex> _(mu_);

    if (!ProtoEqual(info_, rhs->info_)) {
        LOG_INFO("proto info not equal");
        return false;
    }
    if (creating_partition_count_ != rhs->creating_partition_count_) {
        LOG_INFO("creating partition count not equal")
            .put("mine", creating_partition_count_)
            .put("rhs", rhs->creating_partition_count_);
        return false;
    }
    if (!MapEqual(partitions_, rhs->partitions_)) {
        LOG_INFO("partitions not equal");
        return false;
    }
    if (!MapEqual(frozen_partitions_, rhs->frozen_partitions_)) {
        LOG_INFO("frozen partitions not equal");
        return false;
    }
    for (auto& pair : units_) {
        if (pair.second.size() != rhs->units_[pair.first].size()) {
            LOG_INFO("unit count not equal");
            return false;
        }
    }
    auto find_mirror_by_id = [rhs](const uint64_t id) -> PartitionPtr {
        auto iter = rhs->partitions_.find(id);
        if (iter != rhs->partitions_.end()) {
            return iter->second;
        }
        auto iter2 = rhs->frozen_partitions_.find(id);
        if (iter2 != rhs->frozen_partitions_.end()) {
            return iter2->second;
        }
        return nullptr;
    };

    std::vector<PartitionPtr> all;
    for (auto iter : partitions_) {
        all.push_back(iter.second);
    }
    for (auto iter : frozen_partitions_) {
        all.push_back(iter.second);
    }
    for (auto p : all) {
        const uint64_t myid = p->GetId();
        auto derived = p->GetDerived();
        if (derived) {
            auto mirror = find_mirror_by_id(derived->GetId());
            if (!mirror || mirror->GetId() != derived->GetId()) {
                LOG_INFO("partition lineage is not euqal")
                    .put("pid", myid)
                    .put("derived", derived->GetId());
                return false;
            }
        }
        auto inherited = p->GetInherited();
        if (inherited) {
            auto mirror = find_mirror_by_id(inherited->GetId());
            if (!mirror || mirror->GetId() != inherited->GetId()) {
                LOG_INFO("partition lineage is not euqal")
                    .put("pid", myid)
                    .put("inherited", inherited->GetId());
                return false;
            }
        }
    }

    return true;
}

void PartitionSet::DeepCopyTo(DeepCopy* rhs_base) {
    // Note: Cannot copy node_ here
    auto rhs = static_cast<PartitionSet*>(rhs_base);
    std::lock_guard<bthread::Mutex> _(mu_);
    rhs->info_ = info_;
    rhs->creating_partition_count_ = creating_partition_count_;
    for (auto& pair : partitions_) {
        PartitionPtr p = std::make_shared<Partition>(PartitionInfo());
        pair.second->DeepCopyTo(p.get());
        p->Attach(rhs);
        rhs->partitions_[pair.first] = p;
        rhs->units_[p->GetPartitionUnitInfo().id()].push_back(p);
    }
    for (auto& pair : frozen_partitions_) {
        PartitionPtr p = std::make_shared<Partition>(PartitionInfo());
        p->Attach(rhs);
        pair.second->DeepCopyTo(p.get());
        rhs->frozen_partitions_[pair.first] = p;
    }

    auto copy_lineage = [rhs](PartitionPtr& p) {
        auto find_mirror_by_id = [rhs](const uint64_t id) -> PartitionPtr {
            auto iter = rhs->partitions_.find(id);
            if (iter != rhs->partitions_.end()) {
                return iter->second;
            }
            auto iter2 = rhs->frozen_partitions_.find(id);
            if (iter2 != rhs->frozen_partitions_.end()) {
                return iter2->second;
            }
            return nullptr;
        };
        const uint64_t myid = p->GetId();
        auto derived = p->GetDerived();
        if (derived) {
            LOG_INFO("partition has derived").put("pid", myid).put("derived", derived->GetId());
            auto mirror = find_mirror_by_id(derived->GetId());
            CHECK(mirror.get() != nullptr);
            rhs->partitions_[myid]->SetDerived(mirror);
        }
        auto inherited = p->GetInherited();
        if (inherited) {
            LOG_INFO("partition has inherited")
                .put("pid", myid)
                .put("inherited", inherited->GetId());
            auto mirror = find_mirror_by_id(inherited->GetId());
            CHECK(mirror.get() != nullptr);
            rhs->partitions_[myid]->SetInherited(mirror);
        }
    };

    for (auto& pair : partitions_) {
        copy_lineage(pair.second);
    }
    for (auto& pair : frozen_partitions_) {
        copy_lineage(pair.second);
    }
}

Status PartitionSet::DumpSnapshot(google::protobuf::io::FileOutputStream* stream) {
    Status status = PackToStream(stream);
    RETURN_IF_STATUS_ERROR(status);

    std::vector<PartitionPtr> partitions;
    {
        std::lock_guard<bthread::Mutex> _(mu_);
        for (auto& pair : partitions_) {
            partitions.push_back(pair.second);
        }
        for (auto& pair : frozen_partitions_) {
            partitions.push_back(pair.second);
        }
    }
    for (auto& p : partitions) {
        LOG_INFO("start to dump partition")
            .put("pid", p->GetId())
            .put("state", PartitionState_Name(p->GetState()))
            .put("placement", p->GetPlacementActual().ShortDebugString());
        status = p->DumpSnapshot(stream);
        RETURN_IF_STATUS_ERROR(status);
    }
    return Status::OK();
}

Status PartitionSet::LoadSnapshot(google::protobuf::io::FileInputStream* stream) {
    Status status = UnPackFromStream(stream);
    RETURN_IF_STATUS_ERROR(status);
    LOG_INFO("start to load partition set").put("pset_id", GetId());

    std::unordered_set<uint64_t> pids;
    {
        std::lock_guard<bthread::Mutex> _(mu_);
        for (auto id : snap_cache_.partition_id_list()) {
            pids.insert(id);
        }
    }
    int creating_count = 0;
    const int total_count = pids.size();
    LOG_INFO("start to load partitions").put("partition_count", total_count);
    for (int i = 0; i < total_count; i++) {
        auto partition = std::make_shared<Partition>(PartitionInfo());
        status = partition->LoadSnapshot(stream);
        RETURN_IF_STATUS_ERROR(status);

        partition->Attach(this);
        const uint64_t pid = partition->GetId();
        pids.erase(pid);
        const PartitionState state = partition->GetState();

        std::lock_guard<bthread::Mutex> _(mu_);
        if (state == PartitionState::P_FROZEN) {
            frozen_partitions_[pid] = partition;
        } else {
            partitions_[pid] = partition;
            units_[partition->GetPartitionUnitInfo().id()].push_back(partition);
            if (state == PartitionState::P_CREATING || state == PartitionState::P_LOADING) {
                creating_count++;
            }
        }
        LOG_INFO("loaded partition").put("pid", pid).put("state", PartitionState_Name(state));
    }
    if (pids.size() > 0) {
        LOG_WARNING("partition count not match").put("left", pids.size());
        return Status::Internal("partition count not match");
    }

    auto find_partition = [this](const uint64_t id) -> PartitionPtr {
        auto iter = partitions_.find(id);
        if (iter != partitions_.end()) {
            return iter->second;
        }
        auto iter2 = frozen_partitions_.find(id);
        if (iter2 != frozen_partitions_.end()) {
            return iter2->second;
        }
        return nullptr;
    };
    for (auto& l : snap_cache_.lineages()) {
        LOG_INFO("find partition lineage").put("parent", l.parent_id()).put("child", l.child_id());
        auto parent = find_partition(l.parent_id());
        if (!parent) {
            LOG_WARNING("partition of parent not found for lineage").put("l", l.ShortDebugString());
            return Status::Internal("partition parent not found for lineage");
        }
        auto child = find_partition(l.child_id());
        if (!child) {
            LOG_WARNING("partition of child not found for lineage").put("l", l.ShortDebugString());
            return Status::Internal("partition child not found for lineage");
        }
        parent->SetDerived(child);
        child->SetInherited(parent);
    }

    std::lock_guard<bthread::Mutex> _(mu_);
    snap_cache_.Clear();
    creating_partition_count_ = creating_count;
    return Status::OK();
}

}  // namespace metaserver
}  // namespace bcache2

