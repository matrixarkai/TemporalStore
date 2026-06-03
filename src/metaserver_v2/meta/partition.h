// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <list>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bthread/mutex.h"

#include "common/partition_id_type.h"
#include "common/status.h"
#include "metaserver_v2/meta/serializable.h"
#include "metaserver_v2/meta/server.h"
#include "metaserver_v2/utils.h"
#include "protocol/info.pb.h"
#include "protocol/server.pb.h"

namespace bcache2 {
namespace metaserver {

class Table;
class Partition;
class PartitionSet;
using PartitionPtr = std::shared_ptr<Partition>;
using PartitionSetPtr = std::shared_ptr<PartitionSet>;

class Partition : public Serializable, public DeepCopy {
 public:
    explicit Partition(PartitionInfo info);
    ~Partition();

    uint64_t GetId();
    void Attach(PartitionSet* pset);
    void Detach();
    PartitionSet* GetPartitionSet();

    void SetCreatingState(PartitionBornObjective pbo);
    void FinishCreating(int64_t ts);
    void FinishLoading();
    void SetFrozenState(int64_t ts);
    void SetState(PartitionState);
    PartitionState GetState();
    int64_t GetFrozenTimeSec();

    PartitionInfo GetInfo();
    Location GetPlacementExpect();
    void SetPlacementExpect(Location loc);
    PartitionUnit GetPartitionUnitInfo();
    void SetPartitionUnitInfo(PartitionUnit unit);
    const PlacementSpec& GetPlacementActual();
    void SetPlacementActual(PlacementSpec loc, NodePtr node);
    NodePtr GetNode();
    bool IsPrimary();
    PartitionRole GetRole();
    void SetRole(PartitionRole role);
    PartitionBornObjective GetBornObjective();

    void SetDerived(PartitionPtr);
    PartitionPtr GetDerived();
    void SetInherited(PartitionPtr);
    PartitionPtr GetInherited();

    PartitionStats GetRealTimeStats() const;
    void SetRealTimeStats(const PartitionStats& stats);

    Status LoadSnapshot(google::protobuf::io::FileInputStream* stream);
    Status DumpSnapshot(google::protobuf::io::FileOutputStream* stream);

    bool Equal(DeepCopy* rhs) override;
    void DeepCopyTo(DeepCopy* rhs) override;

    std::string GetSerializeTypeName() override { return "partition"; }
    Status SerializeToString(std::string* output) override {
        std::lock_guard<bthread::Mutex> _(mu_);
        if (!info_.SerializeToString(output)) {
            return Status::Internal("Serialize Error");
        }
        return Status::OK();
    }

    Status ParseFromString(const std::string& input) override {
        decltype(info_) info;
        if (!info.ParseFromString(input)) {
            return Status::Internal("Parse Error");
        }
        std::lock_guard<bthread::Mutex> _(mu_);
        info_ = info;
        return Status::OK();
    }

    std::ostream& ToString(std::ostream& os) const;

 private:
    mutable bthread::Mutex mu_;
    PartitionInfo info_;
    // Note: node_ could be a dangling node without any server attached
    std::weak_ptr<Node> node_;

    PartitionSet* partition_set_{nullptr};  // NOT OWNED

    PartitionPtr derived_{nullptr};
    PartitionPtr inherited_{nullptr};

    // not controlled by FSM
    mutable bthread::Mutex stats_mu_;
    PartitionStats real_time_stats_;
};

inline std::ostream& operator<<(std::ostream& os, const Partition& obj) { return obj.ToString(os); }

Status SerializeToLoadRequest(const PartitionPtr& partition, bool async_load, LoadRequest* req);

class PartitionSet : public Serializable, public DeepCopy {
 public:
    explicit PartitionSet(uint64_t pset_id);
    ~PartitionSet() = default;

    uint64_t GetId();
    void SetSlotRange(uint32_t start, uint32_t end);
    std::pair<uint32_t, uint32_t> GetSlotRange();

    void Attach(Table* table);
    void Detach();
    Table* GetTable();

    PartitionSetInfo GetInfo();
    MembershipInfo GetMembershipInfo();
    void SetCreatingState();
    void SetState(PartitionSetState s);
    void FinishCreatingPartition(const PartitionPtr& p);
    PartitionSetState GetState();

    void IncrMembershipGlobalVersion();
    Status InitMembership();

    Status Add(PartitionPtr partition);
    Status Freeze(const PartitionPtr& partition, ElectionPolicy election_policy);
    Status SetFrozen(const PartitionPtr& partition, int64_t ts);
    Status Drop(const PartitionPtr& partition);
    Status UpdatePlacementActual(const PartitionPtr& partition, PlacementSpec loc);
    Status SwitchPrimary(uint32_t unit_id);
    PartitionPtr GetPrimary(const PartitionPtr& p);
    PartitionPtr GetPrimary(uint32_t unit_id);
    std::vector<PartitionPtr> GetPartitions(uint32_t unit_id);
    std::vector<PartitionPtr> GetAllPartitions();
    std::vector<PartitionPtr> GetAllFrozenPartitions();
    std::vector<PartitionPtr> GetSilblings(const PartitionPtr& p);  // self included
    Status FreezeAllPartitions();
    Status DropAllPartitions();

    Status DerivePartition(const PartitionPtr& inherited, PartitionPtr* derived, int64_t ts,
                           PartitionBornObjective pbo);
    Status Metabolize(const PartitionPtr& derived);

    Status AcquireNewPartitionIndex(uint32_t* result);

    bool Equal(DeepCopy* rhs) override;
    void DeepCopyTo(DeepCopy* rhs) override;
    Status LoadSnapshot(google::protobuf::io::FileInputStream* stream);
    Status DumpSnapshot(google::protobuf::io::FileOutputStream* stream);
    std::string GetSerializeTypeName() override { return "partition_set"; }
    Status SerializeToString(std::string* ouput) override;
    Status ParseFromString(const std::string& input) override;

 private:
    Status PromotePrimaryWithLock(uint64_t legacy_primary_id, uint32_t unit_id,
                                  ElectionPolicy election_policy);

 private:
    bthread::Mutex mu_;
    PartitionSetInfo info_;
    Table* table_{nullptr};  // NOT OWNED
    int creating_partition_count_{0};
    std::unordered_map<uint64_t, PartitionPtr> partitions_;
    std::unordered_map<uint32_t, std::list<PartitionPtr>> units_;
    std::unordered_map<uint64_t, PartitionPtr> frozen_partitions_;

    PartitionSetSnapshot snap_cache_;
};

// piece of shit, please remove this class and go back to basics
class PartitionSetGenerator {
 public:
    struct iterator {
        explicit iterator(PartitionSetGenerator* p) : g_(p), cursor_(0) {}
        void operator++() {
            if (g_) {
                if (++cursor_ == g_->pset_num_) {
                    g_ = nullptr;
                }
            }
        }
        bool operator!=(const iterator& rhs) { return g_ != rhs.g_; }
        PartitionSetPtr operator*() {
            partition_id_t pid(g_->table_id_, cursor_, 0, 0);
            auto pset = std::make_shared<PartitionSet>(pid.GetPartitionSetId());
            uint64_t total_slots = kSlotNum;
            uint32_t start = total_slots * cursor_ / g_->pset_num_;
            uint32_t end = total_slots * (cursor_ + 1) / g_->pset_num_ - 1;
            if (end >= total_slots) {
                end = total_slots - 1;
            }
            pset->SetSlotRange(start, end);
            pset->SetCreatingState();
            return pset;
        }

     private:
        PartitionSetGenerator* g_{nullptr};
        uint32_t cursor_{0};
    };

 public:
    explicit PartitionSetGenerator(uint32_t table_id, uint32_t pset_num)
        : table_id_(table_id), pset_num_(pset_num) {}
    iterator begin() {
        return validate_partition_set_num(pset_num_) ? iterator(this) : iterator(nullptr);
    }
    iterator end() { return iterator(nullptr); }

 private:
    friend struct iterator;
    uint32_t table_id_{0};
    uint32_t pset_num_{0};
};

}  // namespace metaserver
}  // namespace bcache2

