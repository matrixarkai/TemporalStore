// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bthread/mutex.h"

#include "common/macros.h"
#include "common/status.h"
#include "metaserver_v2/meta/partition.h"
#include "metaserver_v2/meta/serializable.h"
#include "protocol/info.pb.h"

namespace bcache2 {
namespace metaserver {

class Namespace;
class Table : public Serializable, public DeepCopy {
 public:
    explicit Table(TableInfo info);
    ~Table() = default;

    uint64_t GetId();
    void SetId(uint64_t);
    std::string GetName();
    std::string GetNamespaceName();
    std::string GetFullName();
    TableInfo GetInfo();
    ElectionPolicy GetElectionPolicy();

    void UpdateConfig(const Config& config);
    Config GetConfig();

    Namespace* GetNamespace();
    void Attach(Namespace* ns);
    void Detach();

    TableState GetState();
    void SetCreatingState();
    void SetFrozenState(int64_t ts);
    int64_t GetFrozenTimeSec();
    void SetState(TableState s);

    Status SpawnPartitionSets();
    Status UpdatePartitionUnit(const PartitionUnit& unit);
    Status GetPartitionSet(uint64_t pset_id, PartitionSetPtr* pset);
    size_t GetPartitionSetNum();
    std::vector<PartitionSetPtr> GetAllPartitionSets();
    void FinishCreatingPartitionSet(uint64_t pset_id);

    Status GetPartition(uint64_t id, PartitionPtr* partition);
    std::vector<PartitionPtr> GetAllPartitions();
    std::vector<PartitionPtr> ListFrozenPartitions(int64_t timestamp_before = 0);
    Status FreezeAllPartitions();
    // drop must starts from here, TODO(wuzhenyu) think again
    Status DropPartition(const PartitionPtr& partition);
    Status AddPartition(PartitionPtr partition);
    Status DropAllPartitions();

    bool CanFreezePartitionSafely(uint64_t id);

 public:
    struct PartitionLocMutPlan {
        std::vector<std::pair<Location, Location>> updates;  // from -> to
        std::vector<Location> removes;
        std::vector<Location> addes;
    };

    static PartitionLocMutPlan GeneratePartitionLocMutPlan(const PartitionUnit& origin,
                                                           const PartitionUnit& update);
    bool Equal(DeepCopy* rhs) override;
    void DeepCopyTo(DeepCopy* rhs) override;
    Status LoadSnapshot(google::protobuf::io::FileInputStream* stream);
    Status DumpSnapshot(google::protobuf::io::FileOutputStream* stream);
    DEFINE_COMMON_SERIALIZABLE("table")

 private:
    // for deep copy
    void ConstructFlatPartitionMap();

 private:
    bthread::Mutex mu_;
    TableInfo info_;
    int creating_pset_count_{0};
    Namespace* ns_{nullptr};
    std::unordered_map<uint64_t, PartitionSetPtr> partition_set_map_;  // GUARDED_BY(mu_)

    // flat map includes frozen partitions
    std::unordered_map<uint64_t, PartitionPtr> flat_partition_map_;  // GUARDED_BY(mu_)
};

using TablePtr = std::shared_ptr<Table>;

struct TableFrozenTimeGreater {
    bool operator()(const TablePtr& lhs, const TablePtr& rhs) const {
        return lhs->GetFrozenTimeSec() < rhs->GetFrozenTimeSec();
    }
};

}  // namespace metaserver
}  // namespace bcache2
