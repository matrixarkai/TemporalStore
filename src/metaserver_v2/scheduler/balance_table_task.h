// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>
#include <set>
#include <unordered_map>
#include <vector>

#include "common/proto_enhance.h"
#include "metaserver_v2/meta/location.h"
#include "metaserver_v2/meta/table.h"
#include "metaserver_v2/scheduler/task_scheduler.h"

namespace bcache2 {
namespace metaserver {

class SchedulerManager;

class BalanceTableTask : public Task {
 public:
    BalanceTableTask(TablePtr table, PartitionUnit unit, SchedulerManager* schd_mgr,
                     LocationManager<Server>* loc_mgr);
    ~BalanceTableTask() = default;

    Status Process() override;

    std::ostream& ToString(std::ostream& os) const override {
        return Task::ToString(os) << " balance-table-" << table_->GetId() << "-"
                                  << table_->GetName();
    }

 private:
    void Prepare();
    void ExecuteBalance();

 private:
    struct Stats {
        std::set<NodePtr> used_server_nodes{};
        size_t partition_count_total{0};
        size_t partition_count_per_node_avg{1};
    };

 private:
    TablePtr table_{nullptr};
    const PartitionUnit unit_;
    SchedulerManager* schd_mgr_{nullptr};
    LocationManager<Server>* loc_mgr_{nullptr};

    std::vector<uint64_t> balance_pids_;
    std::unordered_map<Location, Stats, LocationHash> stats_;
};

}  // namespace metaserver
}  // namespace bcache2

