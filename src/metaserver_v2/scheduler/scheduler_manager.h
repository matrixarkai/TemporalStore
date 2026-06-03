// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <atomic>
#include <memory>
#include <vector>

#include "common/macros.h"
#include "common/status.h"
#include "metaserver_v2/meta/partition.h"
#include "metaserver_v2/meta/table.h"
#include "metaserver_v2/resource_placement/placement_manager.h"
#include "metaserver_v2/scheduler/task_scheduler.h"
#include "metaserver_v2/scheduler/update_membership_task.h"

namespace bcache2 {
namespace metaserver {

class Metabase;
class PlacementManager;
class PlacementRule;
class RaftConnector;

class SchedulerManager {
 public:
    struct Options {
        Metabase* metabase{nullptr};
        RaftConnector* raft_connector{nullptr};
    };

 public:
    SchedulerManager() = default;
    ~SchedulerManager() = default;

    Status Start(Options opts);
    void Stop();
    bool Running() { return running_; }

    PlacementManager* GetPlacementManager() const;
    RaftConnector* GetRaftConnector() const;

    Status RepairBrokenTasks();
    Status CreateTable(TablePtr table);
    Status CreatePartition(PartitionPtr partition);
    Status UpdateMembership(PartitionPtr partition, UpdateMembershipTask::Options opts);

    Status BalanceTable(TablePtr table, const PartitionUnit& unit);

 private:
    void RepairPartitionTasks(const TablePtr& table);

 private:
    std::atomic<bool> running_{false};

    Metabase* metabase_{nullptr};
    RaftConnector* raft_connector_{nullptr};

    std::unique_ptr<PlacementManager> placement_mgr_{nullptr};
    std::unique_ptr<TaskScheduler> common_task_schd_{nullptr};

    // separate balance task from common task schd because current
    // all tasks are sync mode, balance task task more time to handle.
    // peeling to avoid task queue over blocking
    std::unique_ptr<TaskScheduler> balance_task_schd_{nullptr};
};

}  // namespace metaserver
}  // namespace bcache2

