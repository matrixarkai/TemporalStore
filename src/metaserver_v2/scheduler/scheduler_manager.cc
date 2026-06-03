// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/scheduler/scheduler_manager.h"

#include <utility>
#include <vector>

#include "common/logging.h"
#include "metaserver_v2/flags.h"
#include "metaserver_v2/meta/metabase.h"
#include "metaserver_v2/meta/namespace.h"
#include "metaserver_v2/raft_connector.h"
#include "metaserver_v2/resource_placement/placement_manager.h"
#include "metaserver_v2/scheduler/balance_table_task.h"
#include "metaserver_v2/scheduler/create_partition_task.h"
#include "metaserver_v2/scheduler/create_table_task.h"
#include "metaserver_v2/scheduler/update_membership_task.h"

namespace bcache2 {
namespace metaserver {

Status SchedulerManager::Start(Options opts) {
    if (running_) {
        return Status::Internal("already running");
    }
    running_ = true;
    metabase_ = opts.metabase;
    raft_connector_ = opts.raft_connector;

    LOG_INFO("start task scheduler");
    common_task_schd_.reset(new TaskScheduler("common"));
    Status status = common_task_schd_->Start();
    if (!status.ok()) {
        return status;
    }
    balance_task_schd_.reset(new TaskScheduler("balance"));
    status = balance_task_schd_->Start();
    if (!status.ok()) {
        return status;
    }

    LOG_INFO("start placement manager");
    placement_mgr_.reset(new PlacementManager(metabase_));
    placement_mgr_->SetDefaultRules();

    return Status::OK();
}

void SchedulerManager::Stop() {
    if (!running_) {
        return;
    }
    running_ = false;
    LOG_INFO("stop task scheduler");
    balance_task_schd_->Stop();
    common_task_schd_->Stop();
    LOG_INFO("stop placement manager");
    placement_mgr_.reset();
}

RaftConnector* SchedulerManager::GetRaftConnector() const { return raft_connector_; }

PlacementManager* SchedulerManager::GetPlacementManager() const { return placement_mgr_.get(); }

Status SchedulerManager::CreateTable(TablePtr table) {
    if (!running_) {
        return Status::Internal("not running");
    }

    Task* task = new CreateTableTask(std::move(table), this);
    return common_task_schd_->Submit(task);
}

Status SchedulerManager::CreatePartition(PartitionPtr partition) {
    if (!running_) {
        return Status::Internal("not running");
    }

    Task* task = new CreatePartitionTask(std::move(partition), this);
    return common_task_schd_->Submit(task);
}

Status SchedulerManager::UpdateMembership(PartitionPtr partition,
                                          UpdateMembershipTask::Options opts) {
    if (!running_) {
        return Status::Internal("not running");
    }

    Task* task = new UpdateMembershipTask(std::move(partition), this, std::move(opts));
    return common_task_schd_->Submit(task);
}

Status SchedulerManager::RepairBrokenTasks() {
    if (!running_) {
        return Status::Internal("not running");
    }

    std::vector<NamespacePtr> nslist = metabase_->GetNamespaceManager()->List();
    LOG_INFO("try to iterate all ns to find broken jobs").put("ns_count", nslist.size());
    for (auto& ns : nslist) {
        std::vector<TablePtr> tables = ns->List();
        LOG_INFO("try to iterate all table to find broken jobs").put("table_count", tables.size());
        for (auto& table : tables) {
            if (table->GetState() == TableState::TABLE_CREATING) {
                LOG_INFO("found creating table, try to re-submit creating task")
                    .put("table_id", table->GetId())
                    .put("table_name", table->GetName());
                CreateTable(table);
                continue;
            }  // if creating
            RepairPartitionTasks(table);
        }
    }  // for ns

    return Status::OK();
}

void SchedulerManager::RepairPartitionTasks(const TablePtr& table) {
    std::vector<PartitionPtr> partitions = table->GetAllPartitions();
    for (auto& partition : partitions) {
        PartitionState state = partition->GetState();
        switch (state) {
        case PartitionState::P_CREATING:
            LOG_INFO("found creating partition, try to re-create it").put("partition", *partition);
            CreatePartition(partition);
            break;
        case PartitionState::P_LOADING:
            LOG_INFO("found loading partition, ignore it now").put("partition", *partition);
            break;
        case PartitionState::P_FREEZING: {
            LOG_INFO("found freezing partition, try to update membership")
                .put("partition", *partition);
            UpdateMembership(partition, {
                                            .success_threshold = 1,
                                            .submit_fsm = true,
                                        });
            break;
        }

        default:
            break;
        }  // switch
    }      // for partitions
}

Status SchedulerManager::BalanceTable(TablePtr table, const PartitionUnit& unit) {
    if (!running_) {
        return Status::Internal("not running");
    }
    Task* task =
        new BalanceTableTask(std::move(table), unit, this, metabase_->GetServerLocationManager());
    return balance_task_schd_->Submit(task);
}

}  // namespace metaserver
}  // namespace bcache2

