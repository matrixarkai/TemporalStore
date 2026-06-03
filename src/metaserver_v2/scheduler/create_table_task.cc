// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/scheduler/create_table_task.h"

#include <utility>
#include <vector>

#include "common/logging.h"
#include "metaserver_v2/meta/partition.h"
#include "metaserver_v2/scheduler/priority.h"
#include "metaserver_v2/scheduler/scheduler_manager.h"

namespace bcache2 {
namespace metaserver {

CreateTableTask::CreateTableTask(TablePtr table, SchedulerManager* schd_mgr)
    : Task(kTaskPriorityCreateTable), table_(std::move(table)), schd_mgr_(schd_mgr) {}

Status CreateTableTask::Process() {
    if (!table_ || table_->GetState() != TableState::TABLE_CREATING) {
        return Status::Aborted("table state mismatch");
    }

    std::vector<PartitionPtr> partitions = table_->GetAllPartitions();
    LOG_INFO("start to split to partition creation task")
        .put("table_name", table_->GetName())
        .put("partition_count", partitions.size());
    Status status;
    for (const auto& partition : partitions) {
        if (partition->GetState() != PartitionState::P_CREATING) {
            continue;
        }
        LOG_INFO("start to submit partition creation task")
            .put("table_name", table_->GetName())
            .put("partition_id", partition->GetId());
        status = schd_mgr_->CreatePartition(partition);
        if (!status.ok()) {
            LOG_WARNING("failed to issue create partition task").put("result", status);
            return status;
        }
    }
    return Status::OK();
}

}  // namespace metaserver
}  // namespace bcache2

