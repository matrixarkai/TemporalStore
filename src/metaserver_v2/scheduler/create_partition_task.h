// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>

#include "metaserver_v2/meta/partition.h"
#include "metaserver_v2/meta/server.h"
#include "metaserver_v2/scheduler/priority.h"
#include "metaserver_v2/scheduler/task_scheduler.h"

namespace bcache2 {
namespace metaserver {

class SchedulerManager;

class CreatePartitionTask : public Task {
 public:
    CreatePartitionTask(PartitionPtr p, SchedulerManager* schd_mgr);
    ~CreatePartitionTask();

    static int GetPriority(const PartitionPtr p) {
        if (p->GetBornObjective() == PartitionBornObjective::PBO_RECOVER) {
            return kTaskPriorityCreatePartitionCritical;
        }
        if (p->IsPrimary()) {
            return kTaskPriorityCreatePartitionUrgent;
        }
        return kTaskPriorityCreatePartitionOrdinary;
    }

    Status Process() override;

    std::ostream& ToString(std::ostream& os) const override {
        return Task::ToString(os) << " create-partition-" << *partition_;
    }

 private:
    Status PreCheck();
    Status HandleFailure(Status load_result);
    Status Submit();

 private:
    const PartitionPtr partition_;
    SchedulerManager* schd_mgr_{nullptr};
    NodePtr selected_node_{nullptr};
};

}  // namespace metaserver
}  // namespace bcache2

