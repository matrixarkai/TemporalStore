// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>

#include "metaserver_v2/meta/table.h"
#include "metaserver_v2/scheduler/task_scheduler.h"

namespace bcache2 {
namespace metaserver {

class SchedulerManager;

class CreateTableTask : public Task {
 public:
    CreateTableTask(TablePtr table, SchedulerManager* schd_mgr);
    ~CreateTableTask() = default;

    Status Process() override;

    std::ostream& ToString(std::ostream& os) const override {
        return Task::ToString(os) << " create-table-" << table_->GetId() << "-"
                                  << table_->GetName();
    }

 private:
    const TablePtr table_;
    SchedulerManager* schd_mgr_{nullptr};
};

}  // namespace metaserver
}  // namespace bcache2

