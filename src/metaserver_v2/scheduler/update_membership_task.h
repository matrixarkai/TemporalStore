// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>

#include "brpc/parallel_channel.h"

#include "metaserver_v2/meta/partition.h"
#include "metaserver_v2/scheduler/task_scheduler.h"

namespace bcache2 {
namespace metaserver {

class SchedulerManager;

class UpdateMembershipTask : public Task {
 public:
    struct Options {
        bool exclude_self{true};

        size_t success_threshold{0};

        // submit to FSM IFF succeed
        bool submit_fsm{false};

        // execution time point:
        //  after sending rpc to partitions,
        //  before submitting to FSM
        std::function<void(Status)> callback{nullptr};
    };

 public:
    UpdateMembershipTask(PartitionPtr partition, SchedulerManager* schd_mgr, Options opts);
    ~UpdateMembershipTask() = default;

    Status Process() override;

    std::ostream& ToString(std::ostream& os) const override {
        return Task::ToString(os) << " update-membership";
    }

 private:
    Status Submit(Status result);

 private:
    PartitionPtr partition_{nullptr};
    SchedulerManager* schd_mgr_{nullptr};
    Options opts_;
    MembershipInfo membership_info_;  // a snapshot
};

}  // namespace metaserver
}  // namespace bcache2

