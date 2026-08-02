// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/scheduler/update_membership_task.h"

#include <set>
#include <utility>
#include <vector>

#include "bthread/countdown_event.h"
#include "butil/fast_rand.h"
#include "spdlog/fmt/fmt.h"

#include "common/logging.h"
#include "metaserver_v2/client/partition_server_client.h"
#include "metaserver_v2/raft_server.h"
#include "metaserver_v2/scheduler/priority.h"

namespace bcache2 {
namespace metaserver {

UpdateMembershipTask::UpdateMembershipTask(PartitionPtr partition, SchedulerManager* schd_mgr,
                                           Options opts)
    : Task(kTaskPriorityUpdateMembership),
      partition_(std::move(partition)),
      schd_mgr_(schd_mgr),
      opts_(std::move(opts)) {
    PartitionSet* pset = partition_->GetPartitionSet();
    CHECK(pset != nullptr);
    membership_info_.CopyFrom(pset->GetMembershipInfo());
}

Status UpdateMembershipTask::Process() {
    Status status;
    PartitionSet* pset = partition_->GetPartitionSet();
    if (pset == nullptr || partition_->GetState() == PartitionState::P_FROZEN) {
        return Status::Aborted("partition state mismatch");
    }
    const MembershipInfo& curr_info = pset->GetMembershipInfo();
    if (curr_info.partition_set_version() != membership_info_.partition_set_version()) {
        LOG_WARNING("partition set version changed")
            .put("partition", *partition_)
            .put("mine", membership_info_.partition_set_version())
            .put("current", curr_info.partition_set_version());
    }

    uint64_t self_id = partition_->GetId();
    partition_id_t self_pid(self_id);
    const PartitionUnit& unit_info = partition_->GetPartitionUnitInfo();
    std::set<uint64_t> valid_ids;
    // Note: only send to my siblings
    for (const auto& unit : membership_info_.units()) {
        if (unit.partition_unit_id() != unit_info.id()) {
            continue;
        }
        for (uint64_t id : unit.active_id_list()) {
            partition_id_t pid(id);
            if (pid.GetPartitionIndex() == self_pid.GetPartitionIndex()) {
                // ignore derived
                continue;
            }
            valid_ids.insert(id);
        }
        break;
    }
    membership_info_.clear_placements();
    auto candidates = pset->GetAllPartitions();
    std::vector<PartitionPtr> partitions;
    for (auto& p : candidates) {
        auto state = p->GetState();
        if (state == PartitionState::P_FROZEN) {
            continue;
        }
        if (p->GetNode() == nullptr) {
            // not placed yet
            continue;
        }
        const uint64_t id = p->GetId();
        if (id == self_id && opts_.exclude_self) {
            continue;
        }
        if (valid_ids.count(id) > 0) {
            auto place_info = membership_info_.add_placements();
            place_info->set_id(id);
            *place_info->mutable_placement() = p->GetPlacementActual();
            LOG_INFO("request will send to")
                .put("partition_id", id)
                .put("endpoint", place_info->placement().server());
            partitions.push_back(p);
        }
    }
    if (partitions.empty()) {
        LOG_WARNING("no valid partitions, abort")
            .put("valid_id_cnt", valid_ids.size())
            .put("candidate_cnt", candidates.size())
            .put("partitoin", *partition_)
            .put("membership", membership_info_.DebugString());
        if (opts_.submit_fsm) {
            return Submit(Status::MetaChanged("no valid partitions"));
        }
        return Status::Aborted("no valid partitions");
    }

    LOG_INFO("start to update membership")
        .put("partition", *partition_)
        .put("partition_count", partitions.size())
        .put("threshold", opts_.success_threshold);
    size_t success_count = 0;
    size_t not_found_count = 0;
    bthread::CountdownEvent cd_latch;
    PartitionServerClientImpl client;
    status = client.UpdateMembership(
        partitions, membership_info_,
        [&](const std::vector<std::pair<Status, google::protobuf::Message*>>& responses) {
            int idx = -1;
            for (auto& p : responses) {
                ++idx;
                if (!p.first.ok()) {
                    LOG_WARNING("one peer rpc failed")
                        .put("result", p.first)
                        .put("request_idx", idx);
                    continue;
                }
                AckResponse* ack = static_cast<AckResponse*>(p.second);
                Status status = Status::FromRpcStatus(ack->status());
                if (!status.ok()) {
                    LOG_WARNING("one peer update failed")
                        .put("request_idx", idx)
                        .put("result", status);
                    if (status.IsNotFound()) {
                        // Rebooted, we have to admit as a success one
                        ++not_found_count;
                    }
                    continue;
                }
                ++success_count;
            }  // for loop

            cd_latch.signal();
        });
    if (!status.ok()) {
        LOG_WARNING("failed to issue rpc to partition server").put("result", status);
        return Status::RetryLater("partition server is not ready");
    }
    cd_latch.wait();

    Status result;
    if (success_count + not_found_count >= opts_.success_threshold) {
        result = Status::OK();
    } else {
        LOG_WARNING("success count not reach expectation")
            .put("got", success_count)
            .put("expect", opts_.success_threshold);
        result = Status::RetryLater("success count not reach expectation");
    }
    if (opts_.callback) {
        opts_.callback(result);
    }
    if (result.ok() && opts_.submit_fsm) {
        return Submit(result);
    }
    return result;
}

Status UpdateMembershipTask::Submit(Status result) {
    const uint64_t partition_id = partition_->GetId();
    const uint64_t log_id = butil::fast_rand();
    UpdateMembershipFinishRequest request;
    request.mutable_id()->set_timestamp(butil::gettimeofday_s());
    request.mutable_id()->set_cluster_name(FLAGS_metaserver_cluster_name);
    request.mutable_id()->set_operator_name("meta_check_routine");
    request.set_partition_id(partition_id);
    request.set_partition_set_version(membership_info_.partition_set_version());
    *request.mutable_result() = result.ToRpcStatus();
    LOG_INFO("propose to submit this update membership").put("request", request.ShortDebugString());
    Status status =
        schd_mgr_->GetRaftConnector()->Propose(log_id, MS_LOG_MEMBERSHIP_UPDATE_FINISH, &request);
    if (!status.ok()) {
        LOG_WARNING("failed to propose to raft server").put("result", status);
        return Status::RetryLater("propose raft failed");
    }
    return Status::OK();
}

}  // namespace metaserver
}  // namespace bcache2

