// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/scheduler/create_partition_task.h"

#include <utility>

#include "bthread/countdown_event.h"
#include "butil/fast_rand.h"

#include "common/logging.h"
#include "metaserver_v2/client/partition_server_client.h"
#include "metaserver_v2/meta/server.h"
#include "metaserver_v2/raft_connector.h"
#include "metaserver_v2/resource_placement/placement_manager.h"
#include "metaserver_v2/scheduler/scheduler_manager.h"

namespace bcache2 {
namespace metaserver {

CreatePartitionTask::CreatePartitionTask(PartitionPtr p, SchedulerManager* schd_mgr)
    : Task(GetPriority(p)), partition_(std::move(p)), schd_mgr_(schd_mgr) {}

CreatePartitionTask::~CreatePartitionTask() {
    if (selected_node_) {
        selected_node_->RemoveIntentPartition(partition_->GetId());
    }
}

Status CreatePartitionTask::Process() {
    Status status = PreCheck();
    if (!status.ok()) {
        return status;
    }

    PlacementManager* placement_mgr = schd_mgr_->GetPlacementManager();
    CHECK(placement_mgr);
    status = placement_mgr->PlacePartition(partition_, &selected_node_);
    if (!status.ok()) {
        LOG_WARNING("failed to place partition").put("result", status);
        return Status::RetryLater("placement failed");
    }
    CHECK(selected_node_);
    Server* server = selected_node_->GetServer();
    CHECK(server);
    PlacementSpec place;
    *place.mutable_node() = selected_node_->GetInfo();
    *place.mutable_location() = server->GetLocation();
    *place.mutable_server() = server->GetEndpoint();

    LOG_INFO("call partition server to load partition")
        .put("partition", *partition_)
        .put("place", place.ShortDebugString());
    PartitionServerClientImpl client;
    bthread::CountdownEvent cd_latch;
    Status load_result;
    status = client.Load(
        partition_, place, true /*async_load*/,
        [&](Status rpc_status, google::protobuf::Message* response) {
            if (!rpc_status.ok()) {
                load_result = std::move(rpc_status);
            } else {
                load_result.FromRpcStatus(static_cast<LoadResponse*>(response)->status());
            }
            cd_latch.signal();
        });
    if (!status.ok()) {
        LOG_WARNING("failed to issue rpc to partition server").put("result", status);
        return Status::RetryLater("partition server is not ready");
    }
    cd_latch.wait();

    if (!load_result.ok() && !load_result.IsAlreadyExists() && !load_result.IsPartitionLoading()) {
        return HandleFailure(std::move(load_result));
    }

    return Submit();
}

Status CreatePartitionTask::PreCheck() {
    if (partition_->GetState() != PartitionState::P_CREATING) {
        return Status::Aborted("partition state mismatch");
    }
    PartitionSet* pset = partition_->GetPartitionSet();
    if (pset == nullptr) {
        return Status::Aborted("partition set state mismatch");
    }
    if (partition_->GetRole() != PartitionRole::PARTITION_ROLE_PRIMARY) {
        PartitionPtr primary_partition = pset->GetPrimary(partition_);
        if (primary_partition == nullptr ||
            primary_partition->GetState() != PartitionState::P_NORMAL) {
            LOG_INFO("primary partition is not ready, delayed")
                .put("partition_id", partition_->GetId());
            return Status::RetryLater("primary partition is not ready");
        }
    }
    return Status::OK();
}

Status CreatePartitionTask::HandleFailure(Status load_result) {
    LOG_WARNING("load partition failed")
        .put("result", load_result)
        .put("partition_id", partition_->GetId());
    return Status::RetryLater("load rpc failed");
}

Status CreatePartitionTask::Submit() {
    Status status = PreCheck();
    if (!status.ok()) {
        return status;
    }
    if (selected_node_->GetServer() == nullptr) {
        return Status::RetryLater("server node invalid");
    }
    const uint64_t partition_id = partition_->GetId();
    const uint64_t node_id = selected_node_->GetId();
    const uint64_t log_id = butil::fast_rand();
    CreatePartitionFinishRequest request;
    request.mutable_id()->set_timestamp(butil::gettimeofday_s());
    request.mutable_id()->set_cluster_name(FLAGS_metaserver_cluster_name);
    request.mutable_id()->set_operator_name("create partition task");
    request.set_partition_id(partition_id);
    request.set_async_load(true);
    request.set_node_id(node_id);
    LOG_INFO("propose to submit this partition creation")
        .put("partition_id", partition_id)
        .put("node_id", node_id)
        .put("log_id", log_id);
    status =
        schd_mgr_->GetRaftConnector()->Propose(log_id, MS_LOG_PARTITION_CREATE_FINISH, &request);
    if (!status.ok()) {
        LOG_WARNING("failed to propose to raft server").put("result", status);
        return Status::RetryLater("propose raft failed");
    }
    return Status::OK();
}

}  // namespace metaserver
}  // namespace bcache2

