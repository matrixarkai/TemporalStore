// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/service/raft_control_service.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "butil/endpoint.h"
#include "byteraft/include/options.h"
#include "spdlog/fmt/fmt.h"

#include "common/logging.h"
#include "metaserver_v2/flags.h"
#include "protocol/metaserver.pb.h"

namespace bcache2 {
namespace metaserver {

static bool Validate(const byteraft::NodeId& node) {
    butil::EndPoint unused;
    return butil::str2endpoint(node.raft_addr.c_str(), &unused) == 0 &&
           butil::str2endpoint(node.snapshot_addr.c_str(), &unused) == 0;
}

static inline byteraft::NodeId Convert(const RaftNode& node_pb) {
    return byteraft::NodeId(node_pb.peer_id(), node_pb.raft_addr(), node_pb.snapshot_addr());
}

static inline RaftNode Convert(const byteraft::NodeId& node) {
    RaftNode node_pb;
    node_pb.set_peer_id(node.peer_id);
    node_pb.set_raft_addr(node.raft_addr);
    node_pb.set_snapshot_addr(node.snapshot_addr);
    return node_pb;
}

void RaftControlServiceImpl::AddNode(google::protobuf::RpcController* controller,
                                     const AddRaftNodeRequest* request, AckResponse* response,
                                     google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    Status status = SanitizeRequest(request->id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    byteraft::NodeId node = Convert(request->node());
    if (!Validate(node)) {
        *response->mutable_status() =
            Status::FailedPrecondition("invalid node format").ToRpcStatus();
        return;
    }
    status = raft_server_->AddNode(node, request->node().role());
    LOG_WARNING("raft add node").put("v", request->node().ShortDebugString()).put("result", status);
    *response->mutable_status() = status.ToRpcStatus();
}

void RaftControlServiceImpl::RemoveNode(google::protobuf::RpcController* controller,
                                        const RemoveRaftNodeRequest* request, AckResponse* response,
                                        google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    Status status = SanitizeRequest(request->id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    byteraft::NodeId node = Convert(request->node());
    if (!Validate(node)) {
        *response->mutable_status() =
            Status::FailedPrecondition("invalid node format").ToRpcStatus();
        return;
    }
    status = raft_server_->RemoveNode(node);
    LOG_WARNING("raft remove node")
        .put("v", request->node().ShortDebugString())
        .put("result", status);
    *response->mutable_status() = status.ToRpcStatus();
}

void RaftControlServiceImpl::ListMembership(google::protobuf::RpcController* controller,
                                            const EmptyRequest* request,
                                            ListRaftMembershipResponse* response,
                                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    Status status = SanitizeRequest(request->id(), false);
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    std::vector<byteraft::NodeId> nodes = raft_server_->GetMembership();
    for (auto& node : nodes) {
        *response->add_nodes() = Convert(node);
    }
    response->mutable_status()->set_code(kOK);
}

void RaftControlServiceImpl::TriggerSnapshot(google::protobuf::RpcController* controller,
                                             const EmptyRequest* request, AckResponse* response,
                                             google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    Status status = raft_server_->TriggerSnapshot();
    LOG_INFO("issue snapshot").put("result", status);
    *response->mutable_status() = status.ToRpcStatus();
}

Status RaftControlServiceImpl::SanitizeRequest(const RequestId& id, bool strict_op_name) {
    if (!raft_server_->IsLeaderReady()) {
        return Status::FailedPrecondition("not leader or not ready");
    }
    if (strict_op_name && id.operator_name().empty()) {
        return Status::FailedPrecondition("operator is required");
    }
    if (id.cluster_name() != FLAGS_metaserver_cluster_name) {
        return Status::FailedPrecondition(fmt::format(
            "cluster name mismatch {} Vs. {}", id.cluster_name(), FLAGS_metaserver_cluster_name));
    }
    return Status::OK();
}

}  // namespace metaserver
}  // namespace bcache2

