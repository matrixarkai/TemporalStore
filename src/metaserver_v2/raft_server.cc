// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/raft_server.h"

#include <string>

#include "bthread/bthread.h"
#include "bthread/countdown_event.h"
#include "byte/include/macros.h"
#include "common/logging.h"
namespace bcache2 {
namespace metaserver {

RaftServer::~RaftServer() { Stop(); }

Status RaftServer::Init(Options opts) {
    LOG_INFO("init fsm");
    Status status = InitStateMachine(opts);
    if (!status.ok()) {
        LOG_WARNING("failed to init fsm").put("status", status);
        return status;
    }
    LOG_INFO("init raft server");
    status = InitServer(opts.byteraft);
    if (!status.ok()) {
        LOG_WARNING("failed to init raft server").put("status", status);
        return status;
    }
    return Status::OK();
}

Status RaftServer::InitStateMachine(const Options& opts) {
    fsm_.reset(new StateMachine());

    StateMachine::Options fsm_opts;
    fsm_opts.server = this;
    fsm_opts.metabase = opts.metabase;
    fsm_opts.convict_routine = opts.convict_routine;
    fsm_opts.proxy_calibrate_routine = opts.proxy_calibrate_routine;
    fsm_opts.meta_check_routine = opts.meta_check_routine;
    fsm_opts.balance_routine = opts.balance_routine;
    fsm_opts.scheduler_manager = opts.scheduler_manager;
    fsm_opts.event_harbor = opts.event_harbor;
    fsm_opts.meta_puber = opts.meta_puber;

    return fsm_->Init(fsm_opts);
}

Status RaftServer::InitServer(const byteraft::Options& opts) {
    auto resolver = byteraft::NewAddressResolver();
    BYTE_ASSERT(resolver.get() != nullptr);
    raft_transport_ =
        byteraft::NewTransport(FLAGS_metaserver_raft_transport_timeout_ms, resolver, nullptr);

    byte::Status byteraft_status =
        byteraft::GroupContextBuilder()
            .WorkerNum(FLAGS_metaserver_raft_worker_num)
            .ReaderNum(FLAGS_metaserver_raft_reader_num)
            .ExecutorNum(FLAGS_metaserver_raft_executor_num)
            .FlusherNum(FLAGS_metaserver_raft_flusher_num)
            .ApplierNum(FLAGS_metaserver_raft_applier_num)
            .SnapshotNum(FLAGS_metaserver_raft_snapshot_num)
            .TickInterval(FLAGS_metaserver_raft_heartbeat_cycle_ms)
            .Transport(raft_transport_.get())
            .Watch(resolver.get())
            .MaxMessagesEachPoll(FLAGS_metaserver_raft_max_msgs_each_poll)
            .MaxQueueDepth(FLAGS_metaserver_raft_group_queue_max_depth)
            .Build(&group_context_);
    if (!byteraft_status.ok()) {
        LOG_WARNING("failed to build byteraft group ctx").put("status", byteraft_status.ToString());
        return Status::Internal("failed to build group ctx");
    }

    BYTE_ASSERT(fsm_.get() != nullptr);
    raft_node_ = group_context_->CreateRaftNode(opts, fsm_);
    server_ =
        std::make_unique<byteraft::MultiRaftServerImpl>(opts.raft_addr, group_context_, nullptr);
    snapshot_server_ =
        std::make_unique<byteraft::MultiSnapshotServerImpl>(opts.snapshot_addr, group_context_);
    connector_.reset(new RaftConnector(raft_node_));
    fsm_->SetConnector(connector_.get());
    return Status::OK();
}

Status RaftServer::Start() {
    if (is_running_) {
        return Status::Internal("already running");
    }

    is_running_ = true;
    Status result = Status::OK();
    do {
        LOG_INFO("start raft server");
        byte::Status byteraft_status = server_->Start();
        if (!byteraft_status.ok()) {
            result = Status::Internal(byteraft_status.ToString());
            break;
        }

        byteraft_status = snapshot_server_->Start();
        if (!byteraft_status.ok()) {
            result = Status::Internal(byteraft_status.ToString());
            break;
        }

        LOG_INFO("start raft node");
        byteraft_status = raft_node_->Restart(true /* recover_fsm_from_snapshot */);
        if (byteraft_status.IsWALNotExists()) {
            LOG_INFO("try create new raft node and start serving");
            byteraft_status = raft_node_->Start();
        } else if (byteraft_status.IsNotFound()) {
            LOG_INFO("try recover raft node from wal instead of snapshot and start serving");
            byteraft_status = raft_node_->Restart();
        }
        if (!byteraft_status.ok()) {
            result = Status::Internal(byteraft_status.ToString());
            break;
        }
    } while (false);
    if (!result.ok()) {
        is_running_ = false;
        LOG_WARNING("failed to start").put("status", result);
    }

    return result;
}

void RaftServer::Stop() {
    if (!is_running_) {
        return;
    }

    // try best
    LOG_INFO("stopping raft node");
    byte::Status status = raft_node_->Stop();
    if (!status.ok()) {
        LOG_WARNING("failed to stop raft node").put("status", status.ToString());
    }

    LOG_INFO("stopping server");
    status = server_->Stop();
    if (!status.ok()) {
        LOG_WARNING("failed to stop server").put("status", status.ToString());
    }

    LOG_INFO("stopping snapshot server");
    status = snapshot_server_->Stop();
    if (!status.ok()) {
        LOG_WARNING("failed to stop snapshot server").put("status", status.ToString());
    }
}

Status RaftServer::WaitForLogApplied() {
    Status status;
    bthread::CountdownEvent cd;
    raft_node_->ReadIndex([&](const byte::Status& byteraft_status) {
        if (!byteraft_status.ok()) {
            status = Status::Internal(byteraft_status.ToString());
        }
        cd.signal();
    });
    cd.wait();
    return status;
}

std::vector<byteraft::NodeId> RaftServer::GetMembership() const {
    return raft_node_->GetMembership();
}

Status RaftServer::AddNode(const byteraft::NodeId& node, RaftNode::Role role) {
    Status status;
    bthread::CountdownEvent cd;
    auto cb = [&](byte::Status raft_status) {
        if (!raft_status.ok()) {
            status = Status::Internal(raft_status.ToString());
        }
        cd.signal();
    };

    switch (role) {
    case RaftNode::NORMAL:
        raft_node_->AddNode(node, cb);
        break;
    case RaftNode::LEARNER:
        raft_node_->AddLearner(node, cb);
        break;
    case RaftNode::WITNESS:
        raft_node_->AddWitness(node, cb);
        break;
    default:
        return Status::FailedPrecondition("unknown role type");
    }
    cd.wait();
    return status;
}

Status RaftServer::RemoveNode(const byteraft::NodeId& node) {
    Status status;
    bthread::CountdownEvent cd;
    auto cb = [&](byte::Status raft_status) {
        if (!raft_status.ok()) {
            status = Status::Internal(raft_status.ToString());
        }
        cd.signal();
    };
    raft_node_->RemoveNode(node, cb);
    cd.wait();
    return status;
}

Status RaftServer::TriggerSnapshot() {
    Status status;
    bthread::CountdownEvent cd;
    auto cb = [&](byte::Status raft_status) {
        if (!raft_status.ok()) {
            status = Status::Internal(raft_status.ToString());
        }
        cd.signal();
    };
    LOG_INFO("start taking a snapshot");
    raft_node_->AsyncSnapshot(cb);
    cd.wait();
    return status;
}

byteraft::NodeId RaftServer::GetNodeId() const { return raft_node_->GetNodeId(); }

std::shared_ptr<byteraft::RaftNode> RaftServer::GetNode() { return raft_node_; }

bool RaftServer::IsLeaderReady() const {
    if (!IsRunning()) {
        return false;
    }
    return fsm_->IsLeaderReady();
}

byteraft::NodeId RaftServer::LeaderNode() const { return raft_node_->LeaderNode(); }

Status RaftServer::Propose(uint64_t id, MetaServerLogType type,
                           const google::protobuf::Message* request) {
    if (!IsRunning()) {
        return Status::FailedPrecondition("closing");
    }
    if (!IsLeaderReady()) {
        return Status::FailedPrecondition("not leader or not ready");
    }
    CHECK(connector_.get() != nullptr);
    return connector_->Propose(id, type, request);
}

}  // namespace metaserver
}  // namespace bcache2
