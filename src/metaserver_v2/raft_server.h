// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "bthread/bthread.h"
#include "butil/time.h"
#include "byteraft/group/builder.h"
#include "byteraft/include/multi_raft_server_impl.h"
#include "byteraft/include/multi_snapshot_server_impl.h"
#include "byteraft/include/options.h"
#include "byteraft/include/raft_node.h"
#include "byteraft/raft/transport.h"
#include "common/macros.h"
#include "common/status.h"
#include "metaserver_v2/balance/balance_routine.h"
#include "metaserver_v2/event_harbor.h"
#include "metaserver_v2/flags.h"
#include "metaserver_v2/fsm.h"
#include "metaserver_v2/ha/convict_routine.h"
#include "metaserver_v2/ha/meta_check_routine.h"
#include "metaserver_v2/ha/proxy_calibrate_routine.h"
#include "metaserver_v2/meta/metabase.h"
#include "metaserver_v2/meta_publisher.h"
#include "metaserver_v2/scheduler/scheduler_manager.h"

namespace bcache2 {
namespace metaserver {

class RaftServer {
 public:
    struct Options {
        byteraft::Options byteraft;

        Metabase* metabase{nullptr};
        ConvictRoutine* convict_routine{nullptr};
        ProxyCalibrateRoutine* proxy_calibrate_routine{nullptr};
        MetaCheckRoutine* meta_check_routine{nullptr};
        BalanceRoutine* balance_routine{nullptr};
        SchedulerManager* scheduler_manager{nullptr};
        EventHarbor* event_harbor{nullptr};
        MetaPublisher* meta_puber{nullptr};
    };

 public:
    RaftServer() = default;
    ~RaftServer();

    Status Init(Options opts);
    Status Start();
    void Stop();
    bool IsRunning() const { return is_running_; }

    byteraft::NodeId GetNodeId() const;
    std::shared_ptr<byteraft::RaftNode> GetNode();

    bool IsLeaderReady() const;
    byteraft::NodeId LeaderNode() const;
    std::vector<byteraft::NodeId> GetMembership() const;

    RaftConnector* GetConnector() { return connector_.get(); }

    // propose entry
    Status Propose(uint64_t id, MetaServerLogType type, const google::protobuf::Message* request);

    Status WaitForLogApplied();

    Status TriggerSnapshot();

    // operate
    Status AddNode(const byteraft::NodeId& node, RaftNode::Role role);
    Status RemoveNode(const byteraft::NodeId& node);

 private:
    Status InitStateMachine(const Options& opts);
    Status InitServer(const byteraft::Options& opts);

 private:
    std::atomic<bool> is_running_{false};

    std::unique_ptr<RaftConnector> connector_;
    std::shared_ptr<StateMachine> fsm_;

    std::shared_ptr<byteraft::IRaftTransport> raft_transport_;
    std::shared_ptr<byteraft::GroupContext> group_context_;
    std::shared_ptr<byteraft::RaftNode> raft_node_;

    std::unique_ptr<byteraft::MultiRaftServer> server_;
    std::unique_ptr<byteraft::MultiRaftServer> snapshot_server_;
};

}  // namespace metaserver
}  // namespace bcache2
