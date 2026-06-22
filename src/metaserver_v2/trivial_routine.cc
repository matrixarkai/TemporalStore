// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/trivial_routine.h"

#include <string>

#include "common/logging.h"
#include "metaserver_v2/flags.h"

namespace bcache2 {
namespace metaserver {

TrivialRoutine::TrivialRoutine(RaftServer* raft_server) : raft_server_(raft_server) {}

void TrivialRoutine::DoLoop() {
    ConsulAnnounce();

    // Note: this behavior is somewhat useless due to snapshot is automatically triggered
    // if raft log size is large than FLAGS_metaserver_raft_max_applied_log_bytes
    MaybeTriggerSnapshot();
}

void TrivialRoutine::ConsulAnnounce() {
    if (!FLAGS_metaserver_consul_announce_enabled) {
        return;
    }
    auto membership = raft_server_->GetMembership();
    auto iter = std::find(membership.begin(), membership.end(), raft_server_->GetNodeId());
    if (iter == membership.end()) {
        return;
    }
    static constexpr int ttl_sec = 60;
    const int consul_port = FLAGS_metaserver_server_port;
    const std::string consul_name = FLAGS_metaserver_announce_consul_name;
    const std::string consul_name_leader = FLAGS_metaserver_announce_consul_name_leader;

    Status status = sd_.Register(consul_name, consul_port, ttl_sec);
    if (!status.ok()) {
        LOG_WARNING("register consul failed").put("status", status);
    }

    if (raft_server_->IsLeaderReady()) {
        status = sd_.Register(consul_name_leader, consul_port, ttl_sec);
        if (!status.ok()) {
            LOG_WARNING("register consul failed").put("status", status);
        }
    }
}

void TrivialRoutine::MaybeTriggerSnapshot() {
    if (!raft_server_->IsRunning() || FLAGS_metaserver_snapshot_trigger_interval_sec == 0) {
        return;
    }
    const uint64_t now = butil::gettimeofday_s();
    if (last_snapshot_timestamp_ == 0) {
        last_snapshot_timestamp_ = now;
        return;
    }

    if (last_snapshot_timestamp_ + FLAGS_metaserver_snapshot_trigger_interval_sec > now) {
        return;
    }

    auto node = raft_server_->GetNode();
    if (!node) {
        return;
    }
    // @see byteraft/include/node_status.h
    auto info = node->GetLocalStatus();
    if (last_snapshot_index_ + FLAGS_metaserver_snapshot_trigger_index_gap < info.applied_index) {
        return;
    }

    Status status = raft_server_->TriggerSnapshot();
    LOG_WARNING("trigger snapshot")
        .put("timestamp", now)
        .put("index", info.applied_index)
        .put("result", status);
    if (status.ok()) {
        last_snapshot_index_ = info.applied_index;
        last_snapshot_timestamp_ = now;
    }
}

}  // namespace metaserver
}  // namespace bcache2
