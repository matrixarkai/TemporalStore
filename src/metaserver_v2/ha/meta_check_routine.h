// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <atomic>
#include <memory>
#include <set>
#include <unordered_set>
#include <vector>

#include "bthread/bthread.h"

#include "common/proto_enhance.h"
#include "metaserver_v2/event_harbor.h"
#include "metaserver_v2/events.h"
#include "metaserver_v2/meta/metabase.h"
#include "metaserver_v2/metrics.h"
#include "metaserver_v2/raft_connector.h"

namespace bcache2 {
namespace metaserver {

class MetaCheckRoutine : public EventHarbor::Listener {
 public:
    struct Options {
        RaftConnector* raft_connector{nullptr};
        EventHarbor* event_harbor{nullptr};
    };

 public:
    explicit MetaCheckRoutine(Metabase* metabase);
    ~MetaCheckRoutine();

    Status Start(const Options& opts);
    void Stop();

    static void* RunRoutine(void* arg);

    void Consume(const EventHarbor::Event* e) override;

    std::set<EventHarbor::topic_t> Subscribed() override {
        static std::set<EventHarbor::topic_t> v{
            kTopicServerHeartbeat,
        };
        return v;
    }

 private:
    void Routine();
    void HandleServerHeartbeat(const ServerHeartbeatEvent* e);

    void PatrolAllPartition();
    void PatrolPartition(const PartitionPtr& partition);

    void DropFrozenMeta();
    Status DropFrozenTable(const TablePtr& table);
    Status DropFrozenPartition(const PartitionPtr& partition);
    Status FreezePartition(const PartitionPtr& partition);

 private:
    std::atomic<bool> running_{false};
    Metabase* metabase_{nullptr};
    RaftConnector* raft_connector_{nullptr};
    bthread_t routine_thd_;

    // rate limit
    int64_t curr_timestamp_min_{0};
    size_t freeze_partition_count_this_window_{0};
};

}  // namespace metaserver
}  // namespace bcache2

