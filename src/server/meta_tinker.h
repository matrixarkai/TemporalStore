// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once
#include <memory>
#include <string>
#include <unordered_map>

#include "common/loop_thread.h"
#include "common/metaserver_tracker.h"
#include "protocol/base.pb.h"
#include "protocol/host_spec.pb.h"

namespace bcache2 {
namespace server {

class PartitionManager;

/// Fetch and compare metadata from metaserver
/// Actions:
//   1) update membership
//   2) update server config
//   3) unload partition if dropped/frozen
class MetaTinker : public LoopThread {
 public:
    MetaTinker(std::string cluster_name, HostSpec host_spec,
               std::shared_ptr<MetaServerTracker> ms_tracker, PartitionManager* partition_manager);
    ~MetaTinker();

    void Stop();

 private:
    void DoLoop() override;
    uint64_t LoopIntervalMs() override;
    Status Fetch(metaserver::ListServerPartitionResponse* response);
    using NodePartition = metaserver::ListServerPartitionResponse_Partition;
    void TinkPartition(const NodePartition& np);
    void UnloadPartition(uint64_t pid);

 private:
    const std::string cluster_name_;
    const HostSpec host_spec_;
    std::shared_ptr<MetaServerTracker> ms_tracker_;

    std::unordered_map<uint64_t, int64_t>
        remote_missing_partition_id_map_;  // pid -> unix timestamp
    PartitionManager* partition_manager_{nullptr};
};

}  // namespace server
}  // namespace bcache2

