// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <byte/include/macros.h>

#include <memory>

#include "brpc/channel.h"
#include "brpc/controller.h"
#include "butil/endpoint.h"
#include "common/coclosure.h"
#include "common/status.h"
#include "partition/index/index.h"
#include "partition/storage/op_logger.h"
#include "protocol/info.pb.h"
#include "protocol/server.pb.h"

namespace bcache2 {

class MetricsManager;

namespace partition {

class Index;
class OpLogger;
class PageStore;
class Partition;
class ObjectManager;

class Replicator {
 public:
    Replicator(Partition* partition, Index* index, OpLogger* op_logger, PageStore* page_store,
               ObjectManager* object_manager, MetricsManager* metrics_manager);
    ~Replicator() {}

    void Start();
    void Stop();

    Status GetStatus() const { return status_; }

    ReplicatorInfo GetInfo() const {
        ReplicatorInfo info;
        info.set_replayed_oplog_num(replayed_oplog_num_);
        info.set_replayed_index_log_num(replayed_index_log_num_);
        info.mutable_status()->CopyFrom(status_.ToRpcStatus());
        return info;
    }

 private:
    void LoopWorker();
    void MainLoop();
    Status UpdateRemoteInfo();
    Status UpdateRemoteChannel();
    Status ReplayOpLog(uint64_t max_log_per_loop);
    Status ReplayIndexLog(uint64_t max_log_per_loop);
    bool ShouldLogOutOfSync(uint64_t now_ms);
    bool LastLoopMadeProgress() const { return last_loop_made_progress_; }

    bool stopped_ = true;
    std::unique_ptr<CoSyncClosure> stop_sync_;

    Partition* partition_ = nullptr;
    Index* index_ = nullptr;
    OpLogger* op_logger_ = nullptr;
    PageStore* page_store_ = nullptr;
    ObjectManager* object_manager_ = nullptr;

    Status status_;

    std::unique_ptr<Index::IndexLogIterator> index_log_iter_;
    std::unique_ptr<OpLogger::Iterator> op_logger_iter_;

    uint64_t last_update_remote_ms_ = 0;
    uint64_t last_replay_time_ms_ = 0;
    uint64_t last_out_of_sync_log_ms_ = 0;
    bool need_update_remote_ = false;
    bool index_log_staged_ = false;
    bool last_loop_made_progress_ = false;
    bool loop_initialized_ = false;

    uint64_t replayed_oplog_num_ = 0;
    uint64_t replayed_index_log_num_ = 0;

    std::unique_ptr<brpc::Channel> remote_channel_;

    ReplicatorMetrics metrics_;

    DISALLOW_COPY_AND_ASSIGN(Replicator);
};

}  // namespace partition
}  // namespace bcache2
