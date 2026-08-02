// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <future>
#include <list>
#include <memory>
#include <string>
#include <vector>

#include "bthread/mutex.h"
#include "byteraft/include/fsm.h"

#include "common/macros.h"
#include "metaserver_v2/balance/balance_routine.h"
#include "metaserver_v2/event_harbor.h"
#include "metaserver_v2/ha/convict_routine.h"
#include "metaserver_v2/ha/meta_check_routine.h"
#include "metaserver_v2/ha/proxy_calibrate_routine.h"
#include "metaserver_v2/meta/metabase.h"
#include "metaserver_v2/meta_publisher.h"
#include "metaserver_v2/raft_connector.h"
#include "metaserver_v2/scheduler/scheduler_manager.h"

namespace bcache2 {
namespace metaserver {

class RaftServer;

class StateMachine : public byteraft::FSM {
 public:
    struct Options {
        RaftServer* server{nullptr};
        Metabase* metabase{nullptr};
        SchedulerManager* scheduler_manager{nullptr};
        ConvictRoutine* convict_routine{nullptr};
        ProxyCalibrateRoutine* proxy_calibrate_routine{nullptr};
        MetaCheckRoutine* meta_check_routine{nullptr};
        BalanceRoutine* balance_routine{nullptr};
        EventHarbor* event_harbor{nullptr};
        MetaPublisher* meta_puber{nullptr};
    };

 public:
    StateMachine() = default;
    ~StateMachine();

    Status Init(const Options& opts);
    bool IsLeaderReady() const { return is_leader_ready_; }
    void SetConnector(RaftConnector* connector);

    /// Override functions
    byte::Status Open() override;
    byte::Status Close() override;
    byte::Status Apply(uint64_t index, const std::string& data) override;
    byte::Status OnLeaderStart(uint64_t term) override;
    byte::Status OnLeaderStop(uint64_t term) override;
    byte::Status OnStartFollowing(uint64_t cur_leader_term, const uint64_t& cur_leader_id) override;
    byte::Status OnStopFollowing(uint64_t prev_leader_term,
                                 const uint64_t& prev_leader_id) override;
    byte::Status Checkpoint(const std::string& path, uint64_t* applied_index) override;
    byte::Status OnSnapshotLoad(const std::string& snapshot_path) override;
    uint64_t FlushedIndex() override;
    void OnConfigurationApplied(const std::vector<byteraft::NodeId>& old_config,
                                const std::vector<byteraft::NodeId>& new_config) override;

 private:
    Status HandleManageInfoUpdate(const UpdateManageInfoRequest* request);

    Status HandleServerAdd(const AddServerRequest* request);
    Status HandleServerFreeze(const FreezeServerRequest* request);
    Status HandleServerDrop(const DropServerRequest* request);
    Status HandleServerUpdate(const UpdateServerRequest* request);

    Status HandleNamespaceAdd(const AddNamespaceRequest* request);

    Status HandleTableAdd(const AddTableRequest* request);
    Status HandleTableUpdate(const UpdateTableRequestV2* request);
    Status HandleTableFreeze(const FreezeTableRequest* request);
    Status HandleTableDrop(const DropTableRequest* request);
    Status HandlePartitionCreateFinish(const CreatePartitionFinishRequest* request);
    Status HandlePartitionLoadFinish(const LoadPartitionFinishRequest* request);
    Status HandleMembershipUpdateFinish(const UpdateMembershipFinishRequest* request);
    Status HandlePartitionFreeze(const FreezePartitionRequest* request);
    Status HandlePartitionDrop(const DropPartitionRequest* request);

    Status HandleProxyAdd(const AddProxyRequest* request);
    Status HandleProxyFreeze(const FreezeProxyRequest* request);
    Status HandleProxyDrop(const DropProxyRequest* request);
    Status HandleProxyGroupPut(const PutProxyGroupRequest* request);
    Status HandleProxyGroupDrop(const DropProxyGroupRequest* request);
    Status HandleProxyAttach(const AttachProxyRequest* request);
    Status HandleProxyDetach(const DetachProxyRequest* request);

    void FinishLoadingPartition(const PartitionPtr& partition);
    void FreezePartition(const PartitionPtr& partition, int64_t ts);
    void CommitFreezePartition(const PartitionPtr& partition, int64_t ts);
    bool NeedRecoverPartition(const PartitionPtr& partition);
    Status SubmitUpdateMembershipTask(const PartitionPtr& partition);

    bool CanSubmitTask() { return is_leader_ready_; }

    void AcquireLock();  // exclude lock
    void ReleaseLock();
    void UpdateSnapshotIndexList(uint64_t index);

 private:
    bthread::Mutex lock_;
    bool locked_{false};  // for debug

    uint64_t peer_id_{0};
    uint64_t applied_index_{0};

    bthread::Mutex snapshot_index_list_lock_;
    std::list<uint64_t> snapshot_index_list_;

    std::atomic<bool> running_{false};
    std::atomic<bool> is_leader_booting_{false};

    uint64_t leader_term_{0};
    std::atomic<bool> is_leader_ready_{false};

    bool mute_meta_change_ = false;

    RaftConnector* connector_{nullptr};
    RaftServer* server_{nullptr};
    Metabase* metabase_{nullptr};
    SchedulerManager* scheduler_manager_{nullptr};
    ConvictRoutine* convict_routine_{nullptr};
    ProxyCalibrateRoutine* proxy_calibrate_routine_{nullptr};
    MetaCheckRoutine* meta_check_routine_{nullptr};
    BalanceRoutine* balance_routine_{nullptr};
    EventHarbor* event_harbor_{nullptr};
    MetaPublisher* meta_puber_{nullptr};

    std::future<void> election_post_task_;
};

}  // namespace metaserver
}  // namespace bcache2
