#include "partition/storage/data_raft_consensus.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

#include "bthread/countdown_event.h"
#include "byte/include/status.h"
#include "byteraft/group/builder.h"
#include "byteraft/include/fsm.h"
#include "byteraft/include/multi_raft_server_impl.h"
#include "byteraft/include/multi_snapshot_server_impl.h"
#include "byteraft/include/options.h"
#include "byteraft/include/raft_node.h"
#include "byteraft/raft/transport.h"
#include "common/logging.h"

DEFINE_uint64(data_raft_transport_timeout_ms, 1000,
              "Byteraft transport timeout for TemporalStore data-node Raft.");
DEFINE_uint64(data_raft_worker_num, 2, "Byteraft worker threads for data-node Raft.");
DEFINE_uint64(data_raft_reader_num, 1, "Byteraft reader threads for data-node Raft.");
DEFINE_uint64(data_raft_executor_num, 2, "Byteraft executor threads for data-node Raft.");
DEFINE_uint64(data_raft_flusher_num, 1, "Byteraft flusher threads for data-node Raft.");
DEFINE_uint64(data_raft_applier_num, 2, "Byteraft applier threads for data-node Raft.");
DEFINE_uint64(data_raft_snapshot_num, 1, "Byteraft snapshot threads for data-node Raft.");
DEFINE_uint64(data_raft_heartbeat_cycle_ms, 10,
              "Byteraft heartbeat/tick interval for data-node Raft.");
DEFINE_uint64(data_raft_max_msgs_each_poll, 256,
              "Byteraft max messages per poll for data-node Raft.");
DEFINE_uint64(data_raft_group_queue_max_depth, 1024,
              "Byteraft group queue max depth for data-node Raft.");
DEFINE_uint64(data_raft_propose_timeout_ms, 5000,
              "Timeout for data-node Raft proposal commit.");
DEFINE_uint64(data_raft_config_change_timeout_ms, 10000,
              "Timeout for data-node Raft membership change commit/application.");
DEFINE_uint64(data_raft_snapshot_trigger_timeout_ms, 5000,
              "Timeout for explicit data-node Raft snapshot trigger completion.");
DEFINE_uint64(data_raft_max_applied_log_bytes, UINT64_MAX,
              "Byteraft automatic snapshot threshold for data-node Raft. Keep UINT64_MAX in "
              "normal operation unless a local snapshot restore test intentionally lowers it.");
DEFINE_bool(data_raft_allow_single_voter_group, false,
            "Allow data-node Raft membership changes to leave a single voter. Keep false in "
            "production except for one-node smoke tests.");
DEFINE_uint32(data_raft_raft_port_delta, 20000,
              "Data-node Raft transport port is server port plus this delta.");
DEFINE_uint32(data_raft_snapshot_port_delta, 21000,
              "Data-node Raft snapshot transport port is server port plus this delta.");
DEFINE_string(data_raft_work_dir, "/tmp/temporalstore-data-raft",
              "Base directory for data-node Raft WAL and snapshots.");
DEFINE_bool(data_raft_enable_empty_snapshot_for_tests, false,
            "Allow empty data-node Raft snapshots. This is only for backend bring-up tests; "
            "production must keep this false until snapshots persist real partition state.");

namespace bcache2 {
namespace partition {

namespace {

class UnavailableDataRaftConsensusBackend final : public DataRaftConsensusBackend {
 public:
    explicit UnavailableDataRaftConsensusBackend(DataRaftConsensusOptions options)
        : options_(std::move(options)) {}

    Status Start() override {
        return Unavailable("start");
    }

    void Stop() override {}

    bool IsLeader() const override { return false; }

    Status GetStatus(DataRaftStatus* status) const override {
        if (status == nullptr) {
            return Status::InvalidArgument("missing data raft status output");
        }
        *status = DataRaftStatus();
        return Status::OK();
    }

    Status Propose(const std::string& serialized_entry, uint64_t* committed_index) override {
        (void)serialized_entry;
        (void)committed_index;
        return Unavailable("propose");
    }

    Status WaitForAppliedIndex(uint64_t index, uint64_t timeout_ms) override {
        (void)index;
        (void)timeout_ms;
        return Unavailable("wait for applied index");
    }

    Status TriggerSnapshot(uint64_t* snapshot_index) override {
        (void)snapshot_index;
        return Unavailable("trigger snapshot");
    }

    Status ReadIndex(uint64_t timeout_ms) override {
        (void)timeout_ms;
        return Unavailable("read index");
    }

    Status AddPeer(const DataRaftPeer& peer) override {
        (void)peer;
        return Unavailable("add peer");
    }

    Status AddLearner(const DataRaftPeer& peer) override {
        (void)peer;
        return Unavailable("add learner");
    }

    Status PromotePeer(uint64_t replica_id) override {
        (void)replica_id;
        return Unavailable("promote peer");
    }

    Status RemovePeer(uint64_t replica_id) override {
        (void)replica_id;
        return Unavailable("remove peer");
    }

    Status TransferLeader(uint64_t replica_id) override {
        (void)replica_id;
        return Unavailable("transfer leader");
    }

    Status Campaign(uint64_t timeout_ms, bool force) override {
        (void)timeout_ms;
        (void)force;
        return Unavailable("campaign");
    }

    Status CanServeBoundedStaleRead(uint64_t max_stale_index_lag) const override {
        (void)max_stale_index_lag;
        return Unavailable("serve bounded-stale read");
    }

 private:
    Status Unavailable(const char* action) const {
        return Status::FailedPrecondition(
            std::string("data_replication_mode=raft_consensus cannot ") + action +
            " for partition " + std::to_string(options_.partition_id) +
            ": no data-node Raft consensus backend is linked. Required backend pieces are "
            "transport, durable Raft WAL, FSM apply, snapshots, membership changes, leader "
            "routing, and read-index/stale-read semantics.");
    }

    DataRaftConsensusOptions options_;
};

byteraft::NodeId ToByteraftNode(const DataRaftPeer& peer) {
    byteraft::NodeId node;
    node.peer_id = peer.replica_id;
    node.raft_addr = peer.raft_addr;
    node.snapshot_addr = peer.snapshot_addr;
    if (peer.auto_promote) {
        node.role_state.state = byteraft::RoleState::State::kLearner;
        node.role_state.auto_promote = true;
    }
    return node;
}

byteraft::Options ToByteraftOptions(const DataRaftConsensusOptions& options) {
    byteraft::Options raft_options;
    raft_options.peer_id = options.replica_id;
    raft_options.raft_addr = options.raft_addr;
    raft_options.snapshot_addr = options.snapshot_addr;
    raft_options.wal_dir = options.wal_dir;
    raft_options.snapshot_dir = options.snapshot_dir;
    raft_options.wal_sync = options.wal_sync;
    raft_options.enable_pre_vote = true;
    raft_options.can_trigger_snapshot = true;
    raft_options.max_applied_log_bytes = FLAGS_data_raft_max_applied_log_bytes;
    if (options.bootstrap_as_learner) {
        raft_options.role_state.state = byteraft::RoleState::State::kLearner;
        // A bootstrapped learner must not start an election while it is still a learner,
        // but it must be able to elect after the Raft leader promotes it to a voter.
        // Keeping prohibits_election=true here survives promotion in Byteraft and leaves
        // the data group leaderless after the original primary dies.
        raft_options.prohibits_election = false;
    }
    for (const auto& peer : options.peers) {
        raft_options.peers.push_back(ToByteraftNode(peer));
    }
    return raft_options;
}

Status FromByteraftStatus(const byte::Status& status) {
    if (status.ok()) {
        return Status::OK();
    }
    return Status::Internal(status.ToString());
}

bool SamePeerAddress(const byteraft::NodeId& node, const DataRaftPeer& peer) {
    return node.raft_addr == peer.raft_addr && node.snapshot_addr == peer.snapshot_addr;
}

class DataRaftFsm final : public byteraft::FSM {
 public:
    DataRaftFsm(uint64_t partition_id, DataRaftApplyFunc apply_func,
                DataRaftSnapshotFunc snapshot_func,
                DataRaftLoadSnapshotFunc load_snapshot_func,
                uint64_t initial_applied_index)
        : partition_id_(partition_id),
          apply_func_(std::move(apply_func)),
          snapshot_func_(std::move(snapshot_func)),
          load_snapshot_func_(std::move(load_snapshot_func)),
          applied_index_(initial_applied_index) {}

    byte::Status Open() override {
        running_ = true;
        return byte::Status::OK();
    }

    byte::Status Close() override {
        running_ = false;
        return byte::Status::OK();
    }

    byte::Status Apply(uint64_t index, const std::string& data) override {
        if (!running_) {
            return byte::Status::NotReady("data raft fsm is not running");
        }
        if (!apply_func_) {
            return byte::Status::Failed("missing data raft apply function");
        }
        Status status = apply_func_(index, data);
        if (!status.ok()) {
            LOG_WARNING("Data raft FSM apply failed")
                .put("PartitionId", partition_id_)
                .put("Index", index)
                .put("Status", status);
            return byte::Status::Failed(status.ToString());
        }
        applied_index_ = index;
        return byte::Status::OK();
    }

    byte::Status FlexibleApply(byteraft::Iterator* iterator) override {
        if (!running_) {
            return byte::Status::NotReady("data raft fsm is not running");
        }
        if (iterator == nullptr) {
            return byte::Status::Failed("missing data raft apply iterator");
        }

        for (; iterator->Valid(); iterator->Next()) {
            const auto kind = iterator->kind();
            switch (kind) {
            case byteraft::Iterator::kData: {
                byte::Status status = Apply(iterator->index(), iterator->data());
                if (!status.ok()) {
                    return status;
                }
                break;
            }
            case byteraft::Iterator::kNoOp:
            case byteraft::Iterator::kConfigChange:
            case byteraft::Iterator::kMeta:
                applied_index_ = iterator->index();
                break;
            default:
                return byte::Status::Failed("unknown data raft iterator entry kind");
            }
        }
        return byte::Status::OK();
    }

    byte::Status OnStartFollowing(uint64_t cur_leader_term,
                                  const uint64_t& cur_leader_id) override {
        leader_ = false;
        leader_term_ = cur_leader_term;
        leader_replica_id_ = cur_leader_id;
        return byte::Status::OK();
    }

    byte::Status OnStopFollowing(uint64_t prev_leader_term,
                                 const uint64_t& prev_leader_id) override {
        (void)prev_leader_term;
        (void)prev_leader_id;
        return byte::Status::OK();
    }

    byte::Status OnLeaderStart(uint64_t term) override {
        leader_ = true;
        leader_term_ = term;
        leader_replica_id_ = 0;
        return byte::Status::OK();
    }

    byte::Status OnLeaderStop(uint64_t term) override {
        (void)term;
        leader_ = false;
        return byte::Status::OK();
    }

    byte::Status Checkpoint(const std::string& path, uint64_t* applied_index) override {
        LOG_INFO("Data raft checkpoint callback invoked")
            .put("PartitionId", partition_id_)
            .put("Path", path)
            .put("AppliedIndex", applied_index_);
        if (snapshot_func_) {
            Status status = snapshot_func_(path, applied_index);
            if (!status.ok()) {
                LOG_WARNING("Data raft snapshot checkpoint failed")
                    .put("PartitionId", partition_id_)
                    .put("Path", path)
                    .put("Status", status);
                return byte::Status::Failed(status.ToString());
            }
            if (applied_index != nullptr) {
                applied_index_ = *applied_index;
            }
            return byte::Status::OK();
        }
        if (!FLAGS_data_raft_enable_empty_snapshot_for_tests) {
            return byte::Status::Failed(
                "data raft snapshot is not implemented for partition state");
        }
        (void)path;
        if (applied_index != nullptr) {
            *applied_index = applied_index_;
        }
        return byte::Status::OK();
    }

    byte::Status OnSnapshotLoad(const std::string& snapshot_path) override {
        if (load_snapshot_func_) {
            Status status = load_snapshot_func_(snapshot_path);
            if (!status.ok()) {
                LOG_WARNING("Data raft snapshot load failed")
                    .put("PartitionId", partition_id_)
                    .put("Path", snapshot_path)
                    .put("Status", status);
                return byte::Status::Failed(status.ToString());
            }
            return byte::Status::OK();
        }
        if (!FLAGS_data_raft_enable_empty_snapshot_for_tests) {
            return byte::Status::Failed(
                "data raft snapshot load is not implemented for partition state");
        }
        (void)snapshot_path;
        return byte::Status::OK();
    }

    void OnConfigurationApplied(const std::vector<byteraft::NodeId>& old_config,
                                const std::vector<byteraft::NodeId>& new_config) override {
        (void)old_config;
        (void)new_config;
    }

    uint64_t FlushedIndex() override { return applied_index_; }

    bool IsLeader() const { return leader_; }
    uint64_t AppliedIndex() const { return applied_index_; }

 private:
    uint64_t partition_id_ = 0;
    DataRaftApplyFunc apply_func_;
    DataRaftSnapshotFunc snapshot_func_;
    DataRaftLoadSnapshotFunc load_snapshot_func_;
    std::atomic<bool> running_{false};
    std::atomic<bool> leader_{false};
    std::atomic<uint64_t> leader_term_{0};
    std::atomic<uint64_t> leader_replica_id_{0};
    std::atomic<uint64_t> applied_index_{0};
};

class ByteraftRuntime {
 public:
    explicit ByteraftRuntime(std::string raft_addr, std::string snapshot_addr)
        : raft_addr_(std::move(raft_addr)), snapshot_addr_(std::move(snapshot_addr)) {}

    Status Start() {
        std::lock_guard<std::mutex> lock(mu_);
        if (started_) {
            return Status::OK();
        }

        auto resolver = byteraft::NewAddressResolver();
        if (resolver == nullptr) {
            return Status::Internal("failed to create byteraft address resolver");
        }
        resolver_ = resolver;
        raft_transport_ =
            byteraft::NewTransport(FLAGS_data_raft_transport_timeout_ms, resolver_, nullptr);

        byte::Status raft_status =
            byteraft::GroupContextBuilder()
                .WorkerNum(FLAGS_data_raft_worker_num)
                .ReaderNum(FLAGS_data_raft_reader_num)
                .ExecutorNum(FLAGS_data_raft_executor_num)
                .FlusherNum(FLAGS_data_raft_flusher_num)
                .ApplierNum(FLAGS_data_raft_applier_num)
                .SnapshotNum(FLAGS_data_raft_snapshot_num)
                .TickInterval(FLAGS_data_raft_heartbeat_cycle_ms)
                .Transport(raft_transport_.get())
                .Watch(resolver_.get())
                .EnableFlexibleApply()
                .MaxMessagesEachPoll(FLAGS_data_raft_max_msgs_each_poll)
                .MaxQueueDepth(FLAGS_data_raft_group_queue_max_depth)
                .Build(&group_context_);
        if (!raft_status.ok()) {
            return FromByteraftStatus(raft_status);
        }

        server_.reset(new byteraft::MultiRaftServerImpl(raft_addr_, group_context_, nullptr));
        snapshot_server_.reset(new byteraft::MultiSnapshotServerImpl(snapshot_addr_, group_context_));
        raft_status = server_->Start();
        if (!raft_status.ok()) {
            LOG_WARNING("Failed to start data raft transport")
                .put("RaftAddr", raft_addr_)
                .put("SnapshotAddr", snapshot_addr_)
                .put("Status", raft_status.ToString());
            server_.reset();
            snapshot_server_.reset();
            return FromByteraftStatus(raft_status);
        }
        raft_status = snapshot_server_->Start();
        if (!raft_status.ok()) {
            LOG_WARNING("Failed to start data raft snapshot transport")
                .put("RaftAddr", raft_addr_)
                .put("SnapshotAddr", snapshot_addr_)
                .put("Status", raft_status.ToString());
            byte::Status stop_status = server_->Stop();
            if (!stop_status.ok()) {
                LOG_WARNING("Failed to roll back data raft transport after snapshot start failure")
                    .put("RaftAddr", raft_addr_)
                    .put("SnapshotAddr", snapshot_addr_)
                    .put("Status", stop_status.ToString());
            }
            server_.reset();
            snapshot_server_.reset();
            return FromByteraftStatus(raft_status);
        }
        started_ = true;
        return Status::OK();
    }

    std::shared_ptr<byteraft::GroupContext> GroupContext() const { return group_context_; }

 private:
    std::mutex mu_;
    bool started_ = false;
    std::string raft_addr_;
    std::string snapshot_addr_;
    std::shared_ptr<byteraft::IAddressResolver> resolver_;
    std::shared_ptr<byteraft::IRaftTransport> raft_transport_;
    std::shared_ptr<byteraft::GroupContext> group_context_;
    std::unique_ptr<byteraft::MultiRaftServer> server_;
    std::unique_ptr<byteraft::MultiRaftServer> snapshot_server_;
};

std::shared_ptr<ByteraftRuntime> GetByteraftRuntime(const std::string& raft_addr,
                                                    const std::string& snapshot_addr) {
    static std::mutex mu;
    static std::map<std::string, std::weak_ptr<ByteraftRuntime>> runtimes;
    const std::string key = raft_addr + "|" + snapshot_addr;
    std::lock_guard<std::mutex> lock(mu);
    auto existing = runtimes[key].lock();
    if (existing != nullptr) {
        return existing;
    }
    auto runtime = std::make_shared<ByteraftRuntime>(raft_addr, snapshot_addr);
    runtimes[key] = runtime;
    return runtime;
}

class ByteraftDataRaftConsensusBackend final : public DataRaftConsensusBackend {
 public:
    ByteraftDataRaftConsensusBackend(DataRaftConsensusOptions options,
                                     DataRaftApplyFunc apply_func)
        : options_(std::move(options)),
          fsm_(new DataRaftFsm(options_.partition_id, std::move(apply_func),
                                options_.snapshot_func, options_.load_snapshot_func,
                                options_.initial_applied_index)) {}

    Status Start() override {
        if (running_) {
            return Status::OK();
        }
        if (options_.raft_addr.empty() || options_.snapshot_addr.empty()) {
            return Status::InvalidArgument("missing data raft address");
        }
        if (options_.wal_dir.empty()) {
            return Status::InvalidArgument("missing data raft wal dir");
        }
        runtime_ = GetByteraftRuntime(options_.raft_addr, options_.snapshot_addr);
        Status status = runtime_->Start();
        if (!status.ok()) {
            return status;
        }
        byte::Status fsm_status = fsm_->Open();
        if (!fsm_status.ok()) {
            return FromByteraftStatus(fsm_status);
        }
        fsm_opened_ = true;
        byteraft::Options raft_options = ToByteraftOptions(options_);
        raft_node_ = runtime_->GroupContext()->CreateRaftNode(raft_options, fsm_);
        byte::Status raft_status = raft_node_->Restart(true);
        if (raft_status.IsWALNotExists()) {
            raft_status = raft_node_->Start();
        } else if (raft_status.IsNotFound()) {
            raft_status = raft_node_->Restart(false);
        }
        status = FromByteraftStatus(raft_status);
        if (!status.ok()) {
            fsm_->Close();
            fsm_opened_ = false;
            return status;
        }
        running_ = true;
        return Status::OK();
    }

    void Stop() override {
        if (!running_ || raft_node_ == nullptr) {
            return;
        }
        byte::Status status = raft_node_->Stop();
        if (!status.ok()) {
            LOG_WARNING("Failed to stop data raft node")
                .put("PartitionId", options_.partition_id)
                .put("Status", status.ToString());
        }
        running_ = false;
        if (fsm_opened_) {
            byte::Status fsm_status = fsm_->Close();
            if (!fsm_status.ok()) {
                LOG_WARNING("Failed to close data raft FSM")
                    .put("PartitionId", options_.partition_id)
                    .put("Status", fsm_status.ToString());
            }
            fsm_opened_ = false;
        }
    }

    bool IsLeader() const override {
        return raft_node_ != nullptr && raft_node_->InLease();
    }

    Status GetStatus(DataRaftStatus* status) const override {
        if (status == nullptr) {
            return Status::InvalidArgument("missing data raft status output");
        }
        *status = DataRaftStatus();
        status->running = running_;
        if (raft_node_ == nullptr) {
            return Status::OK();
        }
        byteraft::LocalStatusInfo local = raft_node_->GetLocalStatus();
        status->leader = local.role.state == byteraft::RoleStatus::kLeader;
        status->learner = local.role.state == byteraft::RoleStatus::kLearner;
        status->term = local.term;
        status->leader_replica_id = local.leader_id;
        status->committed_index = local.committed_index;
        status->applied_index = local.applied_index;
        status->first_index = local.first_index;
        status->last_index = local.last_index;
        status->pending_config_change_index = local.pending_config_change_index;
        status->snapshot_creating = local.trigger_state == byteraft::LocalStatusInfo::kCreating;
        status->snapshot_loading = local.loading_state == byteraft::LocalStatusInfo::kLoading;
        for (const auto& node : raft_node_->GetMembership()) {
            if (node.role_state.IsLearner()) {
                ++status->learner_count;
            } else {
                ++status->voter_count;
            }
        }
        for (const byteraft::FatalDesc* fatal = raft_node_->GetFatalEvents(); fatal != nullptr;
             fatal = fatal->next) {
            ++status->fatal_event_count;
        }
        return Status::OK();
    }

    Status Propose(const std::string& serialized_entry, uint64_t* committed_index) override {
        std::lock_guard<std::mutex> propose_lock(propose_mu_);
        if (raft_node_ == nullptr || !running_) {
            return Status::FailedPrecondition("data raft node is not running");
        }

        const byteraft::LocalStatusInfo before = raft_node_->GetLocalStatus();
        LOG_INFO("Byteraft data propose begin")
            .put("PartitionId", options_.partition_id)
            .put("ReplicaId", options_.replica_id)
            .put("Role", before.role.state)
            .put("LeaderId", before.leader_id)
            .put("Term", before.term)
            .put("CommittedIndex", before.committed_index)
            .put("AppliedIndex", before.applied_index)
            .put("PayloadBytes", serialized_entry.size());
        const uint64_t target_index = before.last_index + 1;
        struct CallbackState {
            std::atomic<bool> done{false};
            std::mutex mu;
            Status status = Status::OK();
        };
        auto callback_state = std::make_shared<CallbackState>();
        byteraft::ProposeOptions propose_options;
        // TemporalStore data mutations must be normal Raft data entries so the
        // FSM applies them. Byteraft command entries are committed control
        // records and do not invoke FSM apply.
        propose_options.is_command = false;
        raft_node_->Propose(propose_options, serialized_entry,
                            [callback_state](const byte::Status& raft_status) {
            std::lock_guard<std::mutex> lock(callback_state->mu);
            callback_state->status = FromByteraftStatus(raft_status);
            callback_state->done = true;
        });

        Status result = Status::OK();
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(FLAGS_data_raft_propose_timeout_ms);
        while (std::chrono::steady_clock::now() < deadline) {
            const bool callback_done = callback_state->done.load();
            if (callback_done) {
                std::lock_guard<std::mutex> lock(callback_state->mu);
                if (!callback_state->status.ok()) {
                    result = callback_state->status;
                    break;
                }
            }
            const byteraft::LocalStatusInfo local = raft_node_->GetLocalStatus();
            if (local.applied_index >= target_index && callback_done) {
                result = Status::OK();
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (std::chrono::steady_clock::now() >= deadline && result.ok()) {
            const byteraft::LocalStatusInfo local = raft_node_->GetLocalStatus();
            if (local.applied_index < target_index || !callback_state->done.load()) {
                result = Status::DeadlineExceeded(
                    "data raft propose apply/callback poll timeout");
            }
        }

        const byteraft::LocalStatusInfo after = raft_node_->GetLocalStatus();
        LOG_INFO("Byteraft data propose finish")
            .put("PartitionId", options_.partition_id)
            .put("ReplicaId", options_.replica_id)
            .put("Role", after.role.state)
            .put("LeaderId", after.leader_id)
            .put("Term", after.term)
            .put("CommittedIndex", after.committed_index)
            .put("AppliedIndex", after.applied_index)
            .put("TargetIndex", target_index)
            .put("CallbackDone", callback_state->done.load())
            .put("Status", result);
        if (!result.ok()) {
            return result;
        }
        if (committed_index != nullptr) {
            *committed_index = target_index;
        }
        return Status::OK();
    }

    Status WaitForAppliedIndex(uint64_t index, uint64_t timeout_ms) override {
        if (raft_node_ == nullptr || !running_) {
            return Status::FailedPrecondition("data raft node is not running");
        }

        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeout_ms);
        while (std::chrono::steady_clock::now() < deadline) {
            const byteraft::LocalStatusInfo local = raft_node_->GetLocalStatus();
            if (local.applied_index >= index) {
                return Status::OK();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return Status::DeadlineExceeded("data raft apply wait timeout");
    }

    Status TriggerSnapshot(uint64_t* snapshot_index) override {
        if (raft_node_ == nullptr || !running_) {
            return Status::FailedPrecondition("data raft node is not running");
        }
        if (!options_.snapshot_func && !FLAGS_data_raft_enable_empty_snapshot_for_tests) {
            return Status::FailedPrecondition(
                "real data raft partition snapshot is not implemented; refusing unsafe empty snapshot");
        }
        struct CallbackState {
            std::mutex mu;
            std::condition_variable cv;
            bool done = false;
            Status status = Status::OK();
        };
        auto callback_state = std::make_shared<CallbackState>();
        raft_node_->AsyncSnapshot([callback_state](const byte::Status& raft_status) {
            {
                std::lock_guard<std::mutex> lock(callback_state->mu);
                callback_state->status = FromByteraftStatus(raft_status);
                callback_state->done = true;
            }
            callback_state->cv.notify_all();
        });
        Status result = Status::OK();
        {
            std::unique_lock<std::mutex> lock(callback_state->mu);
            const bool completed = callback_state->cv.wait_for(
                lock, std::chrono::milliseconds(FLAGS_data_raft_snapshot_trigger_timeout_ms),
                [&]() { return callback_state->done; });
            if (!completed) {
                const byteraft::LocalStatusInfo local = raft_node_->GetLocalStatus();
                result = Status::DeadlineExceeded("data raft snapshot trigger timeout");
                LOG_WARNING("Data raft snapshot trigger timed out")
                    .put("PartitionId", options_.partition_id)
                    .put("ReplicaId", options_.replica_id)
                    .put("Term", local.term)
                    .put("CommittedIndex", local.committed_index)
                    .put("AppliedIndex", local.applied_index)
                    .put("LastIndex", local.last_index)
                    .put("TimeoutMs", FLAGS_data_raft_snapshot_trigger_timeout_ms);
            } else {
                result = callback_state->status;
            }
        }
        if (result.ok() && snapshot_index != nullptr) {
            *snapshot_index = fsm_->AppliedIndex();
        }
        return result;
    }

    Status ReadIndex(uint64_t timeout_ms) override {
        if (raft_node_ == nullptr || !running_) {
            return Status::FailedPrecondition("data raft node is not running");
        }
        Status result;
        bthread::CountdownEvent done;
        raft_node_->ReadIndex([&](const byte::Status& raft_status) {
            result = FromByteraftStatus(raft_status);
            done.signal();
        }, timeout_ms);
        done.wait();
        return result;
    }

    Status AddPeer(const DataRaftPeer& peer) override {
        Status status = ValidatePeer(peer);
        if (!status.ok()) {
            return status;
        }
        status = EnsureLeaderForConfigChange("add voter");
        if (!status.ok()) {
            return status;
        }
        byteraft::NodeId existing;
        byte::Status resolve_status = raft_node_->ResolveAddress(peer.replica_id, &existing);
        if (resolve_status.ok()) {
            if (!SamePeerAddress(existing, peer)) {
                return Status::FailedPrecondition("data raft peer already exists with different address");
            }
            if (!existing.role_state.IsLearner()) {
                return Status::OK();
            }
            return PromotePeer(peer.replica_id);
        }
        Status result;
        bthread::CountdownEvent done;
        raft_node_->AddNode(ToByteraftNode(peer), [&](const byte::Status& raft_status) {
            result = FromByteraftStatus(raft_status);
            done.signal();
        }, FLAGS_data_raft_config_change_timeout_ms);
        done.wait();
        if (!result.ok()) {
            return result;
        }
        return WaitForPeerRole(peer.replica_id, true, false,
                               FLAGS_data_raft_config_change_timeout_ms);
    }

    Status AddLearner(const DataRaftPeer& peer) override {
        Status status = ValidatePeer(peer);
        if (!status.ok()) {
            return status;
        }
        status = EnsureLeaderForConfigChange("add learner");
        if (!status.ok()) {
            return status;
        }
        byteraft::NodeId existing;
        byte::Status resolve_status = raft_node_->ResolveAddress(peer.replica_id, &existing);
        if (resolve_status.ok()) {
            if (!SamePeerAddress(existing, peer)) {
                return Status::FailedPrecondition("data raft peer already exists with different address");
            }
            return Status::OK();
        }
        Status result;
        bthread::CountdownEvent done;
        raft_node_->AddLearner(ToByteraftNode(peer), [&](const byte::Status& raft_status) {
            result = FromByteraftStatus(raft_status);
            done.signal();
        }, FLAGS_data_raft_config_change_timeout_ms);
        done.wait();
        if (!result.ok()) {
            return result;
        }
        if (peer.auto_promote) {
            return WaitForPeerExists(peer.replica_id, FLAGS_data_raft_config_change_timeout_ms);
        }
        return WaitForPeerRole(peer.replica_id, true, true,
                               FLAGS_data_raft_config_change_timeout_ms);
    }

    Status PromotePeer(uint64_t replica_id) override {
        Status status = EnsureLeaderForConfigChange("promote learner");
        if (!status.ok()) {
            return status;
        }
        byteraft::NodeId node;
        byte::Status resolve_status = raft_node_->ResolveAddress(replica_id, &node);
        status = FromByteraftStatus(resolve_status);
        if (!status.ok()) {
            return status;
        }
        if (!node.role_state.IsLearner()) {
            return Status::OK();
        }
        Status result;
        bthread::CountdownEvent done;
        raft_node_->Promote(node, [&](const byte::Status& raft_status) {
            result = FromByteraftStatus(raft_status);
            done.signal();
        }, FLAGS_data_raft_config_change_timeout_ms);
        done.wait();
        if (!result.ok()) {
            return result;
        }
        return WaitForPeerRole(replica_id, true, false,
                               FLAGS_data_raft_config_change_timeout_ms);
    }

    Status RemovePeer(uint64_t replica_id) override {
        Status status = EnsureLeaderForConfigChange("remove peer");
        if (!status.ok()) {
            return status;
        }
        if (replica_id == options_.replica_id) {
            return Status::FailedPrecondition("refusing to remove local data raft leader");
        }
        byteraft::NodeId node;
        byte::Status resolve_status = raft_node_->ResolveAddress(replica_id, &node);
        if (!resolve_status.ok()) {
            return Status::OK();
        }
        const bool removing_voter = !node.role_state.IsLearner();
        if (removing_voter && !FLAGS_data_raft_allow_single_voter_group &&
            VoterCount() <= 1) {
            return Status::FailedPrecondition(
                "refusing to remove last data raft voter; set "
                "--data_raft_allow_single_voter_group only for smoke tests");
        }
        Status result;
        bthread::CountdownEvent done;
        raft_node_->RemoveNode(node, [&](const byte::Status& raft_status) {
            result = FromByteraftStatus(raft_status);
            done.signal();
        }, FLAGS_data_raft_config_change_timeout_ms);
        done.wait();
        if (!result.ok()) {
            return result;
        }
        return WaitForPeerRole(replica_id, false, false,
                               FLAGS_data_raft_config_change_timeout_ms);
    }

    Status TransferLeader(uint64_t replica_id) override {
        if (raft_node_ == nullptr || !running_) {
            return Status::FailedPrecondition("data raft node is not running");
        }
        if (!IsLeader()) {
            return Status::FailedPrecondition("data raft leadership transfer must run on leader");
        }
        if (replica_id == 0 || replica_id == options_.replica_id) {
            return Status::OK();
        }
        byteraft::NodeId node;
        byte::Status resolve_status = raft_node_->ResolveAddress(replica_id, &node);
        Status status = FromByteraftStatus(resolve_status);
        if (!status.ok()) {
            return status;
        }
        if (node.role_state.IsLearner()) {
            return Status::FailedPrecondition("cannot transfer data raft leadership to learner");
        }
        return FromByteraftStatus(raft_node_->TransferLeader(node));
    }

    Status Campaign(uint64_t timeout_ms, bool force) override {
        if (raft_node_ == nullptr || !running_) {
            return Status::FailedPrecondition("data raft node is not running");
        }
        Status result;
        bthread::CountdownEvent done;
        auto callback = [&](const byte::Status& raft_status) {
            result = FromByteraftStatus(raft_status);
            done.signal();
        };
        if (force) {
            raft_node_->ForcedCampaign(callback, timeout_ms);
        } else {
            raft_node_->Campaign(callback, timeout_ms);
        }
        done.wait();
        if (!result.ok()) {
            return result;
        }
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeout_ms);
        while (std::chrono::steady_clock::now() < deadline) {
            if (IsLeader()) {
                return Status::OK();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return IsLeader() ? Status::OK()
                          : Status::DeadlineExceeded("data raft campaign lease wait timeout");
    }

    Status CanServeBoundedStaleRead(uint64_t max_stale_index_lag) const override {
        if (raft_node_ == nullptr || !running_) {
            return Status::FailedPrecondition("data raft node is not running");
        }
        const byteraft::LocalStatusInfo local = raft_node_->GetLocalStatus();
        if (local.applied_index > local.committed_index) {
            return Status::DataLoss("data raft applied index is ahead of committed index");
        }
        const uint64_t lag = local.committed_index - local.applied_index;
        if (lag > max_stale_index_lag) {
            return Status::FailedPrecondition("data raft replica read lag exceeds bound");
        }
        return Status::OK();
    }

 private:
    DataRaftConsensusOptions options_;
    std::shared_ptr<DataRaftFsm> fsm_;
    std::shared_ptr<ByteraftRuntime> runtime_;
    std::shared_ptr<byteraft::RaftNode> raft_node_;
    std::mutex propose_mu_;
    bool running_ = false;
    bool fsm_opened_ = false;

    Status ValidatePeer(const DataRaftPeer& peer) const {
        if (peer.replica_id == 0) {
            return Status::InvalidArgument("missing data raft peer replica id");
        }
        if (peer.replica_id == options_.replica_id) {
            return Status::InvalidArgument("data raft peer is local replica");
        }
        if (peer.raft_addr.empty() || peer.snapshot_addr.empty()) {
            return Status::InvalidArgument("missing data raft peer address");
        }
        return Status::OK();
    }

    Status EnsureLeaderForConfigChange(const char* action) const {
        if (raft_node_ == nullptr || !running_) {
            return Status::FailedPrecondition("data raft node is not running");
        }
        const byteraft::LocalStatusInfo local = raft_node_->GetLocalStatus();
        if (local.role.state != byteraft::RoleStatus::kLeader || !raft_node_->InLease()) {
            return Status::FailedPrecondition(
                std::string("data raft config change requires active leader lease: ") + action);
        }
        if (local.loading_state != byteraft::LocalStatusInfo::kNone) {
            return Status::FailedPrecondition("data raft snapshot install is in progress");
        }
        if (local.pending_config_change_index != 0) {
            LOG_INFO("Data raft config change requested while Byteraft reports a pending config index")
                .put("PartitionId", options_.partition_id)
                .put("ReplicaId", options_.replica_id)
                .put("Action", action)
                .put("PendingConfigChangeIndex", local.pending_config_change_index)
                .put("CommittedIndex", local.committed_index)
                .put("AppliedIndex", local.applied_index);
        }
        return Status::OK();
    }

    Status WaitForConfigChangeApplied(uint64_t timeout_ms) const {
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeout_ms);
        while (std::chrono::steady_clock::now() < deadline) {
            const byteraft::LocalStatusInfo local = raft_node_->GetLocalStatus();
            if (local.pending_config_change_index == 0) {
                return Status::OK();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return Status::DeadlineExceeded("data raft config change apply wait timeout");
    }

    Status WaitForPeerRole(uint64_t replica_id, bool should_exist, bool learner,
                           uint64_t timeout_ms) const {
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeout_ms);
        while (std::chrono::steady_clock::now() < deadline) {
            if (PeerRoleMatches(replica_id, should_exist, learner)) {
                return Status::OK();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (PeerRoleMatches(replica_id, should_exist, learner)) {
            return Status::OK();
        }
        byteraft::LocalStatusInfo local = raft_node_->GetLocalStatus();
        std::ostringstream error;
        error << "data raft peer role wait timeout for replica " << replica_id
              << " should_exist=" << should_exist
              << " learner=" << learner
              << " pending_config_change_index=" << local.pending_config_change_index
              << " committed_index=" << local.committed_index
              << " applied_index=" << local.applied_index
              << " membership=";
        bool first = true;
        for (const auto& node : raft_node_->GetMembership()) {
            if (!first) {
                error << ",";
            }
            first = false;
            error << node.peer_id << (node.role_state.IsLearner() ? ":learner" : ":voter");
        }
        return Status::DeadlineExceeded(error.str());
    }

    Status WaitForPeerExists(uint64_t replica_id, uint64_t timeout_ms) const {
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeout_ms);
        while (std::chrono::steady_clock::now() < deadline) {
            byteraft::NodeId node;
            if (raft_node_->ResolveAddress(replica_id, &node).ok()) {
                return Status::OK();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return Status::DeadlineExceeded(
            "data raft peer existence wait timeout for replica " + std::to_string(replica_id));
    }

    bool PeerRoleMatches(uint64_t replica_id, bool should_exist, bool learner) const {
        for (const auto& node : raft_node_->GetMembership()) {
            if (node.peer_id == replica_id) {
                return should_exist && node.role_state.IsLearner() == learner;
            }
        }
        return !should_exist;
    }

    uint64_t VoterCount() const {
        uint64_t voters = 0;
        for (const auto& node : raft_node_->GetMembership()) {
            if (!node.role_state.IsLearner()) {
                ++voters;
            }
        }
        return voters;
    }
};

}  // namespace

std::unique_ptr<DataRaftConsensusBackend> NewUnavailableDataRaftConsensusBackend(
    const DataRaftConsensusOptions& options) {
    return std::unique_ptr<DataRaftConsensusBackend>(
        new UnavailableDataRaftConsensusBackend(options));
}

std::unique_ptr<DataRaftConsensusBackend> NewByteraftDataRaftConsensusBackend(
    const DataRaftConsensusOptions& options, DataRaftApplyFunc apply_func) {
    return std::unique_ptr<DataRaftConsensusBackend>(
        new ByteraftDataRaftConsensusBackend(options, std::move(apply_func)));
}

}  // namespace partition
}  // namespace bcache2
