#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "protocol/base.pb.h"

namespace bcache2 {
namespace partition {

// A clean data-node Raft contract.
//
// This interface is intentionally independent from a concrete Raft library so
// TemporalStore can keep the storage/FSM shape stable while choosing an
// open-source consensus backend. A production backend must provide transport,
// durable Raft WAL, committed FSM apply, snapshots, membership changes, and
// read-index/stale-read status.
struct DataRaftPeer {
    uint64_t replica_id = 0;
    std::string raft_addr;
    std::string snapshot_addr;
    bool auto_promote = false;
};

using DataRaftApplyFunc = std::function<Status(uint64_t raft_index, const std::string& data)>;
using DataRaftSnapshotFunc = std::function<Status(const std::string& path,
                                                  uint64_t* applied_index)>;
using DataRaftLoadSnapshotFunc = std::function<Status(const std::string& path)>;

struct DataRaftConsensusOptions {
    uint64_t partition_id = 0;
    uint64_t replica_id = 0;
    uint64_t group_id = 0;
    std::string raft_addr;
    std::string snapshot_addr;
    std::string wal_dir;
    std::string snapshot_dir;
    bool wal_sync = true;
    bool bootstrap_as_learner = false;
    std::vector<DataRaftPeer> peers;
    uint64_t initial_applied_index = 0;
    DataRaftSnapshotFunc snapshot_func;
    DataRaftLoadSnapshotFunc load_snapshot_func;
};

struct DataRaftStatus {
    bool running = false;
    bool leader = false;
    bool learner = false;
    uint64_t term = 0;
    uint64_t leader_replica_id = 0;
    uint64_t committed_index = 0;
    uint64_t applied_index = 0;
    uint64_t first_index = 0;
    uint64_t last_index = 0;
    uint64_t pending_config_change_index = 0;
    uint64_t voter_count = 0;
    uint64_t learner_count = 0;
    uint64_t fatal_event_count = 0;
    bool snapshot_creating = false;
    bool snapshot_loading = false;
};

class DataRaftConsensusBackend {
 public:
    virtual ~DataRaftConsensusBackend() = default;

    virtual Status Start() = 0;
    virtual void Stop() = 0;
    virtual bool IsLeader() const = 0;
    virtual Status GetStatus(DataRaftStatus* status) const = 0;

    // Propose a serialized DataRaftLogEntry. The backend must return only after
    // the entry is quorum-committed, or fail with a clear status.
    virtual Status Propose(const std::string& serialized_entry, uint64_t* committed_index) = 0;
    virtual Status WaitForAppliedIndex(uint64_t index, uint64_t timeout_ms) = 0;

    // Snapshot and membership operations are part of the contract even before
    // the first concrete backend is linked. This keeps autoscale/failover code
    // from growing a second control path.
    virtual Status TriggerSnapshot(uint64_t* snapshot_index) = 0;
    virtual Status ReadIndex(uint64_t timeout_ms) = 0;
    virtual Status AddPeer(const DataRaftPeer& peer) = 0;
    virtual Status AddLearner(const DataRaftPeer& peer) = 0;
    virtual Status PromotePeer(uint64_t replica_id) = 0;
    virtual Status RemovePeer(uint64_t replica_id) = 0;
    virtual Status TransferLeader(uint64_t replica_id) = 0;
    virtual Status Campaign(uint64_t timeout_ms, bool force) = 0;
    virtual Status CanServeBoundedStaleRead(uint64_t max_stale_index_lag) const = 0;
};

std::unique_ptr<DataRaftConsensusBackend> NewUnavailableDataRaftConsensusBackend(
    const DataRaftConsensusOptions& options);

std::unique_ptr<DataRaftConsensusBackend> NewByteraftDataRaftConsensusBackend(
    const DataRaftConsensusOptions& options, DataRaftApplyFunc apply_func);

}  // namespace partition
}  // namespace bcache2
