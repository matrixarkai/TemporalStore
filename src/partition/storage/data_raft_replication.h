#pragma once

#include <cstdint>
#include <string>

#include "common/status.h"
#include "protocol/server.pb.h"
#include "protocol/storage.pb.h"

namespace bcache2 {

namespace partition {

class ObjectManager;
class OpLogger;

// Serialized payload proposed to a data-node Byteraft group.
struct DataRaftLogEntry {
    uint64_t partition_id = 0;
    uint64_t raft_index = 0;
    uint64_t log_id = 0;
    uint32_t log_size = 0;
    storage::OpLog oplog;
};

// Serialized write command proposed before local mutation. This is the
// production Raft write payload shape: client command first, FSM apply later.
struct DataRaftCommandEntry {
    uint64_t partition_id = 0;
    uint64_t raft_index = 0;
    uint64_t request_id = 0;
    BatchExecuteCmdRequest request;
};

Status SerializeDataRaftLog(const DataRaftLogEntry& entry, std::string* out);
Status ParseDataRaftLog(const std::string& data, DataRaftLogEntry* entry);
Status SerializeDataRaftCommand(const DataRaftCommandEntry& entry, std::string* out);
Status ParseDataRaftCommand(const std::string& data, DataRaftCommandEntry* entry);

class DataRaftCommittedLogApplier {
 public:
    DataRaftCommittedLogApplier(uint64_t partition_id, ObjectManager* object_manager,
                                OpLogger* op_logger);

    Status Apply(uint64_t raft_index, const std::string& committed_log);

    uint64_t AppliedRaftIndex() const { return applied_raft_index_; }
    uint64_t AppliedOplogSequence() const { return applied_oplog_sequence_; }

 private:
    uint64_t partition_id_ = 0;
    ObjectManager* object_manager_ = nullptr;
    OpLogger* op_logger_ = nullptr;
    uint64_t applied_raft_index_ = 0;
    uint64_t applied_oplog_sequence_ = 0;
};

}  // namespace partition
}  // namespace bcache2
