#include "partition/storage/data_raft_replication.h"

#include <cstring>
#include <limits>
#include <utility>

#include "partition/storage/object_manager.h"
#include "partition/storage/op_logger.h"

namespace bcache2 {
namespace partition {

namespace {

constexpr uint32_t kDataRaftLogMagic = 0x54535246;  // "TSRF"
constexpr uint32_t kDataRaftCommandMagic = 0x54535243;  // "TSRC"
constexpr uint32_t kDataRaftLogVersion = 1;
constexpr size_t kFixedHeaderSize = sizeof(uint32_t) * 3 + sizeof(uint64_t) * 5;
constexpr size_t kFixedCommandHeaderSize = sizeof(uint32_t) * 3 + sizeof(uint64_t) * 3;

void AppendFixed32(uint32_t value, std::string* out) {
    out->append(reinterpret_cast<const char*>(&value), sizeof(value));
}

void AppendFixed64(uint64_t value, std::string* out) {
    out->append(reinterpret_cast<const char*>(&value), sizeof(value));
}

bool ReadFixed32(const char** cursor, size_t* remaining, uint32_t* value) {
    if (*remaining < sizeof(*value)) {
        return false;
    }
    std::memcpy(value, *cursor, sizeof(*value));
    *cursor += sizeof(*value);
    *remaining -= sizeof(*value);
    return true;
}

bool ReadFixed64(const char** cursor, size_t* remaining, uint64_t* value) {
    if (*remaining < sizeof(*value)) {
        return false;
    }
    std::memcpy(value, *cursor, sizeof(*value));
    *cursor += sizeof(*value);
    *remaining -= sizeof(*value);
    return true;
}

}  // namespace

Status SerializeDataRaftLog(const DataRaftLogEntry& entry, std::string* out) {
    if (out == nullptr) {
        return Status::InvalidArgument("missing output buffer");
    }
    if (entry.oplog.sequence() == 0) {
        return Status::InvalidArgument("missing oplog sequence");
    }

    std::string oplog_bytes;
    if (!entry.oplog.SerializeToString(&oplog_bytes)) {
        return Status::Internal("serialize oplog failed");
    }

    out->clear();
    out->reserve(kFixedHeaderSize + oplog_bytes.size());
    AppendFixed32(kDataRaftLogMagic, out);
    AppendFixed32(kDataRaftLogVersion, out);
    AppendFixed64(entry.partition_id, out);
    AppendFixed64(entry.raft_index, out);
    AppendFixed64(entry.log_id, out);
    AppendFixed64(entry.log_size == 0 ? oplog_bytes.size() : entry.log_size, out);
    AppendFixed64(entry.oplog.sequence(), out);
    AppendFixed32(static_cast<uint32_t>(oplog_bytes.size()), out);
    out->append(oplog_bytes);
    return Status::OK();
}

Status ParseDataRaftLog(const std::string& data, DataRaftLogEntry* entry) {
    if (entry == nullptr) {
        return Status::InvalidArgument("missing output entry");
    }
    const char* cursor = data.data();
    size_t remaining = data.size();

    uint32_t magic = 0;
    uint32_t version = 0;
    uint64_t partition_id = 0;
    uint64_t raft_index = 0;
    uint64_t log_id = 0;
    uint64_t log_size = 0;
    uint64_t oplog_sequence = 0;
    uint32_t oplog_size = 0;

    if (!ReadFixed32(&cursor, &remaining, &magic) ||
        !ReadFixed32(&cursor, &remaining, &version) ||
        !ReadFixed64(&cursor, &remaining, &partition_id) ||
        !ReadFixed64(&cursor, &remaining, &raft_index) ||
        !ReadFixed64(&cursor, &remaining, &log_id) ||
        !ReadFixed64(&cursor, &remaining, &log_size) ||
        !ReadFixed64(&cursor, &remaining, &oplog_sequence) ||
        !ReadFixed32(&cursor, &remaining, &oplog_size)) {
        return Status::InvalidArgument("data raft log header is incomplete");
    }

    if (magic != kDataRaftLogMagic) {
        return Status::InvalidArgument("invalid data raft log magic");
    }
    if (version != kDataRaftLogVersion) {
        return Status::InvalidArgument("unsupported data raft log version");
    }
    if (remaining != oplog_size) {
        return Status::InvalidArgument("data raft log size mismatch");
    }
    if (log_size > std::numeric_limits<uint32_t>::max()) {
        return Status::InvalidArgument("data raft log record too large");
    }

    storage::OpLog oplog;
    if (!oplog.ParseFromArray(cursor, static_cast<int>(remaining))) {
        return Status::DataLoss("parse committed oplog failed");
    }
    if (oplog.sequence() != oplog_sequence) {
        return Status::DataLoss("oplog sequence mismatch");
    }

    entry->partition_id = partition_id;
    entry->raft_index = raft_index;
    entry->log_id = log_id;
    entry->log_size = static_cast<uint32_t>(log_size);
    entry->oplog = std::move(oplog);
    return Status::OK();
}

Status SerializeDataRaftCommand(const DataRaftCommandEntry& entry, std::string* out) {
    if (out == nullptr) {
        return Status::InvalidArgument("missing output buffer");
    }
    if (entry.partition_id == 0) {
        return Status::InvalidArgument("missing partition id");
    }
    if (entry.request.partition_id() != 0 && entry.request.partition_id() != entry.partition_id) {
        return Status::InvalidArgument("request partition id mismatch");
    }
    if (entry.request.request_size() == 0) {
        return Status::InvalidArgument("empty raft command request");
    }

    std::string request_bytes;
    if (!entry.request.SerializeToString(&request_bytes)) {
        return Status::Internal("serialize raft command request failed");
    }

    out->clear();
    out->reserve(kFixedCommandHeaderSize + request_bytes.size());
    AppendFixed32(kDataRaftCommandMagic, out);
    AppendFixed32(kDataRaftLogVersion, out);
    AppendFixed64(entry.partition_id, out);
    AppendFixed64(entry.raft_index, out);
    AppendFixed64(entry.request_id, out);
    AppendFixed32(static_cast<uint32_t>(request_bytes.size()), out);
    out->append(request_bytes);
    return Status::OK();
}

Status ParseDataRaftCommand(const std::string& data, DataRaftCommandEntry* entry) {
    if (entry == nullptr) {
        return Status::InvalidArgument("missing output entry");
    }
    const char* cursor = data.data();
    size_t remaining = data.size();

    uint32_t magic = 0;
    uint32_t version = 0;
    uint64_t partition_id = 0;
    uint64_t raft_index = 0;
    uint64_t request_id = 0;
    uint32_t request_size = 0;

    if (!ReadFixed32(&cursor, &remaining, &magic) ||
        !ReadFixed32(&cursor, &remaining, &version) ||
        !ReadFixed64(&cursor, &remaining, &partition_id) ||
        !ReadFixed64(&cursor, &remaining, &raft_index) ||
        !ReadFixed64(&cursor, &remaining, &request_id) ||
        !ReadFixed32(&cursor, &remaining, &request_size)) {
        return Status::InvalidArgument("data raft command header is incomplete");
    }

    if (magic != kDataRaftCommandMagic) {
        return Status::InvalidArgument("invalid data raft command magic");
    }
    if (version != kDataRaftLogVersion) {
        return Status::InvalidArgument("unsupported data raft command version");
    }
    if (remaining != request_size) {
        return Status::InvalidArgument("data raft command size mismatch");
    }

    BatchExecuteCmdRequest request;
    if (!request.ParseFromArray(cursor, static_cast<int>(remaining))) {
        return Status::DataLoss("parse committed command request failed");
    }
    if (request.partition_id() != 0 && request.partition_id() != partition_id) {
        return Status::DataLoss("committed command partition id mismatch");
    }
    if (request.request_size() == 0) {
        return Status::DataLoss("committed command request is empty");
    }

    entry->partition_id = partition_id;
    entry->raft_index = raft_index;
    entry->request_id = request_id;
    entry->request = std::move(request);
    return Status::OK();
}

DataRaftCommittedLogApplier::DataRaftCommittedLogApplier(uint64_t partition_id,
                                                         ObjectManager* object_manager,
                                                         OpLogger* op_logger)
    : partition_id_(partition_id), object_manager_(object_manager), op_logger_(op_logger) {}

Status DataRaftCommittedLogApplier::Apply(uint64_t raft_index, const std::string& committed_log) {
    if (object_manager_ == nullptr) {
        return Status::FailedPrecondition("missing object manager");
    }
    if (op_logger_ == nullptr) {
        return Status::FailedPrecondition("missing op logger");
    }
    if (raft_index != 0 && raft_index <= applied_raft_index_) {
        return Status::OK();
    }

    DataRaftLogEntry entry;
    Status status = ParseDataRaftLog(committed_log, &entry);
    if (!status.ok()) {
        return status;
    }
    if (entry.partition_id != partition_id_) {
        return Status::InvalidArgument("committed log belongs to another partition");
    }
    if (entry.raft_index != 0 && raft_index != 0 && entry.raft_index != raft_index) {
        return Status::InvalidArgument("raft index mismatch");
    }

    uint64_t local_log_id = entry.log_id;
    uint32_t local_log_size = entry.log_size;
    status = op_logger_->AppendReplayedLog(entry.oplog, &local_log_id, &local_log_size);
    if (!status.ok()) {
        return status;
    }
    if (local_log_id == 0 && local_log_size == 0) {
        applied_raft_index_ = raft_index == 0 ? entry.raft_index : raft_index;
        applied_oplog_sequence_ = entry.oplog.sequence();
        return Status::OK();
    }

    status = object_manager_->ReplayOplog(local_log_id, local_log_size, entry.oplog);
    if (!status.ok()) {
        return status;
    }

    applied_raft_index_ = raft_index == 0 ? entry.raft_index : raft_index;
    applied_oplog_sequence_ = entry.oplog.sequence();
    return Status::OK();
}

}  // namespace partition
}  // namespace bcache2
