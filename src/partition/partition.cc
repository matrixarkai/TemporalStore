// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/partition.h"

#include <absl/strings/string_view.h>
#include <byte/thread/async_thread.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <memory>
#include <set>
#include <sstream>
#include <thread>
#include <utility>
#include <vector>

#include "bthread/bthread.h"
#include "bthread/countdown_event.h"
#include "butil/endpoint.h"
#include "butil/files/file_path.h"
#include "common/cmd_manager.h"
#include "common/coclosure.h"
#include "common/logging.h"
#include "common/scoped_invoker.h"
#include "common/time_tracer.h"
#include "partition/metrics.h"
#include "partition/remote_partition_stream.h"
#include "partition/storage/data_raft_consensus.h"
#include "partition/storage/data_raft_replication.h"
#include "partition/storage/evicter.h"
#include "partition/storage/expirer.h"
#include "partition/storage/object_manager.h"
#include "partition/storage/page_compactor.h"
#include "partition/storage/page_gc.h"
#include "partition/storage/replicator.h"
#include "partition/storage/slot_context_manager.h"
#include "partition/storage/slot_store.h"
#include "partition/storage/storage_manager.h"
#include "protocol/config.pb.h"

DECLARE_string(data_replication_mode);
DECLARE_uint64(data_raft_propose_timeout_ms);
DECLARE_uint32(data_raft_raft_port_delta);
DECLARE_uint32(data_raft_snapshot_port_delta);
DECLARE_string(data_raft_work_dir);

namespace bcache2 {
namespace partition {
namespace fs = std::filesystem;

DEFINE_bool(secondary_pull_stream_from_primary, false,
            "When true, readonly secondary partitions pull index/oplog/page stream payloads "
            "from the primary through ServerService RPCs after GetInfo. When false, "
            "secondaries open the stream URI directly through Env, which preserves the old "
            "shared object-store/local-file recovery path.");
DEFINE_uint64(remote_partition_info_timeout_ms, 10000,
              "Timeout for readonly secondary partitions to fetch primary partition info.");
DEFINE_uint64(data_raft_partition_snapshot_owner_wait_timeout_ms, 5000,
              "Timeout while a data-Raft snapshot worker waits for the partition owner "
              "thread to export local partition files.");
DEFINE_bool(data_raft_snapshot_commit_index_before_copy, false,
            "Commit index stream before copying data-Raft snapshot files. Keep false unless "
            "the stream backend can complete Commit without re-entering the partition owner "
            "thread.");

static void OnBrpcDone(CoSyncClosure* sync) { sync->Run(); }

static bool IsDataRaftConsensusMode() {
    return ::FLAGS_data_replication_mode == "raft_consensus";
}

static bool UsePrimaryPullStreams() {
    return FLAGS_secondary_pull_stream_from_primary ||
           ::FLAGS_data_replication_mode == "primary_pull";
}

static std::string HostPort(const std::string& host, uint32_t port) {
    std::ostringstream out;
    out << host << ":" << port;
    return out.str();
}

static Status DataRaftHostPort(const std::string& host, uint32_t base_port, uint32_t delta,
                               const char* name, std::string* addr) {
    const uint64_t derived_port = static_cast<uint64_t>(base_port) + delta;
    if (base_port == 0 || derived_port > 65535) {
        std::ostringstream error;
        error << "invalid data raft " << name << " port: base_port=" << base_port
              << " delta=" << delta << " derived_port=" << derived_port;
        return Status::InvalidArgument(error.str());
    }
    *addr = HostPort(host, static_cast<uint32_t>(derived_port));
    return Status::OK();
}

static bool LocalFilesystemUriToPath(const std::string& uri, std::string* path) {
    static const char* kPrefixes[] = {
        "file://",
        "shared-file://",
        "shared://",
        "efs://",
        "nfs://",
    };
    for (const char* prefix : kPrefixes) {
        const size_t prefix_len = std::strlen(prefix);
        if (uri.rfind(prefix, 0) == 0) {
            *path = uri.substr(prefix_len);
            return !path->empty();
        }
    }
    return false;
}

static Status CopyPathForDataRaftSnapshot(const fs::path& src, const fs::path& dst) {
    std::error_code ec;
    if (!fs::exists(src, ec)) {
        return Status::OK();
    }
    fs::remove_all(dst, ec);
    ec.clear();
    if (fs::is_directory(src, ec)) {
        fs::create_directories(dst.parent_path(), ec);
        if (ec) {
            return Status::Internal("create snapshot parent failed: " + ec.message());
        }
        fs::copy(src, dst, fs::copy_options::recursive | fs::copy_options::overwrite_existing, ec);
    } else {
        fs::create_directories(dst.parent_path(), ec);
        if (ec) {
            return Status::Internal("create snapshot parent failed: " + ec.message());
        }
        fs::copy_file(src, dst, fs::copy_options::overwrite_existing, ec);
    }
    if (ec) {
        return Status::Internal("copy data raft snapshot path failed: " + ec.message());
    }
    return Status::OK();
}

static Status FsyncDirectory(const fs::path& path) {
    const int fd = open(path.string().c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) {
        return Status::Internal("open dir for fsync failed: " + path.string() + ": " +
                                strerror(errno));
    }
    if (fsync(fd) != 0) {
        const std::string error = strerror(errno);
        close(fd);
        return Status::Internal("fsync dir failed: " + path.string() + ": " + error);
    }
    if (close(fd) != 0) {
        return Status::Internal("close fsync dir failed: " + path.string() + ": " +
                                strerror(errno));
    }
    return Status::OK();
}

static Status WriteFileDurably(const std::string& path, const std::string& content) {
    const fs::path dst(path);
    const fs::path parent = dst.parent_path().empty() ? fs::path(".") : dst.parent_path();
    std::error_code ec;
    fs::create_directories(parent, ec);
    if (ec) {
        return Status::Internal("create durable file dir failed: " + path + ": " + ec.message());
    }

    const std::string tmp_path = path + ".tmp";
    const int fd = open(tmp_path.c_str(), O_CREAT | O_TRUNC | O_WRONLY | O_CLOEXEC, 0644);
    if (fd < 0) {
        return Status::Internal("open durable temp file failed: " + tmp_path + ": " +
                                strerror(errno));
    }
    const char* data = content.data();
    size_t remaining = content.size();
    while (remaining > 0) {
        const ssize_t written = write(fd, data, remaining);
        if (written < 0) {
            const std::string error = strerror(errno);
            close(fd);
            return Status::Internal("write durable temp file failed: " + tmp_path + ": " +
                                    error);
        }
        data += written;
        remaining -= static_cast<size_t>(written);
    }
    if (fsync(fd) != 0) {
        const std::string error = strerror(errno);
        close(fd);
        return Status::Internal("fsync durable temp file failed: " + tmp_path + ": " + error);
    }
    if (close(fd) != 0) {
        return Status::Internal("close durable temp file failed: " + tmp_path + ": " +
                                strerror(errno));
    }

    fs::rename(tmp_path, path, ec);
    if (ec) {
        fs::remove(tmp_path, ec);
        return Status::Internal("rename durable temp file failed: " + path + ": " +
                                ec.message());
    }
    Status status = FsyncDirectory(parent);
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}

static Status WriteDataRaftSnapshotManifest(const fs::path& snapshot_dir, uint64_t partition_id,
                                            const std::string& storage_uri,
                                            uint64_t applied_index) {
    std::ostringstream manifest;
    manifest << "version=1\n";
    manifest << "partition_id=" << partition_id << "\n";
    manifest << "storage_uri=" << storage_uri << "\n";
    manifest << "applied_index=" << applied_index << "\n";
    manifest << "complete=1\n";
    return WriteFileDurably((snapshot_dir / "temporalstore_partition_snapshot.manifest").string(),
                            manifest.str());
}

static Status ReadDataRaftSnapshotManifest(const fs::path& snapshot_dir, uint64_t partition_id,
                                           uint64_t* applied_index) {
    const fs::path manifest_path = snapshot_dir / "temporalstore_partition_snapshot.manifest";
    if (!fs::exists(manifest_path)) {
        return Status::DataLoss("missing data raft partition snapshot manifest");
    }

    uint64_t manifest_partition_id = 0;
    uint64_t snapshot_index = 0;
    bool complete = false;
    std::ifstream manifest(manifest_path.string());
    std::string line;
    while (std::getline(manifest, line)) {
        static const std::string kPartitionPrefix = "partition_id=";
        static const std::string kAppliedPrefix = "applied_index=";
        static const std::string kCompletePrefix = "complete=";
        if (line.rfind(kPartitionPrefix, 0) == 0) {
            manifest_partition_id =
                std::strtoull(line.substr(kPartitionPrefix.size()).c_str(), nullptr, 10);
        } else if (line.rfind(kAppliedPrefix, 0) == 0) {
            snapshot_index =
                std::strtoull(line.substr(kAppliedPrefix.size()).c_str(), nullptr, 10);
        } else if (line.rfind(kCompletePrefix, 0) == 0) {
            complete = line.substr(kCompletePrefix.size()) == "1";
        }
    }
    if (!manifest.good() && !manifest.eof()) {
        return Status::DataLoss("read data raft partition snapshot manifest failed");
    }
    if (manifest_partition_id != partition_id) {
        return Status::DataLoss("data raft partition snapshot manifest partition mismatch");
    }
    if (snapshot_index == 0) {
        return Status::DataLoss("invalid data raft partition snapshot applied index");
    }
    if (!complete) {
        return Status::DataLoss("incomplete data raft partition snapshot manifest");
    }
    if (applied_index != nullptr) {
        *applied_index = snapshot_index;
    }
    return Status::OK();
}

static Status AtomicReplacePathFromDataRaftSnapshot(const fs::path& src, const fs::path& dst) {
    std::error_code ec;
    if (!fs::exists(src, ec)) {
        fs::remove_all(dst, ec);
        return Status::OK();
    }

    fs::path tmp = dst;
    tmp += ".raft_snapshot_tmp";
    fs::remove_all(tmp, ec);
    ec.clear();
    Status status = CopyPathForDataRaftSnapshot(src, tmp);
    if (!status.ok()) {
        fs::remove_all(tmp, ec);
        return status;
    }

    fs::remove_all(dst, ec);
    ec.clear();
    fs::rename(tmp, dst, ec);
    if (ec) {
        fs::remove_all(tmp, ec);
        return Status::Internal("install data raft snapshot path failed: " + ec.message());
    }
    return Status::OK();
}

static std::vector<fs::path> PartitionLocalFilePaths(const std::string& base_path) {
    std::vector<fs::path> paths;
    fs::path base(base_path);
    paths.push_back(fs::path(base_path + "-condition"));
    paths.push_back(fs::path(base_path + "-index"));
    paths.push_back(fs::path(base_path + "-oplog"));

    const fs::path parent = base.parent_path().empty() ? fs::path(".") : base.parent_path();
    const std::string prefix = base.filename().string() + "-page";
    std::error_code ec;
    if (fs::exists(parent, ec)) {
        for (const auto& entry : fs::directory_iterator(parent, ec)) {
            if (ec) {
                break;
            }
            const std::string name = entry.path().filename().string();
            if (name.rfind(prefix, 0) == 0) {
                paths.push_back(entry.path());
            }
        }
    }
    return paths;
}

static bool HasExistingDataRaftWal(const std::string& wal_dir) {
    std::error_code ec;
    const fs::path path(wal_dir);
    if (!fs::exists(path, ec)) {
        return false;
    }
    if (!fs::is_directory(path, ec)) {
        return fs::file_size(path, ec) > 0;
    }
    fs::directory_iterator it(path, ec);
    if (ec) {
        return false;
    }
    return it != fs::directory_iterator();
}

static std::string DataRaftWalDirForPartition(uint64_t partition_id) {
    return FLAGS_data_raft_work_dir + "/wal/" + std::to_string(partition_id);
}

static std::string DataRaftSnapshotDirForPartition(uint64_t partition_id) {
    return FLAGS_data_raft_work_dir + "/snapshot/" + std::to_string(partition_id);
}

static std::string DataRaftAppliedIndexPathForPartition(uint64_t partition_id) {
    return FLAGS_data_raft_work_dir + "/applied/" + std::to_string(partition_id);
}

static Status GuardMissingDataRaftAppliedIndex(uint64_t partition_id,
                                               const std::string& applied_index_path) {
    if (HasExistingDataRaftWal(DataRaftWalDirForPartition(partition_id))) {
        return Status::DataLoss(
            "missing data raft applied-index checkpoint for existing raft WAL: " +
            applied_index_path);
    }
    return Status::OK();
}

static const MembershipInfo::Unit* FindMembershipUnitForPartition(const MembershipInfo& info,
                                                                  uint64_t partition_id) {
    for (const auto& unit : info.units()) {
        for (uint64_t active_partition_id : unit.active_id_list()) {
            if (active_partition_id == partition_id) {
                return &unit;
            }
        }
    }
    return nullptr;
}

static bool UnitContainsPartition(const MembershipInfo::Unit* unit, uint64_t partition_id) {
    if (unit == nullptr) {
        return false;
    }
    for (uint64_t active_partition_id : unit->active_id_list()) {
        if (active_partition_id == partition_id) {
            return true;
        }
    }
    return false;
}

static uint64_t StableDataRaftGroupPartitionId(const MembershipInfo::Unit& unit,
                                               uint64_t fallback_partition_id) {
    uint64_t group_partition_id = std::numeric_limits<uint64_t>::max();
    for (uint64_t partition_id : unit.active_id_list()) {
        group_partition_id = std::min(group_partition_id, partition_id);
    }
    for (uint64_t partition_id : unit.frozen_id_list()) {
        group_partition_id = std::min(group_partition_id, partition_id);
    }
    return group_partition_id == std::numeric_limits<uint64_t>::max() ? fallback_partition_id
                                                                      : group_partition_id;
}

static std::vector<DataRaftPeer> BuildDataRaftPeersForPartition(const MembershipInfo& info,
                                                                uint64_t partition_id) {
    std::vector<DataRaftPeer> peers;
    const MembershipInfo::Unit* unit = FindMembershipUnitForPartition(info, partition_id);
    for (const auto& placement : info.placements()) {
        if (!UnitContainsPartition(unit, placement.id())) {
            continue;
        }
        if (!placement.placement().has_server() || placement.placement().server().port() == 0) {
            continue;
        }
        const auto& server = placement.placement().server();
        DataRaftPeer peer;
        peer.replica_id = placement.id();
        Status status = DataRaftHostPort(server.ip4(), server.port(),
                                         FLAGS_data_raft_raft_port_delta,
                                         "raft", &peer.raft_addr);
        if (!status.ok()) {
            LOG_WARNING("Skipping data raft peer with invalid raft address")
                .put("PartitionId", partition_id)
                .put("PeerId", placement.id())
                .put("BasePort", server.port())
                .put("Status", status);
            continue;
        }
        status = DataRaftHostPort(server.ip4(), server.port(),
                                  FLAGS_data_raft_snapshot_port_delta,
                                  "snapshot", &peer.snapshot_addr);
        if (!status.ok()) {
            LOG_WARNING("Skipping data raft peer with invalid snapshot address")
                .put("PartitionId", partition_id)
                .put("PeerId", placement.id())
                .put("BasePort", server.port())
                .put("Status", status);
            continue;
        }
        peer.auto_promote = placement.id() != unit->primary_id();
        peers.push_back(std::move(peer));
    }
    return peers;
}

Partition::Partition(const Options& options)
    : options_(options),
      blockcache_(options.blockcache),
      readonly_(options.readonly),
      membership_info_(std::move(options.membership)),
      metrics_manager_(
          new MetricsManager({{"partition_id", std::to_string(options.partition_id)},  //
                              {"table", options.table_name}},
                             "partition")),
      allocator_manager_(new AllocatorManager(metrics_manager_.get())),
      slot_context_manager_(
          new SlotContextManager(metrics_manager_.get(), allocator_manager_.get())),
      cmd_metrics_(new RequestMetrics(metrics_manager_.get(), "cmd", {})),
      index_(new Index(this, allocator_manager_.get(), slot_context_manager_.get(),
                       metrics_manager_.get())),
      op_logger_(
          new OpLogger(this, index_.get(), slot_context_manager_.get(), metrics_manager_.get())),
      page_store_(new PageStore(this, index_.get(), options_.env, metrics_manager_.get(),
                                options_.config.stream_config().store_rep_policy(),
                                options.partition_id, blockcache_)),
      slot_store_(new SlotStore(this, index_.get(), page_store_.get(), op_logger_.get(),
                                slot_context_manager_.get(), metrics_manager_.get())),
      object_manager_(new ObjectManager(this, index_.get(), page_store_.get(), op_logger_.get(),
                                        slot_store_.get(), slot_context_manager_.get(),
                                        allocator_manager_.get())),
      evicter_(new Evicter(this, object_manager_.get(), index_.get(), op_logger_.get(),
                           slot_store_.get(), allocator_manager_.get(), metrics_manager_.get())),
      page_gc_(new PageGc(this, options_.env, index_.get(), page_store_.get(), slot_store_.get(),
                          op_logger_.get(), metrics_manager_.get())),
      page_compactor_(
          new PageCompactor(this, index_.get(), slot_store_.get(), metrics_manager_.get())),
      expirer_(new Expirer(index_.get(), object_manager_.get(), op_logger_.get(),
                           metrics_manager_.get())),
      cmd_executor_(new CmdExecutor(this, object_manager_.get(),
                    op_logger_.get(), metrics_manager_.get())),
      storage_manager_(new StorageManager(
          this, cmd_executor_.get(), index_.get(), page_store_.get(), op_logger_.get(),
          slot_store_.get(), evicter_.get(), expirer_.get(), object_manager_.get(), page_gc_.get(),
          page_compactor_.get(), slot_context_manager_.get(), metrics_manager_.get(),
          allocator_manager_.get())),
      replicator_(new Replicator(this, index_.get(), op_logger_.get(), page_store_.get(),
                                 object_manager_.get(), metrics_manager_.get())),
      cmd_(new CmdExecutorManager(object_manager_.get(), metrics_manager_.get())) {
    slot_store_->SetObjectManager(object_manager_.get());
    load_success_ = metrics_manager_->Get<MetricsEnv::Counter>(kMetricsLoadPartitionSuccess, {});
    load_failed_ = metrics_manager_->Get<MetricsEnv::Counter>(kMetricsLoadPartitionFailed, {});
    load_latency_ = metrics_manager_->Get<MetricsEnv::Histogram>(kMetricsLoadPartitionLatency, {});
    unload_success_ =
        metrics_manager_->Get<MetricsEnv::Counter>(kMetricsUnLoadPartitionSuccess, {});
    unload_latency_ =
        metrics_manager_->Get<MetricsEnv::Histogram>(kMetricsUnLoadPartitionLatency, {});

    InitMembership();
}

Partition::~Partition() {}

Status Partition::Load() {
    LOG_CALL_INFO().put("PartitionId", options_.partition_id);
    if (!IsCoContext()) {
        LOG_WARNING("Partition load running outside coroutine context")
            .put("PartitionId", options_.partition_id);
    }

    if (stage_ != PartitionLoadStage::INIT) {
        LOG_WARNING("Partition not init state").put("PartitionId", options_.partition_id);
        return Status::FailedPrecondition("Partition not init state");
    }

    BYTE_DEFER({
        if (stage_ != PartitionLoadStage::LOADED) {
            stage_ = PartitionLoadStage::FAILED;
            load_failed_->get()->Increment();
            LOG_WARNING("Partition load failed").put("PartitionId", options_.partition_id);
        }
    });
    stage_ = PartitionLoadStage::LOADING;
    ScopedLatency latency(load_latency_->get());
    TimeTracer tracer;

    LOG_INFO("Partition start load")
        .put("PartitionId", options_.partition_id)
        .put("Uri", options_.uri)
        .put("LoadVersion", options_.load_version)
        .put("Config", options_.config.ShortDebugString());
    Status status = SetupCondition();
    if (!status.ok()) {
        LOG_ERROR("Setup condition failed")
            .put("PartitionId", options_.partition_id)
            .put("Status", status);
        return status;
    }
    tracer.AddEvent("SetupCondition");

    PartitionInfo remote_info;
    status = SetupRemoteInfo(&remote_info);
    if (!status.ok()) {
        LOG_ERROR("Setup remote info failed")
            .put("PartitionId", options_.partition_id)
            .put("Status", status);
        return status;
    }
    tracer.AddEvent("SetupRemoteInfo");

    status = SetupIndex(remote_info);
    if (!status.ok()) {
        LOG_ERROR("Setup index failed")
            .put("PartitionId", options_.partition_id)
            .put("Status", status);
        return status;
    }
    tracer.AddEvent("SetupIndex");

    status = SetupOplogger(remote_info);
    if (!status.ok()) {
        LOG_ERROR("Setup oplogger failed")
            .put("PartitionId", options_.partition_id)
            .put("Status", status);
        return status;
    }
    tracer.AddEvent("SetupOplogger");

    status = SetupPageStore(remote_info);
    if (!status.ok()) {
        LOG_ERROR("Setup page store failed")
            .put("PartitionId", options_.partition_id)
            .put("Status", status);
        return status;
    }
    tracer.AddEvent("SetupPageStore");

    status = SetupObjectManager();
    if (!status.ok()) {
        LOG_ERROR("Setup object manager failed")
            .put("PartitionId", options_.partition_id)
            .put("Status", status);
        return status;
    }
    tracer.AddEvent("SetupObjectManager");

    status = SetupStorageManager();
    if (!status.ok()) {
        LOG_ERROR("Setup storage manager failed")
            .put("PartitionId", options_.partition_id)
            .put("Status", status);
        return status;
    }
    tracer.AddEvent("SetupStorageManager");

    status = SetupReplicator();
    if (!status.ok()) {
        LOG_ERROR("Setup replicator failed")
            .put("PartitionId", options_.partition_id)
            .put("Status", status);
        return status;
    }
    tracer.AddEvent("SetupReplicator");

    stage_ = PartitionLoadStage::LOADED;
    load_success_->get()->Increment();

    const LimitConfig& limit_config = options_.config.limit_config();
    cmd_executor_->UpdateLimitConfig(limit_config);

    LOG_INFO("Partition Load Success")
        .put("PartitionId", options_.partition_id)
        .put("TimeTrace", tracer);

    status = cmd_executor_->LoadModuleCustomConfig(options_.config);
    BYTE_ASSERT(status.ok());
    return Status::OK();
}

Status Partition::Unload() {
    LOG_CALL_INFO().put("PartitionId", options_.partition_id);
    if (!IsCoContext()) {
        LOG_WARNING("Partition unload running outside coroutine context")
            .put("PartitionId", options_.partition_id);
    }

    if (stage_ != PartitionLoadStage::LOADED && stage_ != PartitionLoadStage::FAILED) {
        return Status::FailedPrecondition("Invalid load stage " + PartitionLoadStage_Name(stage_));
    }

    ScopedLatency latency(unload_latency_->get());
    stage_ = PartitionLoadStage::UNLOADING;
    LOG_INFO("Partition start unload").put("PartitionId", options_.partition_id);

    while (inflight_io_count_ != 0) {
        LOG_INFO("Partition waiting for inflight io")
            .put("PartitionId", options_.partition_id)
            .put("InflightCount", inflight_io_count_);
        CoSleep(1 * 1000 * 1000);  // 1s
    }

    if (storage_manager_) {
        storage_manager_->Stop();
        LOG_INFO("Partition storage manager stopped").put("PartitionId", options_.partition_id);
    }

    if (data_raft_consensus_) {
        data_raft_consensus_->Stop();
        LOG_INFO("Partition data raft consensus backend stopped")
            .put("PartitionId", options_.partition_id);
    }

    if (replicator_) {
        replicator_->Stop();
        LOG_INFO("Partition replicator stopped").put("PartitionId", options_.partition_id);
    }

    if (op_logger_) {
        SYNC_CALL0(op_logger_->Close);
        LOG_INFO("Partition oplogger stopped").put("PartitionId", options_.partition_id);
    }

    if (page_store_) {
        page_store_->Close();
        LOG_INFO("Partition page store stopped").put("PartitionId", options_.partition_id);
    }

    if (index_) {
        SYNC_CALL0(index_->Close);
        LOG_INFO("Partition index stopped").put("PartitionId", options_.partition_id);
    }

    stage_ = PartitionLoadStage::UNLOADED;
    unload_success_->get()->Increment();
    LOG_INFO("Partition unloaded").put("PartitionId", options_.partition_id);
    return Status::OK();
}

Status Partition::SetupCondition() {
    std::string condition_uri = options_.uri + "-condition";
    Controller ctrl;
    stream::Env::ConditionData condition_data;
    options_.env->GetCondition(&ctrl, condition_uri, &condition_data);
    if (!ctrl.status().ok() && !ctrl.status().IsStoreNotFound()) {
        LOG_ERROR("Get condition data failed")
            .put("PartitionId", options_.partition_id)
            .put("Uri", options_.uri)
            .put("LockUri", condition_uri)  // yes, this is in essence just a lock
            .put("Error", ctrl.status().ToString());
        return ctrl.status();
    }

    stream::Env::Condition old_condition;
    if (ctrl.status().ok()) {
        old_condition.name = condition_uri;
        old_condition.data = condition_data;
    } else if (readonly_ && !IsDataRaftConsensusMode()) {
        LOG_WARNING("Missing condition info").put("PartitionId", options_.partition_id);
        return Status::FailedPrecondition("Missing condition info");
    }

    ConditionInfoObserver condition_os(old_condition.data.data());
    if (!readonly_             // RW partition
        && ctrl.status().ok()  // found Condition
        && options_.load_version <= condition_os.LoadVersion()) {
        LOG_ERROR("Load version should be larger")
            .put("PartitionId", options_.partition_id)
            .put("Uri", options_.uri)
            .put("OldVersion", condition_os.LoadVersion())
            .put("NewVersion", options_.load_version);
        return Status::InvalidArgument("Load version should be larger");
    }

    if (readonly_ && !IsDataRaftConsensusMode() && UsePrimaryPullStreams()) {
        // no need set condition for RO partition
        LOG_INFO("Load condition success")
            .put("PartitionId", options_.partition_id)
            .put("Uri", options_.uri)
            .put("Condition", ConditionInfoObserver(old_condition.data.data()));
        condition_ = std::move(old_condition);
        return Status::OK();
    }

    ConditionInfoV1::Structure condition_info;
    condition_info.version = ConditionVersion::kV1;
    condition_info.partition_id = options_.partition_id;
    condition_info.load_version = options_.load_version;
    if (!options_.host_v6.empty()) {
        condition_info.remote_family = AddressFamily::kIpv6;
        if (inet_pton(AF_INET6, options_.host_v6.c_str(), condition_info.remote_ip) != 1) {
            LOG_ERROR("Invalid IPv6 host")
                .put("PartitionId", options_.partition_id)
                .put("HostV6", options_.host_v6);
            return Status::InvalidArgument("Invalid IPv6 host");
        }
    } else {
        condition_info.remote_family = AddressFamily::kIpv4;
        if (inet_pton(AF_INET, options_.host.c_str(), condition_info.remote_ip) != 1) {
            LOG_ERROR("Invalid IPv4 host")
                .put("PartitionId", options_.partition_id)
                .put("Host", options_.host);
            return Status::InvalidArgument("Invalid IPv4 host");
        }
    }
    condition_info.remote_port = options_.port;

    condition_.name = condition_uri;
    memcpy(condition_.data.data(), &condition_info, condition_.data.size());

    ctrl.Reset();
    // update the condition with this partition's new condition, i.e., to acquire the lock
    // to avoid race conditions, old_condition is passed
    options_.env->SetCondition(&ctrl, old_condition, condition_uri, condition_.data);
    if (!ctrl.status().ok()) {
        LOG_ERROR("Set condition failed")
            .put("PartitionId", options_.partition_id)
            .put("Uri", options_.uri)
            .put("LockUri", condition_uri)
            .put("Error", ctrl.status().ToString());
        return ctrl.status();
    }

    LOG_INFO("Setup condition success")
        .put("PartitionId", options_.partition_id)
        .put("Uri", options_.uri)
        .put("OldCondition", ConditionInfoObserver(old_condition.data.data()))
        .put("NewCondition", ConditionInfoObserver(condition_.data.data()));
    return Status::OK();
}

// for read-only replicas to connect to the primary replica
Status Partition::SetupRemoteInfo(PartitionInfo* remote_info) {
    if (!readonly_ || IsDataRaftConsensusMode() || !UsePrimaryPullStreams()) {
        // RW partitions, data-Raft replicas, and shared-store replicas own their stream state.
        // Remote info is only needed when a readonly replica pulls stream payloads from primary.
        return Status::OK();
    }

    if (!IsCoContext()) {
        LOG_WARNING("SetupRemoteInfo running outside coroutine context")
            .put("PartitionId", options_.partition_id);
    }

    ConditionInfoObserver condition_info(condition_.data.data());
    brpc::Channel channel;
    if (channel.Init(condition_info.RemoteIpStr().c_str(), condition_info.RemotePort(), nullptr) !=
        0) {
        LOG_ERROR("Invalid remote addr")
            .put("PartitionId", options_.partition_id)
            .put("RemoteIpStr", condition_info.RemoteIpStr())
            .put("RemotePort", condition_info.RemotePort())
            .put("ConditionInfo", condition_info.ToString());
        return Status::InvalidArgument("Invalid remote addr");
    }

    // TODO(wangtai.10): custom timeout
    brpc::Controller ctrl;
    ctrl.set_timeout_ms(FLAGS_remote_partition_info_timeout_ms);
    bcache2::ServerService_Stub stub(&channel);
    GetInfoRequest request;
    GetInfoResponse response;
    request.mutable_opt()->set_trace_id(butil::fast_rand());
    request.set_partition_id(GetPrimaryPartitionId());

    CoSyncClosure sync;
    stub.GetInfo(&ctrl, &request, &response, brpc::NewCallback(&OnBrpcDone, &sync));
    sync.Wait();

    if (ctrl.Failed()) {
        LOG_ERROR("Failed to send rpc call")
            .put("PartitionId", options_.partition_id)
            .put("ErrorText", ctrl.ErrorText());
        return Status::DeadlineExceeded(ctrl.ErrorText());
    }

    if (response.status().code() != kOK) {
        LOG_ERROR("Failed to get remote partition info")
            .put("PartitionId", options_.partition_id)
            .put("Status", response.status().message());
        return Status::FromRpcStatus(response.status());
    }

    *remote_info = std::move(*response.mutable_partition_info());
    LOG_INFO("Setup remote info success")
        .put("PartitionId", options_.partition_id)
        .put("RemoteInfo", remote_info->ShortDebugString());
    return Status::OK();
}

Status Partition::SetupIndex(const PartitionInfo& remote_info) {
    TimeTracer tracer;

    std::unique_ptr<stream::Stream> stream;
    std::string index_stream_uri = options_.uri + "-index/";
    Status status = LoadStream(index_stream_uri, PARTITION_STREAM_INDEX, 0, &stream);
    if (!status.ok()) {
        LOG_ERROR("LoadStream index stream fail")
            .put("PartitionId", options_.partition_id)
            .put("Uri", index_stream_uri)
            .put("Status", status);
        return status;
    }
    tracer.AddEvent("LoadStream");

    Index::Options opts;
    opts.stream = stream.release();
    index_->Init(std::move(opts));
    tracer.AddEvent("Init");

    if (readonly_ && !IsDataRaftConsensusMode() && UsePrimaryPullStreams()) {
        status = index_->RestoreInfo(remote_info.index_info());
        if (!status.ok()) {
            LOG_ERROR("Failed restore index info")
                .put("PartitionId", options_.partition_id)
                .put("Status", status)
                .put("IndexInfo", remote_info.index_info().ShortDebugString());
            return status;
        }
    }
    status = index_->Load();
    if (!status.ok()) {
        LOG_ERROR("Failed load index")
            .put("PartitionId", options_.partition_id)
            .put("Status", status.ToString());
        return status;
    }
    tracer.AddEvent("Load");

    LOG_INFO("Setup index success")
        .put("PartitionId", options_.partition_id)
        .put("Uri", index_stream_uri)
        .put("Time", tracer);
    return Status::OK();
}

Status Partition::SetupOplogger(const PartitionInfo& remote_info) {
    std::unique_ptr<stream::Stream> stream;
    std::string oplog_stream_uri = options_.uri + "-oplog/";
    Status status = LoadStream(oplog_stream_uri, PARTITION_STREAM_OPLOG, 0, &stream);
    if (!status.ok()) {
        LOG_ERROR("LoadStream fail")
            .put("PartitionId", options_.partition_id)
            .put("Status", status.ToString());
        return status;
    }

    OpLogger::Options opts;
    opts.stream = stream.release();
    op_logger_->Init(std::move(opts));

    if (readonly_ && !IsDataRaftConsensusMode() && UsePrimaryPullStreams()) {
        status = op_logger_->RestoreInfo(remote_info.op_logger_info());
        if (!status.ok()) {
            LOG_ERROR("Failed restore oplogger info")
                .put("PartitionId", options_.partition_id)
                .put("OploggerInfo", remote_info.op_logger_info().ShortDebugString())
                .put("Status", status);
            return status;
        }
    }

    return Status::OK();
}

Status Partition::SetupObjectManager() {
    Status status = object_manager_->Load();
    if (!status.ok()) {
        LOG_ERROR("Failed to load oplog")
            .put("PartitionId", options_.partition_id)
            .put("Status", status.ToString());
        return status;
    }
    LOG_INFO("Setup object manager success").put("PartitionId", options_.partition_id);
    return Status::OK();
}

Status Partition::SetupPageStore(const PartitionInfo& remote_info) {
    PageStore::Options opts;
    opts.condition = condition_;
    opts.stream_uri_pattern = options_.uri + "-page%d/";
    page_store_->Init(std::move(opts));

    Status status = page_store_->UpdateZones();
    if (!status.ok()) {
        LOG_ERROR("UpdateZones fail")
            .put("PartitionId", options_.partition_id)
            .put("Status", status.ToString());
        return status;
    }

    if (readonly_ && !IsDataRaftConsensusMode()) {
        status = page_store_->RestoreInfo(remote_info.page_store_info());
        if (!status.ok()) {
            LOG_ERROR("Failed restore page store info")
                .put("PartitionId", options_.partition_id)
                .put("Status", status.ToString())
                .put("PageStoreInfo", remote_info.page_store_info().ShortDebugString());
            return status;
        }
    }

    LOG_INFO("Setup page store success").put("PartitionId", options_.partition_id);
    return Status::OK();
}

Status Partition::SetupStorageManager() {
    if (readonly_) {
        options_.config.mutable_evicter_config()->mutable_operation_type()->set_value(
            OperationType::DUMP);
    }
    evicter_->Init(options_.config.evicter_config());

    StorageManager::Options opts;
    if (readonly_) {
        opts.enable_prepare = false;
        opts.enable_oplog_rolling = false;
        opts.enable_evict = true;
        opts.enable_expire = false;
        opts.enable_page_gc = false;
        opts.enable_index_gc = false;
        opts.enable_page_compaction = false;
    }
    storage_manager_->Init(std::move(opts));

    if (FLAGS_start_storage_manager_when_loading) {
        storage_manager_->Start();
    }

    LOG_INFO("Setup storage manager success").put("PartitionId", options_.partition_id);
    return Status::OK();
}

Status Partition::SetupReplicator() {
    if (IsDataRaftConsensusMode()) {
        Status applied_status = LoadDataRaftAppliedIndex();
        if (!applied_status.ok()) {
            return applied_status;
        }

        DataRaftConsensusOptions options;
        options.partition_id = options_.partition_id;
        options.replica_id = options_.partition_id;
        options.group_id = GetDataRaftGroupPartitionId();
        Status status = DataRaftHostPort(options_.host, options_.port,
                                         FLAGS_data_raft_raft_port_delta,
                                         "raft", &options.raft_addr);
        if (!status.ok()) {
            return status;
        }
        status = DataRaftHostPort(options_.host, options_.port,
                                  FLAGS_data_raft_snapshot_port_delta,
                                  "snapshot", &options.snapshot_addr);
        if (!status.ok()) {
            return status;
        }
        options.wal_dir = DataRaftWalDirForPartition(options_.partition_id);
        options.snapshot_dir = DataRaftSnapshotDirForPartition(options_.partition_id);
        options.wal_sync = true;
        options.initial_applied_index = data_raft_applied_index_.load();
        options.bootstrap_as_learner = options_.partition_id != GetPrimaryPartitionId();
        options.snapshot_func = [this](const std::string& path, uint64_t* applied_index) {
            return CreateDataRaftSnapshot(path, applied_index);
        };
        options.load_snapshot_func = [this](const std::string& path) {
            return LoadDataRaftSnapshot(path);
        };

        options.peers = BuildDataRaftPeersForPartition(membership_info_, options_.partition_id);
        if (options.bootstrap_as_learner) {
            // Non-primary replicas must not bootstrap their own full voter set. They start as
            // incomplete learner nodes and let the Raft leader install membership/log state.
            options.peers.clear();
        }
        bool has_self_peer = false;
        for (const auto& peer : options.peers) {
            if (peer.replica_id == options_.partition_id) {
                has_self_peer = true;
                break;
            }
        }
        if (!has_self_peer && !options.bootstrap_as_learner) {
            DataRaftPeer self;
            self.replica_id = options_.partition_id;
            self.raft_addr = options.raft_addr;
            self.snapshot_addr = options.snapshot_addr;
            options.peers.push_back(std::move(self));
        }

        data_raft_consensus_ = NewByteraftDataRaftConsensusBackend(
            options, [this](uint64_t raft_index, const std::string& data) {
                uint64_t applied_raft_index = 0;
                uint64_t applied_oplog_sequence = 0;
                return ApplyDataRaftEntry(raft_index, data, &applied_raft_index,
                                          &applied_oplog_sequence);
            });
        status = data_raft_consensus_->Start();
        if (!status.ok()) {
            return status;
        }
        if (options_.partition_id == GetPrimaryPartitionId()) {
            std::thread([this] {
                for (int attempt = 1; attempt <= 50; ++attempt) {
                    if (stage_ != PartitionLoadStage::LOADED) {
                        return;
                    }
                    if (data_raft_consensus_ == nullptr ||
                        !data_raft_consensus_->IsLeader()) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(100));
                        continue;
                    }
                    Status status = UpdateMembership(membership_info_);
                    if (!status.ok() && (attempt == 1 || attempt == 50 || attempt % 10 == 0)) {
                        LOG_WARNING("Delayed data raft membership reconciliation failed")
                            .put("PartitionId", options_.partition_id)
                            .put("Attempt", attempt)
                            .put("Status", status);
                    }
                    return;
                }
            }).detach();
        }
        return Status::OK();
    }

    if (readonly_) {  // read-only followers
        LOG_INFO("Start replicator").put("PartitionId", options_.partition_id);
        replicator_->Start();
    }
    return Status::OK();
}

std::string Partition::DataRaftAppliedIndexPath() const {
    return DataRaftAppliedIndexPathForPartition(options_.partition_id);
}

Status Partition::LoadDataRaftAppliedIndex() {
    const std::string path = DataRaftAppliedIndexPath();
    if (!fs::exists(path)) {
        Status guard_status = GuardMissingDataRaftAppliedIndex(options_.partition_id, path);
        if (!guard_status.ok()) {
            return guard_status;
        }
        data_raft_applied_index_ = 0;
        return Status::OK();
    }

    std::string content;
    {
        std::ifstream in(path);
        if (!in.good()) {
            return Status::Internal("read data raft applied index file failed: " + path);
        }
        content.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    }
    if (content.empty()) {
        return Status::Internal("read data raft applied index file failed: " + path);
    }
    char* end = nullptr;
    const uint64_t index = std::strtoull(content.c_str(), &end, 10);
    if (end == content.c_str()) {
        return Status::DataLoss("invalid data raft applied index file: " + path);
    }
    data_raft_applied_index_ = index;
    return Status::OK();
}

Status Partition::PersistDataRaftAppliedIndex(uint64_t raft_index) {
    const uint64_t current = data_raft_applied_index_.load();
    if (raft_index <= current) {
        return Status::OK();
    }

    const std::string path = DataRaftAppliedIndexPath();
    const std::string content = std::to_string(raft_index) + "\n";
    Status status = WriteFileDurably(path, content);
    if (!status.ok()) {
        return status;
    }
    data_raft_applied_index_ = raft_index;
    return Status::OK();
}

Status Partition::CreateDataRaftSnapshot(const std::string& path, uint64_t* applied_index) {
    if (options_.owning_thread != nullptr && byte::GetCurrentThread() != options_.owning_thread) {
        struct SnapshotOwnerState {
            std::mutex mu;
            std::condition_variable cv;
            bool done = false;
            Status result;
            uint64_t owner_applied_index = 0;
        };
        auto state = std::make_shared<SnapshotOwnerState>();
        std::string snapshot_path = path;
        LOG_INFO("Dispatching data raft snapshot export to partition owner thread")
            .put("PartitionId", options_.partition_id)
            .put("Path", path)
            .put("TimeoutMs", FLAGS_data_raft_partition_snapshot_owner_wait_timeout_ms);
        options_.owning_thread->Invoke(NewCoFuncClosure([this, snapshot_path, state] {
            Status result = CreateDataRaftSnapshot(snapshot_path, &state->owner_applied_index);
            {
                std::lock_guard<std::mutex> lock(state->mu);
                state->result = result;
                state->done = true;
            }
            state->cv.notify_one();
        }));
        std::unique_lock<std::mutex> lock(state->mu);
        const bool completed = state->cv.wait_for(
            lock,
            std::chrono::milliseconds(FLAGS_data_raft_partition_snapshot_owner_wait_timeout_ms),
            [&state] { return state->done; });
        if (!completed) {
            return Status::DeadlineExceeded("data raft partition snapshot owner thread timeout");
        }
        if (applied_index != nullptr) {
            *applied_index = state->owner_applied_index;
        }
        return state->result;
    }

    LOG_INFO("Data raft partition snapshot export started")
        .put("PartitionId", options_.partition_id)
        .put("Path", path)
        .put("AppliedIndex", data_raft_applied_index_.load());
    std::lock_guard<std::mutex> lock(data_raft_snapshot_mu_);
    std::string base_path;
    if (!LocalFilesystemUriToPath(options_.uri, &base_path)) {
        return Status::Unimplemented(
            "data raft snapshot currently supports local filesystem partition storage "
            "(file://, shared-file://, shared://, efs://, nfs://). Use local-streams for "
            "raft_consensus data, or add an object-store snapshot exporter for s3://.");
    }
    LOG_INFO("Data raft partition snapshot resolved local path")
        .put("PartitionId", options_.partition_id)
        .put("BasePath", base_path);

    if (op_logger_) {
        LOG_INFO("Data raft partition snapshot committing oplog")
            .put("PartitionId", options_.partition_id);
        Controller ctrl;
        SYNC_CALL(op_logger_->Commit, &ctrl);
        if (!ctrl.status().ok() && !ctrl.status().IsStreamAbort()) {
            return ctrl.status();
        }
        LOG_INFO("Data raft partition snapshot committed oplog")
            .put("PartitionId", options_.partition_id)
            .put("Status", ctrl.status());
    }
    if (index_ && FLAGS_data_raft_snapshot_commit_index_before_copy) {
        LOG_INFO("Data raft partition snapshot committing index")
            .put("PartitionId", options_.partition_id);
        Controller ctrl;
        SYNC_CALL(index_->Commit, &ctrl);
        if (!ctrl.status().ok() && !ctrl.status().IsStreamAbort()) {
            return ctrl.status();
        }
        LOG_INFO("Data raft partition snapshot committed index")
            .put("PartitionId", options_.partition_id)
            .put("Status", ctrl.status());
    } else if (index_) {
        LOG_WARNING("Data raft partition snapshot skipped synchronous index commit")
            .put("PartitionId", options_.partition_id)
            .put("Reason", "index commit can re-enter the partition owner thread");
    }

    const uint64_t snapshot_index = data_raft_applied_index_.load();
    const fs::path snapshot_dir(path);
    const fs::path files_dir = snapshot_dir / "partition_files";
    std::error_code ec;
    fs::remove_all(files_dir, ec);
    ec.clear();
    fs::create_directories(files_dir, ec);
    if (ec) {
        return Status::Internal("create data raft snapshot dir failed: " + ec.message());
    }

    const fs::path base(base_path);
    const fs::path parent = base.parent_path().empty() ? fs::path(".") : base.parent_path();
    for (const fs::path& src : PartitionLocalFilePaths(base_path)) {
        LOG_INFO("Data raft partition snapshot copying path")
            .put("PartitionId", options_.partition_id)
            .put("Source", src.string());
        const fs::path rel = src.lexically_relative(parent);
        Status status = CopyPathForDataRaftSnapshot(src, files_dir / rel);
        if (!status.ok()) {
            return status;
        }
    }

    Status status = WriteDataRaftSnapshotManifest(snapshot_dir, options_.partition_id,
                                                  options_.uri, snapshot_index);
    if (!status.ok()) {
        return status;
    }

    if (applied_index != nullptr) {
        *applied_index = snapshot_index;
    }
    LOG_INFO("Created data raft partition snapshot")
        .put("PartitionId", options_.partition_id)
        .put("Path", path)
        .put("AppliedIndex", snapshot_index);
    return Status::OK();
}

Status Partition::LoadDataRaftSnapshot(const std::string& path) {
    if (options_.owning_thread != nullptr && byte::GetCurrentThread() != options_.owning_thread) {
        std::mutex mu;
        std::condition_variable cv;
        bool done = false;
        Status result;
        options_.owning_thread->Invoke(NewCoFuncClosure([this, &path, &mu, &cv, &done, &result] {
            result = LoadDataRaftSnapshot(path);
            {
                std::lock_guard<std::mutex> lock(mu);
                done = true;
            }
            cv.notify_one();
        }));
        std::unique_lock<std::mutex> lock(mu);
        cv.wait(lock, [&done] { return done; });
        return result;
    }

    std::lock_guard<std::mutex> lock(data_raft_snapshot_mu_);
    std::string base_path;
    if (!LocalFilesystemUriToPath(options_.uri, &base_path)) {
        return Status::Unimplemented(
            "data raft snapshot install currently supports local filesystem partition storage "
            "(file://, shared-file://, shared://, efs://, nfs://). Use local-streams for "
            "raft_consensus data, or add an object-store snapshot importer for s3://.");
    }

    const fs::path snapshot_dir(path);
    uint64_t snapshot_index = 0;
    Status status = ReadDataRaftSnapshotManifest(snapshot_dir, options_.partition_id,
                                                 &snapshot_index);
    if (!status.ok()) {
        return status;
    }

    if (storage_manager_) {
        storage_manager_->Stop();
    }
    if (op_logger_) {
        SYNC_CALL0(op_logger_->Close);
    }
    if (page_store_) {
        page_store_->Close();
    }
    if (index_) {
        SYNC_CALL0(index_->Close);
    }

    const fs::path base(base_path);
    const fs::path parent = base.parent_path().empty() ? fs::path(".") : base.parent_path();
    const fs::path files_dir = snapshot_dir / "partition_files";
    if (!fs::exists(files_dir)) {
        return Status::DataLoss("missing data raft partition snapshot files");
    }
    for (const fs::path& dst : PartitionLocalFilePaths(base_path)) {
        const fs::path rel = dst.lexically_relative(parent);
        Status status = AtomicReplacePathFromDataRaftSnapshot(files_dir / rel, dst);
        if (!status.ok()) {
            return status;
        }
    }

    status = PersistDataRaftAppliedIndex(snapshot_index);
    if (!status.ok()) {
        return status;
    }
    status = RebuildLocalStateAfterDataRaftSnapshot();
    if (!status.ok()) {
        return status;
    }
    LOG_INFO("Loaded data raft partition snapshot")
        .put("PartitionId", options_.partition_id)
        .put("Path", path)
        .put("AppliedIndex", snapshot_index);
    return Status::OK();
}

Status Partition::RebuildLocalStateAfterDataRaftSnapshot() {
    allocator_manager_.reset(new AllocatorManager(metrics_manager_.get()));
    slot_context_manager_.reset(
        new SlotContextManager(metrics_manager_.get(), allocator_manager_.get()));
    cmd_metrics_.reset(new RequestMetrics(metrics_manager_.get(), "cmd", {}));
    index_.reset(new Index(this, allocator_manager_.get(), slot_context_manager_.get(),
                           metrics_manager_.get()));
    op_logger_.reset(
        new OpLogger(this, index_.get(), slot_context_manager_.get(), metrics_manager_.get()));
    page_store_.reset(new PageStore(this, index_.get(), options_.env, metrics_manager_.get(),
                                    options_.config.stream_config().store_rep_policy(),
                                    options_.partition_id, blockcache_));
    slot_store_.reset(new SlotStore(this, index_.get(), page_store_.get(), op_logger_.get(),
                                    slot_context_manager_.get(), metrics_manager_.get()));
    object_manager_.reset(new ObjectManager(this, index_.get(), page_store_.get(),
                                            op_logger_.get(), slot_store_.get(),
                                            slot_context_manager_.get(), allocator_manager_.get()));
    evicter_.reset(new Evicter(this, object_manager_.get(), index_.get(), op_logger_.get(),
                               slot_store_.get(), allocator_manager_.get(), metrics_manager_.get()));
    page_gc_.reset(new PageGc(this, options_.env, index_.get(), page_store_.get(),
                              slot_store_.get(), op_logger_.get(), metrics_manager_.get()));
    page_compactor_.reset(
        new PageCompactor(this, index_.get(), slot_store_.get(), metrics_manager_.get()));
    expirer_.reset(new Expirer(index_.get(), object_manager_.get(), op_logger_.get(),
                               metrics_manager_.get()));
    cmd_executor_.reset(new CmdExecutor(this, object_manager_.get(), op_logger_.get(),
                                        metrics_manager_.get()));
    storage_manager_.reset(new StorageManager(
        this, cmd_executor_.get(), index_.get(), page_store_.get(), op_logger_.get(),
        slot_store_.get(), evicter_.get(), expirer_.get(), object_manager_.get(), page_gc_.get(),
        page_compactor_.get(), slot_context_manager_.get(), metrics_manager_.get(),
        allocator_manager_.get()));
    cmd_.reset(new CmdExecutorManager(object_manager_.get(), metrics_manager_.get()));
    slot_store_->SetObjectManager(object_manager_.get());

    PartitionInfo empty_remote_info;
    Status status = SetupIndex(empty_remote_info);
    if (!status.ok()) {
        return status;
    }
    status = SetupOplogger(empty_remote_info);
    if (!status.ok()) {
        return status;
    }
    status = SetupPageStore(empty_remote_info);
    if (!status.ok()) {
        return status;
    }
    status = SetupObjectManager();
    if (!status.ok()) {
        return status;
    }
    return SetupStorageManager();
}

Status Partition::OpenPartitionStream(const std::string& uri, PartitionStreamKind stream_kind,
                                      uint32_t zone_id, bool created, stream::Stream** stream) {
    LOG_INFO("LoadStream").put("PartitionId", options_.partition_id).put("Uri", uri);

    if (readonly_ && UsePrimaryPullStreams()) {
        RemotePartitionStream::Options remote_options;
        remote_options.partition = this;
        remote_options.stream_kind = stream_kind;
        remote_options.zone_id = zone_id;
        std::unique_ptr<stream::Stream> remote_stream(new RemotePartitionStream(remote_options));
        Status status = remote_stream->Load();
        if (!status.ok()) {
            return status;
        }
        *stream = remote_stream.release();
        LOG_INFO("Open remote primary-backed stream success")
            .put("PartitionId", options_.partition_id)
            .put("PrimaryPartitionId", GetPrimaryPartitionId())
            .put("StreamKind", stream_kind)
            .put("ZoneId", zone_id)
            .put("Uri", uri);
        return Status::OK();
    }

    if (readonly_) {
        LOG_INFO("Open readonly stream from Env")
            .put("PartitionId", options_.partition_id)
            .put("PrimaryPartitionId", GetPrimaryPartitionId())
            .put("StreamKind", stream_kind)
            .put("ZoneId", zone_id)
            .put("Uri", uri);
    }

    Controller ctrl;
    stream::Env::OpenOptions options;
    options.created = created;
    options.metrics_manager = metrics_manager_.get();
    options.readonly = readonly_ && !IsDataRaftConsensusMode();
    options.rep_policy = options_.config.stream_config().store_rep_policy();
    options_.env->OpenStream(&ctrl, condition_, uri, options, stream);
    if (!ctrl.status().ok()) {
        return ctrl.status();
    }
    return Status::OK();
}

Status Partition::LoadStream(const std::string& uri, PartitionStreamKind stream_kind,
                             uint32_t zone_id,
                             std::unique_ptr<stream::Stream>* stream_ptr) {
    stream::Stream* stream = nullptr;
    Status status = OpenPartitionStream(uri, stream_kind, zone_id, true, &stream);
    if (!status.ok()) {
        return status;
    }
    stream_ptr->reset(stream);
    return Status::OK();
}

void Partition::ReadPartitionStream(Controller* ctrl, const ReadPartitionStreamRequest* request,
                                    ReadPartitionStreamResponse* response,
                                    Closure<void>* callback) {
    if (request->size() > std::numeric_limits<size_t>::max()) {
        ctrl->set_status(Status::InvalidArgument("Partition stream read size too large"));
        callback->Run();
        return;
    }
    const size_t read_size = static_cast<size_t>(request->size());
    std::string* data = response->mutable_data();
    data->resize(read_size);
    void* buffer = data->empty() ? nullptr : &(*data)[0];

    switch (request->stream_kind()) {
    case PARTITION_STREAM_INDEX:
        index_->ReadRawStream(ctrl, request->offset(), buffer, read_size, callback);
        return;
    case PARTITION_STREAM_OPLOG:
        op_logger_->ReadRawStream(ctrl, request->offset(), buffer, read_size, callback);
        return;
    case PARTITION_STREAM_PAGE:
        page_store_->ReadRawStream(request->zone_id(), ctrl, request->offset(), buffer,
                                   read_size, callback);
        return;
    default:
        ctrl->set_status(Status::InvalidArgument("Unknown partition stream kind"));
        callback->Run();
        return;
    }
}

void Partition::ScanPartitionStream(Controller* ctrl, const ScanPartitionStreamRequest* request,
                                    ScanPartitionStreamResponse* response,
                                    Closure<void>* callback) {
    ScopedInvoker done(callback);

    stream::ScopedIterator iter;
    switch (request->stream_kind()) {
    case PARTITION_STREAM_INDEX:
        iter = index_->NewRawStreamIterator(request->start_offset(), request->end_offset());
        break;
    case PARTITION_STREAM_OPLOG:
        iter = op_logger_->NewRawStreamIterator(request->start_offset(), request->end_offset());
        break;
    case PARTITION_STREAM_PAGE:
        iter = page_store_->NewRawStreamIterator(request->zone_id(), request->start_offset(),
                                                 request->end_offset());
        break;
    default:
        ctrl->set_status(Status::InvalidArgument("Unknown partition stream kind"));
        return;
    }

    if (iter == nullptr) {
        ctrl->set_status(Status::NotFound("Partition stream not found"));
        return;
    }

    const uint32_t max_records = request->max_records() > 0 ? request->max_records() : 1;
    const uint64_t max_bytes =
        request->max_bytes() > 0 ? request->max_bytes() : std::numeric_limits<uint64_t>::max();
    uint64_t returned_bytes = 0;

    for (uint32_t i = 0; i < max_records; ++i) {
        Status status = iter->Next();
        if (status.IsOutOfRange()) {
            response->set_end_of_stream(true);
            ctrl->set_status(Status::OK());
            return;
        }
        if (!status.ok()) {
            ctrl->set_status(status);
            return;
        }

        const absl::string_view record = iter->Data();
        if (response->records_size() > 0 && returned_bytes + record.size() > max_bytes) {
            ctrl->set_status(Status::OK());
            return;
        }

        PartitionStreamRecord* out = response->add_records();
        out->set_offset(iter->Id());
        out->set_data(record.data(), record.size());
        returned_bytes += record.size();
    }

    ctrl->set_status(Status::OK());
}

Status Partition::ApplyDataRaftLog(uint64_t raft_index, const std::string& committed_log,
                                   uint64_t* applied_raft_index,
                                   uint64_t* applied_oplog_sequence) {
    if (!IsDataRaftConsensusMode()) {
        return Status::FailedPrecondition(
            "data raft apply requires data_replication_mode=raft_consensus");
    }
    if (!IsLoaded()) {
        return Status::FailedPrecondition("Partition not loaded");
    }

    DataRaftCommittedLogApplier applier(options_.partition_id, object_manager_.get(),
                                        op_logger_.get());
    Status status = applier.Apply(raft_index, committed_log);
    if (!status.ok()) {
        return status;
    }
    if (applied_raft_index != nullptr) {
        *applied_raft_index = applier.AppliedRaftIndex();
    }
    if (applied_oplog_sequence != nullptr) {
        *applied_oplog_sequence = applier.AppliedOplogSequence();
    }
    return Status::OK();
}

Status Partition::ApplyDataRaftCommand(uint64_t raft_index, const std::string& committed_command) {
    if (!IsDataRaftConsensusMode()) {
        return Status::FailedPrecondition(
            "data raft command apply requires data_replication_mode=raft_consensus");
    }
    if (!IsLoaded()) {
        return Status::FailedPrecondition("Partition not loaded");
    }

    DataRaftCommandEntry entry;
    Status status = ParseDataRaftCommand(committed_command, &entry);
    if (!status.ok()) {
        return status;
    }
    const uint64_t raft_group_partition_id = GetDataRaftGroupPartitionId();
    if (entry.partition_id != raft_group_partition_id) {
        return Status::InvalidArgument("committed command belongs to another partition");
    }
    if (entry.raft_index != 0 && raft_index != 0 && entry.raft_index != raft_index) {
        return Status::InvalidArgument("raft command index mismatch");
    }
    if (entry.request.partition_id() != raft_group_partition_id) {
        return Status::InvalidArgument("committed command request partition id mismatch");
    }

    BatchExecuteCmdResponse applied_response;
    Status apply_status = Status::OK();
    BatchExecuteCmdRequest local_request = entry.request;
    local_request.set_partition_id(options_.partition_id);

    for (int i = 0; i < local_request.request_size(); ++i) {
        const CmdRequest& cmd_request = local_request.request(i);
        Controller ctrl;
        ctrl.set_trace_id(entry.request.opt().trace_id());
        CmdResponse* cmd_response = applied_response.add_response();
        CoSyncClosure sync;

        if (cmd_request.module_id() == 0) {
            ExecuteCmd(&ctrl, &cmd_request, cmd_response, &sync);
            sync.Wait();
            if (!ctrl.status().ok()) {
                cmd_response->mutable_status()->CopyFrom(ctrl.status().ToRpcStatus());
                apply_status = ctrl.status();
                break;
            }
        } else {
            const CmdManager::CmdInfo* cmd =
                CmdManager::GetCmd(cmd_request.module_id(), cmd_request.function_id());
            if (cmd == nullptr) {
                apply_status = Status::Unimplemented("committed command not implemented");
                cmd_response->mutable_status()->CopyFrom(apply_status.ToRpcStatus());
                break;
            }
            std::unique_ptr<google::protobuf::Message> request(cmd->request_builder());
            std::unique_ptr<google::protobuf::Message> response(cmd->response_builder());
            if (!request->ParseFromString(cmd_request.request_bytes())) {
                apply_status = Status::DataLoss("parse committed command request bytes failed");
                cmd_response->mutable_status()->CopyFrom(apply_status.ToRpcStatus());
                break;
            }
            Status response_status;
            ExecuteCmd(&ctrl, cmd_request.module_id(), cmd_request.function_id(), request.get(),
                       response.get(), &response_status, &sync);
            sync.Wait();
            cmd_response->mutable_status()->CopyFrom(ctrl.status().ToRpcStatus());
            if (!ctrl.status().ok()) {
                apply_status = ctrl.status();
                break;
            }
            cmd_response->mutable_response_status()->CopyFrom(response_status.ToRpcStatus());
            if (!response->SerializeToString(cmd_response->mutable_response_bytes())) {
                apply_status = Status::Internal("Serialize committed response failed");
                cmd_response->mutable_status()->CopyFrom(apply_status.ToRpcStatus());
                break;
            }
        }
    }

    std::shared_ptr<PendingDataRaftApply> pending;
    {
        std::lock_guard<std::mutex> lock(data_raft_pending_mu_);
        auto it = data_raft_pending_.find(entry.request_id);
        if (it != data_raft_pending_.end()) {
            pending = it->second;
        }
    }
    if (pending != nullptr) {
        std::lock_guard<std::mutex> lock(data_raft_pending_mu_);
        pending->status = apply_status;
        pending->response = std::move(applied_response);
        pending->done = true;
        pending->cv.notify_all();
    }
    return apply_status;
}

Status Partition::ApplyDataRaftEntry(uint64_t raft_index, const std::string& committed_entry,
                                     uint64_t* applied_raft_index,
                                     uint64_t* applied_oplog_sequence) {
    if (options_.owning_thread == nullptr ||
        byte::GetCurrentThread() == options_.owning_thread) {
        return ApplyDataRaftEntryOnOwnerThread(raft_index, committed_entry,
                                               applied_raft_index, applied_oplog_sequence);
    }

    bthread::CountdownEvent done(1);
    Status result;
    options_.owning_thread->Invoke(NewCoFuncClosure([this, raft_index, &committed_entry,
                                                     applied_raft_index, applied_oplog_sequence,
                                                     &done, &result] {
        result = ApplyDataRaftEntryOnOwnerThread(raft_index, committed_entry,
                                                 applied_raft_index, applied_oplog_sequence);
        done.signal();
    }));
    done.wait();
    return result;
}

Status Partition::ApplyDataRaftEntryOnOwnerThread(uint64_t raft_index,
                                                  const std::string& committed_entry,
                                                  uint64_t* applied_raft_index,
                                                  uint64_t* applied_oplog_sequence) {
    std::lock_guard<std::mutex> snapshot_lock(data_raft_snapshot_mu_);
    const bool old_applying = data_raft_applying_;
    data_raft_applying_ = true;
    Status command_status = ApplyDataRaftCommand(raft_index, committed_entry);
    data_raft_applying_ = old_applying;
    if (command_status.ok()) {
        if (applied_raft_index != nullptr) {
            *applied_raft_index = raft_index;
        }
        if (applied_oplog_sequence != nullptr) {
            *applied_oplog_sequence = op_logger_->CurrentSequence();
        }
        return PersistDataRaftAppliedIndex(raft_index);
    }

    data_raft_applying_ = true;
    Status log_status =
        ApplyDataRaftLog(raft_index, committed_entry, applied_raft_index, applied_oplog_sequence);
    data_raft_applying_ = old_applying;
    if (log_status.ok()) {
        return PersistDataRaftAppliedIndex(raft_index);
    }
    return command_status;
}

Status Partition::ProposeDataRaftCommand(const BatchExecuteCmdRequest& request,
                                         uint64_t request_id, uint64_t* committed_index,
                                         BatchExecuteCmdResponse* response) {
    if (!IsDataRaftConsensusMode()) {
        return Status::FailedPrecondition(
            "data raft command propose requires data_replication_mode=raft_consensus");
    }
    if (data_raft_consensus_ == nullptr) {
        return Status::FailedPrecondition("data raft consensus backend is not initialized");
    }
    if (!data_raft_consensus_->IsLeader() && GetPrimaryPartitionId() == options_.partition_id) {
        bool should_campaign = false;
        {
            std::unique_lock<std::mutex> lock(data_raft_campaign_mu_);
            if (!data_raft_campaign_inflight_) {
                data_raft_campaign_inflight_ = true;
                should_campaign = true;
            } else {
                data_raft_campaign_cv_.wait_for(lock, std::chrono::milliseconds(1200), [this] {
                    return !data_raft_campaign_inflight_ || data_raft_consensus_->IsLeader();
                });
            }
        }

        if (should_campaign) {
            Status campaign_status = data_raft_consensus_->Campaign(1000, true);
            {
                std::lock_guard<std::mutex> lock(data_raft_campaign_mu_);
                data_raft_campaign_inflight_ = false;
            }
            data_raft_campaign_cv_.notify_all();

            if (campaign_status.ok() && data_raft_consensus_->IsLeader()) {
                LOG_INFO("Data raft metadata primary campaigned leadership for write")
                    .put("PartitionId", options_.partition_id)
                    .put("RequestId", request_id);
            } else {
                LOG_WARNING("Data raft metadata primary failed to campaign leadership for write")
                    .put("PartitionId", options_.partition_id)
                    .put("RequestId", request_id)
                    .put("Status", campaign_status);
            }
        }
    }
    if (!data_raft_consensus_->IsLeader()) {
        DataRaftStatus raft_status;
        Status status = data_raft_consensus_->GetStatus(&raft_status);
        LOG_WARNING("Data raft command rejected on non-leader partition")
            .put("PartitionId", options_.partition_id)
            .put("PrimaryPartitionId", GetPrimaryPartitionId())
            .put("RaftGroupPartitionId", GetDataRaftGroupPartitionId())
            .put("RequestId", request_id)
            .put("GetStatus", status)
            .put("Running", raft_status.running)
            .put("RoleLeader", raft_status.leader)
            .put("Learner", raft_status.learner)
            .put("Term", raft_status.term)
            .put("LeaderReplicaId", raft_status.leader_replica_id)
            .put("CommittedIndex", raft_status.committed_index)
            .put("AppliedIndex", raft_status.applied_index)
            .put("FirstIndex", raft_status.first_index)
            .put("LastIndex", raft_status.last_index)
            .put("VoterCount", raft_status.voter_count)
            .put("LearnerCount", raft_status.learner_count);
        return Status::FailedPrecondition("data raft partition is not leader");
    }
    if (request.partition_id() != options_.partition_id) {
        return Status::InvalidArgument("raft command request partition id mismatch");
    }

    LOG_INFO("Data raft command propose begin")
        .put("PartitionId", options_.partition_id)
        .put("RequestId", request_id)
        .put("RequestSize", request.request_size());

    DataRaftCommandEntry entry;
    entry.partition_id = GetDataRaftGroupPartitionId();
    entry.request_id = request_id;
    entry.request = request;
    entry.request.set_partition_id(entry.partition_id);

    auto pending = std::make_shared<PendingDataRaftApply>();
    {
        std::lock_guard<std::mutex> lock(data_raft_pending_mu_);
        data_raft_pending_[request_id] = pending;
    }

    std::string payload;
    Status status = SerializeDataRaftCommand(entry, &payload);
    if (!status.ok()) {
        std::lock_guard<std::mutex> lock(data_raft_pending_mu_);
        data_raft_pending_.erase(request_id);
        return status;
    }
    uint64_t local_committed_index = 0;
    Status propose_status = data_raft_consensus_->Propose(payload, &local_committed_index);
    if (!propose_status.ok()) {
        DataRaftStatus raft_status;
        Status status = data_raft_consensus_->GetStatus(&raft_status);
        LOG_WARNING("Data raft command propose failed")
            .put("PartitionId", options_.partition_id)
            .put("PrimaryPartitionId", GetPrimaryPartitionId())
            .put("RaftGroupPartitionId", GetDataRaftGroupPartitionId())
            .put("RequestId", request_id)
            .put("GetStatus", status)
            .put("Running", raft_status.running)
            .put("RoleLeader", raft_status.leader)
            .put("Learner", raft_status.learner)
            .put("Term", raft_status.term)
            .put("LeaderReplicaId", raft_status.leader_replica_id)
            .put("CommittedIndex", raft_status.committed_index)
            .put("AppliedIndex", raft_status.applied_index)
            .put("FirstIndex", raft_status.first_index)
            .put("LastIndex", raft_status.last_index)
            .put("VoterCount", raft_status.voter_count)
            .put("LearnerCount", raft_status.learner_count)
            .put("Status", propose_status);
        std::lock_guard<std::mutex> lock(data_raft_pending_mu_);
        data_raft_pending_.erase(request_id);
        return propose_status;
    }
    LOG_INFO("Data raft command propose committed")
        .put("PartitionId", options_.partition_id)
        .put("RequestId", request_id)
        .put("CommittedIndex", local_committed_index);
    Status apply_status =
        data_raft_consensus_->WaitForAppliedIndex(local_committed_index,
                                                  FLAGS_data_raft_propose_timeout_ms);
    if (!apply_status.ok()) {
        LOG_WARNING("Data raft command apply wait failed")
            .put("PartitionId", options_.partition_id)
            .put("RequestId", request_id)
            .put("CommittedIndex", local_committed_index)
            .put("Status", apply_status);
        std::lock_guard<std::mutex> lock(data_raft_pending_mu_);
        data_raft_pending_.erase(request_id);
        return apply_status;
    }
    LOG_INFO("Data raft command apply observed")
        .put("PartitionId", options_.partition_id)
        .put("RequestId", request_id)
        .put("CommittedIndex", local_committed_index);
    {
        std::unique_lock<std::mutex> lock(data_raft_pending_mu_);
        const bool applied = pending->cv.wait_for(
            lock, std::chrono::milliseconds(FLAGS_data_raft_propose_timeout_ms),
            [&pending] { return pending->done; });
        data_raft_pending_.erase(request_id);
        if (!applied) {
            LOG_WARNING("Data raft command response wait timed out")
                .put("PartitionId", options_.partition_id)
                .put("RequestId", request_id)
                .put("CommittedIndex", local_committed_index);
            return Status::DeadlineExceeded("data raft committed response wait timeout");
        }
        if (!pending->status.ok()) {
            LOG_WARNING("Data raft command applied with error")
                .put("PartitionId", options_.partition_id)
                .put("RequestId", request_id)
                .put("CommittedIndex", local_committed_index)
                .put("Status", pending->status);
            return pending->status;
        }
        if (response != nullptr) {
            response->CopyFrom(pending->response);
        }
    }
    LOG_INFO("Data raft command propose done")
        .put("PartitionId", options_.partition_id)
        .put("RequestId", request_id)
        .put("CommittedIndex", local_committed_index);
    if (committed_index != nullptr) {
        *committed_index = local_committed_index;
    }
    return Status::OK();
}

Status Partition::DataRaftReadIndex(uint64_t timeout_ms) {
    if (!IsDataRaftConsensusMode()) {
        return Status::FailedPrecondition(
            "data raft read index requires data_replication_mode=raft_consensus");
    }
    if (data_raft_consensus_ == nullptr) {
        return Status::FailedPrecondition("data raft consensus backend is not initialized");
    }
    return data_raft_consensus_->ReadIndex(timeout_ms);
}

Status Partition::CanServeDataRaftBoundedStaleRead(uint64_t max_stale_index_lag) const {
    if (!IsDataRaftConsensusMode()) {
        return Status::FailedPrecondition(
            "bounded stale read requires data_replication_mode=raft_consensus");
    }
    if (data_raft_consensus_ == nullptr) {
        return Status::FailedPrecondition("data raft consensus backend is not initialized");
    }
    return data_raft_consensus_->CanServeBoundedStaleRead(max_stale_index_lag);
}

Status Partition::GetDataRaftStatus(DataRaftStatus* status) const {
    if (status == nullptr) {
        return Status::InvalidArgument("missing data raft status output");
    }
    if (!IsDataRaftConsensusMode()) {
        return Status::FailedPrecondition(
            "data raft status requires data_replication_mode=raft_consensus");
    }
    if (data_raft_consensus_ == nullptr) {
        return Status::FailedPrecondition("data raft consensus backend is not initialized");
    }
    return data_raft_consensus_->GetStatus(status);
}

Status Partition::TriggerDataRaftSnapshot(uint64_t* snapshot_index) {
    if (!IsDataRaftConsensusMode()) {
        return Status::FailedPrecondition(
            "data raft snapshot requires data_replication_mode=raft_consensus");
    }
    if (data_raft_consensus_ == nullptr) {
        return Status::FailedPrecondition("data raft consensus backend is not initialized");
    }
    return data_raft_consensus_->TriggerSnapshot(snapshot_index);
}

Status Partition::SetConfig(const Config& config) {
    if (stage_ != PartitionLoadStage::LOADED) {
        return Status::FailedPrecondition("partition not loaded");
    }

    const Config& original = GetConfig();
    if (config.version() < original.version()) {
        return Status::FailedPrecondition("legacy config version");
    }
    if (config.version() == original.version()) {
        return Status::OK();
    }

    options_.config.MergeFrom(config);
    if (readonly_) {
        options_.config.mutable_evicter_config()->mutable_operation_type()->set_value(
            OperationType::DUMP);
    }

    op_logger_->UpdateConfig(config);
    index_->UpdateConfig(config);
    page_store_->UpdateConfig(config);
    evicter_->UpdateConfig(config.evicter_config());
    cmd_executor_->UpdateLimitConfig(config.limit_config());
    cmd_executor_->UpdateModuleCustomConfig(config);
    return Status::OK();
}

const Config& Partition::GetConfig() const { return options_.config; }

PartitionInfo Partition::GetInfo() const {
    PartitionInfo info;
    info.set_stage(stage_);
    info.set_readonly(readonly_);
    if (LIKELY(stage_ == PartitionLoadStage::LOADED)) {
        *info.mutable_index_info() = std::move(index_->GetInfo());
        *info.mutable_index_info()->mutable_model_alloc_stats() =
            std::move(allocator_manager_->GetAllocator(AllocatorType::kModel)->GetStats());
        *info.mutable_index_info()->mutable_slot_context_alloc_stats() =
            std::move(allocator_manager_->GetAllocator(AllocatorType::kSlotContext)->GetStats());
        *info.mutable_op_logger_info() = std::move(op_logger_->GetInfo());
        *info.mutable_page_store_info() = std::move(page_store_->GetInfo());
        *info.mutable_condition_info() =
            std::move(ConditionInfoObserver(condition_.data.data()).TransformProtocol());
        *info.mutable_replicator_info() = std::move(replicator_->GetInfo());
    }
    return info;
}

void Partition::InitMembership() {
    if (membership_info_.units_size() == 0) {
        LOG_INFO("no membership info, i am controlled by alchemy cfgsrv")
            .put("PartitionId", options_.partition_id);
        primary_partition_id_ = options_.partition_id;
        data_raft_group_partition_id_ = options_.partition_id;
        return;
    }
    for (const auto& unit : membership_info_.units()) {
        for (uint64_t partition_id : unit.active_id_list()) {
            if (partition_id == options_.partition_id) {
                primary_partition_id_ = unit.primary_id();
                data_raft_group_partition_id_ =
                    StableDataRaftGroupPartitionId(unit, options_.partition_id);
                partition_unit_id_ = unit.partition_unit_id();
                break;
            }
        }
    }  // for membership
    if (data_raft_group_partition_id_ == 0) {
        data_raft_group_partition_id_ = options_.partition_id;
    }
}

Status Partition::UpdateMembership(const MembershipInfo& info) {
    if (info.partition_set_version() < membership_info_.partition_set_version()) {
        return Status::FailedPrecondition("legacy membership info");
    }
    const MembershipInfo old_membership_info = membership_info_;
    bool global_update = false;
    if (info.partition_set_version() > membership_info_.partition_set_version()) {
        global_update = true;
    }

    bool im_valid = false;
    std::set<uint64_t> active_partition_ids;
    for (const auto& unit : info.units()) {
        if (unit.partition_unit_id() != partition_unit_id_) {
            continue;
        }

        const bool data_raft_reconcile_unchanged =
            IsDataRaftConsensusMode() && data_raft_consensus_ != nullptr &&
            data_raft_consensus_->IsLeader();
        if (!global_update && unit.version() == partition_unit_version_ &&
            !data_raft_reconcile_unchanged) {
            // no change
            return Status::OK();
        } else if (unit.version() < partition_unit_version_) {
            return Status::FailedPrecondition("legacy membership unit info");
        }

        partition_unit_version_ = unit.version();
        if (primary_partition_id_ != unit.primary_id()) {
            LOG_WARNING("primary partition changed")
                .put("PartitionId", options_.partition_id)
                .put("from", primary_partition_id_)
                .put("to", unit.primary_id());
            primary_partition_id_ = unit.primary_id();
        }
        if (data_raft_group_partition_id_ == 0) {
            data_raft_group_partition_id_ =
                StableDataRaftGroupPartitionId(unit, options_.partition_id);
        }
        for (uint64_t partition_id : unit.active_id_list()) {
            active_partition_ids.insert(partition_id);
            if (partition_id == options_.partition_id) {
                im_valid = true;
            }
        }
        break;
    }
    membership_info_ = info;

    if (IsDataRaftConsensusMode() && data_raft_consensus_ != nullptr && im_valid &&
        !data_raft_consensus_->IsLeader()) {
        DataRaftStatus raft_status;
        Status status = data_raft_consensus_->GetStatus(&raft_status);
        if (status.ok()) {
            LOG_INFO("Skipping data raft membership reconciliation on non-leader")
                .put("PartitionId", options_.partition_id)
                .put("PrimaryPartitionId", primary_partition_id_)
                .put("RaftGroupPartitionId", data_raft_group_partition_id_)
                .put("RoleLeader", raft_status.leader)
                .put("Learner", raft_status.learner)
                .put("Term", raft_status.term)
                .put("LeaderReplicaId", raft_status.leader_replica_id)
                .put("VoterCount", raft_status.voter_count)
                .put("LearnerCount", raft_status.learner_count);
        }
    }

    if (IsDataRaftConsensusMode() && data_raft_consensus_ != nullptr &&
        data_raft_consensus_->IsLeader() && im_valid) {
        std::vector<DataRaftPeer> active_peers =
            BuildDataRaftPeersForPartition(membership_info_, options_.partition_id);
        for (auto& peer : active_peers) {
            peer.auto_promote = true;
        }
        std::vector<uint64_t> inactive_peer_ids;
        for (const auto& placement : membership_info_.placements()) {
            const uint64_t peer_id = placement.id();
            if (peer_id != options_.partition_id && active_partition_ids.count(peer_id) == 0) {
                inactive_peer_ids.push_back(peer_id);
            }
        }
        const uint64_t target_primary_partition_id = primary_partition_id_;
        const uint64_t partition_id = options_.partition_id;
        std::mutex* membership_mu = &data_raft_membership_mu_;
        DataRaftConsensusBackend* data_raft = data_raft_consensus_.get();
        std::thread([membership_mu, data_raft, partition_id, active_peers, inactive_peer_ids,
                     target_primary_partition_id] {
            std::lock_guard<std::mutex> membership_lock(*membership_mu);
            for (const auto& peer : active_peers) {
                if (peer.replica_id == partition_id) {
                    continue;
                }
                Status status = data_raft->AddLearner(peer);
                if (!status.ok()) {
                    LOG_WARNING("Failed to add data raft learner from membership update; "
                                "will still try promotion because the learner may already exist")
                        .put("PartitionId", partition_id)
                        .put("LearnerId", peer.replica_id)
                        .put("RaftAddr", peer.raft_addr)
                        .put("SnapshotAddr", peer.snapshot_addr)
                        .put("Status", status);
                } else {
                    LOG_INFO("Added data raft learner from membership update")
                        .put("PartitionId", partition_id)
                        .put("LearnerId", peer.replica_id)
                        .put("RaftAddr", peer.raft_addr)
                        .put("SnapshotAddr", peer.snapshot_addr);
                }

                for (int attempt = 1; attempt <= 50; ++attempt) {
                    status = data_raft->PromotePeer(peer.replica_id);
                    if (status.ok()) {
                        LOG_INFO("Promoted data raft learner from membership update")
                            .put("PartitionId", partition_id)
                            .put("LearnerId", peer.replica_id)
                            .put("RaftAddr", peer.raft_addr)
                            .put("SnapshotAddr", peer.snapshot_addr)
                            .put("Attempt", attempt);
                        break;
                    }
                    if (attempt == 1 || attempt == 50 || attempt % 10 == 0) {
                        LOG_WARNING("Failed to promote data raft learner from membership update")
                            .put("PartitionId", partition_id)
                            .put("LearnerId", peer.replica_id)
                            .put("RaftAddr", peer.raft_addr)
                            .put("SnapshotAddr", peer.snapshot_addr)
                            .put("Attempt", attempt)
                            .put("Status", status);
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
            }

            if (target_primary_partition_id != 0 &&
                target_primary_partition_id != partition_id) {
                for (int attempt = 1; attempt <= 50; ++attempt) {
                    Status status = data_raft->TransferLeader(target_primary_partition_id);
                    if (status.ok()) {
                        LOG_INFO("Transferred data raft leadership to promoted primary")
                            .put("PartitionId", partition_id)
                            .put("TargetPrimaryPartitionId", target_primary_partition_id)
                            .put("Attempt", attempt);
                        break;
                    }
                    if (attempt == 1 || attempt == 50 || attempt % 10 == 0) {
                        LOG_WARNING("Failed to transfer data raft leadership to promoted primary")
                            .put("PartitionId", partition_id)
                            .put("TargetPrimaryPartitionId", target_primary_partition_id)
                            .put("Attempt", attempt)
                            .put("Status", status);
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
            }

            for (const uint64_t peer_id : inactive_peer_ids) {
                for (int attempt = 1; attempt <= 50; ++attempt) {
                    Status status = data_raft->RemovePeer(peer_id);
                    if (status.ok()) {
                        LOG_INFO("Removed inactive data raft peer from membership update")
                            .put("PartitionId", partition_id)
                            .put("PeerId", peer_id)
                            .put("Attempt", attempt);
                        break;
                    }
                    if (attempt == 1 || attempt == 50 || attempt % 10 == 0) {
                        LOG_WARNING("Failed to remove inactive data raft peer from membership update")
                            .put("PartitionId", partition_id)
                            .put("PeerId", peer_id)
                            .put("Attempt", attempt)
                            .put("Status", status);
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
            }
        }).detach();
    }

    if (!im_valid) {
        LOG_WARNING("im not in valid membership now")
            .put("partition_id", options_.partition_id)
            .put("membership_version", membership_info_.partition_set_version());
        // Note: Unload behavior is performed in meta_tinker
    }

    return Status::OK();
}

Status Partition::GetStats(PartitionStats* stats) {
    stats->set_id(options_.partition_id);
    stats->set_stage(stage_);
    if (stage_ != PartitionLoadStage::LOADED) {
        return Status::OK();
    }
    if (readonly_) {
        *stats->mutable_replicator_status() = replicator_->GetStatus().ToRpcStatus();
        stats->set_role(PartitionRole::PARTITION_ROLE_SECONDARY);
    } else {
        stats->set_role(PartitionRole::PARTITION_ROLE_PRIMARY);
    }

    // ...
    return Status::OK();
}

Status Partition::UpdateCondition() {
    std::string condition_uri = options_.uri + "-condition";
    Controller ctrl;
    stream::Env::ConditionData condition_data;
    options_.env->GetCondition(&ctrl, condition_uri, &condition_data);
    if (!ctrl.status().ok()) {
        LOG_WARNING("Get condition data failed")
            .put("PartitionId", options_.partition_id)
            .put("Uri", options_.uri)
            .put("LockUri", condition_uri)
            .put("Status", ctrl.status());
        return ctrl.status();
    }

    stream::Env::Condition new_condition;
    new_condition.name = condition_uri;
    new_condition.data = condition_data;

    ConditionInfoObserver new_condition_info(new_condition.data.data());
    ConditionInfoObserver curr_condition_info(condition_.data.data());
    if (UNLIKELY(new_condition_info.LoadVersion() < curr_condition_info.LoadVersion())) {
        // maybe bug
        LOG_ERROR("Load version should be larger")
            .put("PartitionId", options_.partition_id)
            .put("Uri", options_.uri)
            .put("CurrConditionInfo", curr_condition_info.ToString())
            .put("NewConditionInfo", new_condition_info.ToString());
        return Status::FailedPrecondition("Invalid condition info");
    }

    condition_.name = condition_uri;
    condition_.data = condition_data;
    return Status::OK();
}

}  // namespace partition
}  // namespace bcache2
