// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/ha/meta_check_routine.h"

#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "butil/time.h"

#include "common/logging.h"
#include "common/status.h"
#include "metaserver_v2/flags.h"
#include "metaserver_v2/meta/metabase.h"
#include "metaserver_v2/metrics.h"

namespace bcache2 {
namespace metaserver {

MetaCheckRoutine::MetaCheckRoutine(Metabase* metabase) : metabase_(metabase) {}

MetaCheckRoutine::~MetaCheckRoutine() { Stop(); }

Status MetaCheckRoutine::Start(const Options& opts) {
    if (running_) {
        return Status::Internal("already running");
    }
    running_ = true;
    raft_connector_ = opts.raft_connector;

    Status status = opts.event_harbor->RegisterListener(this);
    if (!status.ok()) {
        running_ = false;
        return status;
    }

    int rc = bthread_start_background(&routine_thd_, nullptr, RunRoutine, this);
    if (rc != 0) {
        running_ = false;
        return Status::Internal("failed to start background bthread");
    }
    return Status::OK();
}

void MetaCheckRoutine::Stop() {
    if (!running_) {
        return;
    }
    running_ = false;
    bthread_stop(routine_thd_);
    bthread_join(routine_thd_, nullptr);
}

void* MetaCheckRoutine::RunRoutine(void* arg) {
    auto cr = static_cast<MetaCheckRoutine*>(arg);
    cr->Routine();
    return nullptr;
}

void MetaCheckRoutine::Consume(const EventHarbor::Event* e) {
    if (e->Topic() == kTopicServerHeartbeat) {
        HandleServerHeartbeat(static_cast<const ServerHeartbeatEvent*>(e));
    }
}

void MetaCheckRoutine::HandleServerHeartbeat(const ServerHeartbeatEvent* e) {
    if (!e->request.with_stats()) {
        return;
    }

    if (!FLAGS_metaserver_freeze_missing_partition_enabled) {
        return;
    }

    const Endpoint& ep = e->request.endpoint();
    ServerPtr server = metabase_->GetServerLocationManager()->Get(ep);
    if (!server || server->GetState() != ServerState::SERVER_NORMAL) {
        return;
    }

    std::unordered_map<uint64_t, PartitionStats> remote_partition_ids;
    for (const auto& stats : e->request.stats()) {
        remote_partition_ids[stats.id()] = stats;
    }
    std::vector<NodePtr> nodes = server->GetNodes();
    for (auto& node : nodes) {
        for (uint64_t id : node->GetPartitionIds()) {
            partition_id_t pid(id);
            TablePtr table;
            Status status = metabase_->GetNamespaceManager()->GetTable(pid.GetTableId(), &table);
            if (status.IsNotFound()) {
                continue;
            }
            CHECK(status.ok()) << this << status;
            PartitionPtr partition;
            status = table->GetPartition(id, &partition);
            if (status.IsNotFound() || partition->GetState() != PartitionState::P_NORMAL) {
                continue;
            }
            CHECK(status.ok()) << this << status;

            auto iter = remote_partition_ids.find(id);
            if (iter != remote_partition_ids.end()) {
                partition->SetRealTimeStats(iter->second);
            } else {
                LOG_INFO("partition not found in server")
                    .put("partition", *partition)
                    .put("server", ep);
                MS_METRIC(missing_partition_count)->Add(1);
                if (!table->CanFreezePartitionSafely(id)) {
                    LOG_WARNING("partition is the last considered healthy one, freeze it by force")
                        .put("partition", *partition);
                }
                FreezePartition(partition);
            }  // partition lost from server
        }
    }  // for nodes of server
}

void MetaCheckRoutine::Routine() {
    LOG_INFO("enter routine");
    while (running_) {
        DropFrozenMeta();
        PatrolAllPartition();
        bthread_usleep(FLAGS_metaserver_meta_check_routine_interval_sec * 1'000 * 1'000);
    }
    LOG_INFO("exiting routine");
}

void MetaCheckRoutine::DropFrozenMeta() {
    NamespaceManager* ns_mgr = metabase_->GetNamespaceManager();
    std::vector<NamespacePtr> namespaces = ns_mgr->List();
    int64_t now = butil::gettimeofday_s();
    const int64_t table_drop_timepoint_sec = now - FLAGS_metaserver_frozen_table_cool_down_time_sec;
    const int64_t partition_drop_timepoint_sec =
        now - FLAGS_metaserver_frozen_partition_cool_down_time_sec;
    size_t drop_count = 0;
    constexpr size_t kMaxDropCount = 20;
    for (auto& ns : namespaces) {
        if (drop_count > kMaxDropCount) {
            break;
        }
        if (table_drop_timepoint_sec > 0) {
            std::vector<TablePtr> frozen_tables = ns->ListFrozen(table_drop_timepoint_sec);
            for (auto& table : frozen_tables) {
                Status status = DropFrozenTable(table);
                LOG_WARNING("drop frozen table")
                    .put("table_id", table->GetId())
                    .put("table_name", table->GetFullName())
                    .put("frozen_at", table->GetFrozenTimeSec())
                    .put("result", status);
                if (status.ok()) {
                    ++drop_count;
                }
            }  // drop tables
        }

        if (partition_drop_timepoint_sec > 0) {
            std::vector<TablePtr> tables = ns->List();
            for (auto table : tables) {
                if (drop_count > kMaxDropCount) {
                    break;
                }
                std::vector<PartitionPtr> frozen_partitions =
                    table->ListFrozenPartitions(partition_drop_timepoint_sec);
                for (auto& partition : frozen_partitions) {
                    Status status = DropFrozenPartition(partition);
                    LOG_WARNING("drop frozen partition")
                        .put("partition", *partition)
                        .put("frozen_at", partition->GetFrozenTimeSec())
                        .put("result", status);
                    if (status.ok()) {
                        ++drop_count;
                    }
                }
            }  // for table
        }      // drop partition
    }          // for ns
}

void MetaCheckRoutine::PatrolAllPartition() {
    NamespaceManager* ns_mgr = metabase_->GetNamespaceManager();
    std::vector<NamespacePtr> namespaces = ns_mgr->List();
    for (auto& ns : namespaces) {
        std::vector<TablePtr> tables = ns->List();
        for (auto table : tables) {
            TableState table_state = table->GetState();
            if (table_state != TableState::TABLE_CREATING &&
                table_state != TableState::TABLE_NORMAL) {
                continue;
            }
            std::vector<PartitionPtr> partitions = table->GetAllPartitions();
            for (auto partition : partitions) {
                PatrolPartition(partition);
            }
        }  // for table
    }      // for ns
}

static void InitRequestId(RequestId* id) {
    id->set_timestamp(butil::gettimeofday_s());
    id->set_cluster_name(FLAGS_metaserver_cluster_name);
    id->set_operator_name("meta_check_routine");
}

Status MetaCheckRoutine::DropFrozenTable(const TablePtr& table) {
    CHECK(table->GetState() == TableState::TABLE_FROZEN) << this;

    DropTableRequest request;
    InitRequestId(request.mutable_id());
    request.set_table_id(table->GetId());
    uint64_t log_id = butil::fast_rand();
    LOG_INFO("propose to raft to drop table")
        .put("log_id", log_id)
        .put("table_id", table->GetId())
        .put("table_name", table->GetFullName());
    Status status = raft_connector_->Propose(log_id, MS_LOG_TABLE_DROP, &request);
    if (!status.ok()) {
        LOG_WARNING("propose to raft failed")
            .put("log_id", log_id)
            .put("status", status)
            .put("table_id", table->GetId())
            .put("table_name", table->GetFullName());
    }
    return status;
}

Status MetaCheckRoutine::DropFrozenPartition(const PartitionPtr& partition) {
    if (partition->GetState() != PartitionState::P_FROZEN) {
        return Status::Internal("invalid partition state");
    }
    DropPartitionRequest request;
    InitRequestId(request.mutable_id());
    request.set_partition_id(partition->GetId());
    uint64_t log_id = butil::fast_rand();
    LOG_INFO("propose to raft to drop partition")
        .put("log_id", log_id)
        .put("partition", *partition);
    Status status = raft_connector_->Propose(log_id, MS_LOG_PARTITION_DROP, &request);
    if (!status.ok()) {
        LOG_WARNING("propose to raft failed")
            .put("log_id", log_id)
            .put("status", status)
            .put("partition", *partition);
    }
    return status;
}

Status MetaCheckRoutine::FreezePartition(const PartitionPtr& partition) {
    if (partition->GetState() != PartitionState::P_NORMAL &&
        partition->GetState() != PartitionState::P_LOADING) {
        return Status::FailedPrecondition("partition is already abnormal");
    }

    int64_t now = butil::cpuwide_time_s() / 60;
    if (curr_timestamp_min_ != now) {
        curr_timestamp_min_ = now;
        freeze_partition_count_this_window_ = 0;
    } else if (freeze_partition_count_this_window_ >=
               FLAGS_metaserver_meta_check_max_freeze_partition_per_min) {
        LOG_INFO("too many partition frozen this minute, fallback")
            .put("v", freeze_partition_count_this_window_);
        return Status::FailedPrecondition("rate limited");
    }

    MS_METRIC(freeze_partition_count)->Add(1);
    FreezePartitionRequest request;
    InitRequestId(request.mutable_id());
    request.set_partition_id(partition->GetId());
    request.set_force(true);
    uint64_t log_id = butil::fast_rand();
    LOG_INFO("propose to raft to freeze partition")
        .put("log_id", log_id)
        .put("partition", *partition);
    Status status = raft_connector_->Propose(log_id, MS_LOG_PARTITION_FREEZE, &request);
    if (!status.ok()) {
        LOG_WARNING("propose to raft failed")
            .put("log_id", log_id)
            .put("status", status)
            .put("partition", *partition);
    } else {
        freeze_partition_count_this_window_++;
    }
    return status;
}

void MetaCheckRoutine::PatrolPartition(const PartitionPtr& partition) {
    PartitionState state = partition->GetState();
    if (state == PartitionState::P_LOADING) {
        const PartitionInfo info = partition->GetInfo();
        int64_t created_at = info.created_at();
        LOG_INFO("found loading partition")
            .put("partition", *partition)
            .put("created_at", created_at);

        const int64_t freeze_timepoint_sec =
            butil::gettimeofday_s() - FLAGS_metaserver_loading_partition_max_loading_time_sec;
        if (freeze_timepoint_sec > 0 && freeze_timepoint_sec > created_at) {
            LOG_INFO("found long loading partition, try to freeze")
                .put("partition", *partition)
                .put("created_at", created_at);
            MS_METRIC(long_time_loading_partition_count)->Add(1);
            FreezePartition(partition);
        }
    } else if (state == PartitionState::P_NORMAL) {
        const Location& loc = partition->GetPlacementExpect();
        const PlacementSpec& placement_actual = partition->GetPlacementActual();
        if (!BelongsTo(placement_actual.location(), loc)) {
            LOG_WARNING("placement not match, try to freeze it")
                .put("partition", *partition)
                .put("loc_expect", loc)
                .put("loc_actual", placement_actual.location());
            FreezePartition(partition);
            return;
        }
        PartitionStats stats = partition->GetRealTimeStats();
        if (partition->GetRole() == PartitionRole::PARTITION_ROLE_SECONDARY) {
            Status replicator_status = Status::FromRpcStatus(stats.replicator_status());
            if (!replicator_status.ok()) {
                LOG_WARNING("replicator status error, try to freeze it")
                    .put("partition", *partition);
                MS_METRIC(replicator_error_partition_count)->Add(1);
                FreezePartition(partition);
            }
        }  // if role == secondary
    }
}

}  // namespace metaserver
}  // namespace bcache2
