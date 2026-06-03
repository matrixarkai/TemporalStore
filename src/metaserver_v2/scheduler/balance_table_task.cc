// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/scheduler/balance_table_task.h"

#include <algorithm>
#include <random>
#include <set>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "metaserver_v2/meta/partition.h"
#include "metaserver_v2/meta/table.h"
#include "metaserver_v2/metrics.h"
#include "metaserver_v2/raft_connector.h"
#include "metaserver_v2/scheduler/priority.h"
#include "metaserver_v2/scheduler/scheduler_manager.h"

namespace bcache2 {
namespace metaserver {

extern std::default_random_engine s_rng;

BalanceTableTask::BalanceTableTask(TablePtr table, PartitionUnit unit, SchedulerManager* schd_mgr,
                                   LocationManager<Server>* loc_mgr)
    : Task(kTaskPriorityBalanceTable),
      table_(std::move(table)),
      unit_(std::move(unit)),
      schd_mgr_(schd_mgr),
      loc_mgr_(loc_mgr) {}

/// Note: it's brute-force policy currently:
///  1. calc avg partition count per node
///  2. get all related server nodes and find these reach high water
///  3. freeze partition from high water nodes, reuse palcement rule to auto select low load server
///  nodes
Status BalanceTableTask::Process() {
    if (!table_ || table_->GetState() != TableState::TABLE_NORMAL) {
        return Status::Aborted("table state mismatch");
    }

    Prepare();
    const uint32_t table_id = table_->GetId();
    const size_t safe_gap = FLAGS_metaserver_balance_partition_count_safe_gap;
    for (auto& pair : stats_) {
        Stats& stats = pair.second;
        size_t safe_line = stats.partition_count_per_node_avg + safe_gap;
        for (auto& node : stats.used_server_nodes) {
            std::vector<uint64_t> table_pids;
            std::set<uint64_t> all_pids = node->GetPartitionIds();
            for (uint64_t pid : all_pids) {
                partition_id_t pidt(pid);
                if (pidt.GetTableId() != table_id) {
                    continue;
                }
                PartitionPtr partition;
                Status status = table_->GetPartition(pid, &partition);
                if (!status.ok()) {
                    LOG_WARNING("invalid partition id, BUG")
                        .put("pid", pid)
                        .put("node", node->GetId());
                    continue;
                }
                if (partition->GetState() == PartitionState::P_NORMAL) {
                    table_pids.push_back(pid);
                }
            }
            if (table_pids.size() <= safe_line) {
                continue;
            }
            LOG_INFO("find high load node")
                .put("node", node->GetServer()->GetEndpoint())
                .put("table_id", table_id)
                .put("safe_line", safe_line)
                .put("got", table_pids.size());
            int index = std::uniform_int_distribution<>(0, table_pids.size() - 1)(s_rng);
            for (size_t i = 0; i < table_pids.size() - safe_line; i++) {
                balance_pids_.push_back(table_pids[(index++) % table_pids.size()]);
            }
        }
    }
    if (balance_pids_.empty()) {
        return Status::OK();
    }

    LOG_INFO("find high load node, prepare to move partition out")
        .put("total", balance_pids_.size())
        .put("table_id", table_id)
        .put("table_name", table_->GetName());
    ExecuteBalance();
    return Status::OK();
}

void BalanceTableTask::Prepare() {
    std::vector<PartitionSetPtr> psets = table_->GetAllPartitionSets();
    // 1. collect
    for (auto& pset : psets) {
        std::vector<PartitionPtr> partitions = pset->GetPartitions(unit_.id());
        for (auto& p : partitions) {
            if (p->GetState() != PartitionState::P_NORMAL) {
                continue;
            }
            const Location& loc = p->GetPlacementExpect();
            const PlacementSpec& placement_spec = p->GetPlacementActual();
            if (!BelongsTo(placement_spec.location(), loc)) {
                LOG_WARNING("invalid placement, maybe just modified placement expect")
                    .put("p", *p)
                    .put("loc_expect", loc)
                    .put("loc_actual", placement_spec.location());
                continue;
            }
            NodePtr node = p->GetNode();
            if (!node) {
                LOG_WARNING("invalid node ref").put("p", *p);
                continue;
            }
            Stats& stats = stats_[loc];
            stats.partition_count_total++;
            stats.used_server_nodes.insert(std::move(node));
        }
    }  // for psets

    // 2. aggr
    for (auto& pair : stats_) {
        Stats& stats = pair.second;
        size_t total_node_count = 0;
        const Location& loc = pair.first;
        loc_mgr_->List(loc, [&](auto& server) -> bool {
            if (server->GetState() != ServerState::SERVER_NORMAL ||
                server->GetLocation().tag() != loc.tag()) {
                return false;
            }
            total_node_count += server->GetNodes().size();
            return false;  // do not need results
        });
        if (total_node_count == 0) {
            LOG_WARNING("no valid node found, BUG").put("loc", pair.first);
            continue;
        }
        if (stats.partition_count_total == 0) {
            stats.partition_count_per_node_avg = 1;
        } else {
            stats.partition_count_per_node_avg =
                stats.partition_count_total / total_node_count +
                (stats.partition_count_total % total_node_count == 0 ? 0 : 1);
        }
    }
}

void BalanceTableTask::ExecuteBalance() {
    std::set<uint64_t> used_pids;
    size_t limit = FLAGS_metaserver_max_balance_partition_per_round;
    size_t balance_count = 0;
    FreezePartitionRequest request;
    request.mutable_id()->set_timestamp(butil::gettimeofday_s());
    request.mutable_id()->set_cluster_name(FLAGS_metaserver_cluster_name);
    request.mutable_id()->set_operator_name("balance_routine");
    while (balance_count < limit && used_pids.size() < balance_pids_.size()) {
        int index = std::uniform_int_distribution<>(0, balance_pids_.size() - 1)(s_rng);
        uint64_t pid = balance_pids_[index];
        if (used_pids.count(pid) > 0) {
            continue;
        }
        MS_METRIC(balance_partition_count)->Add(1);
        ++balance_count;
        used_pids.insert(pid);

        // TODO(wuzhenyu) refactor me
        // brute-force freeze is only suitable for PROMOTE_DERIVED mode
        if (!table_->CanFreezePartitionSafely(pid)) {
            LOG_INFO("partition can not be freeze safely, skip balance");
            continue;
        }
        uint64_t log_id = butil::fast_rand();
        LOG_INFO("propose to raft to freeze partition due to blanace policy")
            .put("log_id", log_id)
            .put("pid", pid);
        MS_METRIC(freeze_partition_count)->Add(1);
        request.set_partition_id(pid);
        Status status =
            schd_mgr_->GetRaftConnector()->Propose(log_id, MS_LOG_PARTITION_FREEZE, &request);
        if (!status.ok()) {
            LOG_WARNING("propose to raft failed")
                .put("log_id", log_id)
                .put("status", status)
                .put("pid", pid);
        }
    }
}

}  // namespace metaserver
}  // namespace bcache2

