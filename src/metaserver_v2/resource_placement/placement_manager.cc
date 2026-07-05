// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/resource_placement/placement_manager.h"

#include <mutex>
#include <random>
#include <utility>

#include "common/logging.h"
#include "common/status.h"
#include "metaserver_v2/flags.h"
#include "metaserver_v2/metrics.h"
#include "metaserver_v2/resource_placement/host_dedup_placement_rule.h"
#include "metaserver_v2/resource_placement/load_filter_placement_rule.h"
#include "metaserver_v2/resource_placement/location_placement_rule.h"

namespace bcache2 {
namespace metaserver {

PlacementManager::PlacementManager(Metabase* metabase) : metabase_(metabase) {}

PlacementManager::~PlacementManager() {}

void PlacementManager::SetDefaultRules() {
    // TODO(wuzhenyu) refactor
    std::vector<std::shared_ptr<PlacementRule>> result;
    result.emplace_back(new LocationPlacementRule(metabase_->GetServerLocationManager()));
    result.emplace_back(new HostDeduplicatePlacementRule());
    result.emplace_back(
        new SimpleLoadFilterPlacementRule(FLAGS_metaserver_placement_low_load_candidate_count));

    std::lock_guard<bthread::Mutex> _(mu_);
    std::swap(result, rules_);
}

void PlacementManager::UpdateRules(std::vector<std::shared_ptr<PlacementRule>> rules) {
    std::lock_guard<bthread::Mutex> _(mu_);
    std::swap(rules, rules_);
}

Status PlacementManager::PlacePartition(const PartitionPtr& partition, NodePtr* node) {
    Status status;
    PlacementContext ctx(partition);
    std::vector<std::shared_ptr<PlacementRule>> rules;
    {
        std::lock_guard<bthread::Mutex> _(mu_);
        rules = rules_;
    }
    for (auto& rule : rules) {
        if (!rule->Enabled()) {
            LOG_INFO("rule is not enabled").put("name", rule->Name());
            continue;
        }
        status = rule->Acquire(partition, &ctx);
        if (!status.ok()) {
            LOG_INFO("acquire resource failed").put("rule", rule->Name()).put("result", status);
            break;
        }

        size_t cnt = ctx.Size();
        LOG_INFO("acquire resource").put("rule", rule->Name()).put("candidate_cnt", cnt);
        // here is a double check
        if (cnt == 0) {
            status = Status::Cancelled("empty resource");
            break;
        }
    }
    if (!status.ok()) {
        MS_METRIC(placement_fail_count).get()->Add(1);
        return status;
    }

    status = ctx.AutoAward();
    if (!status.ok()) {
        MS_METRIC(placement_fail_count).get()->Add(1);
        return status;
    }
    *node = ctx.Champion();
    return Status::OK();
}

}  // namespace metaserver
}  // namespace bcache2
