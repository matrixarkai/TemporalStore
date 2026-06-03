// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/resource_placement/host_dedup_placement_rule.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "metaserver_v2/flags.h"

namespace bcache2 {
namespace metaserver {

Status HostDeduplicatePlacementRule::Acquire(const PartitionPtr& partition,
                                             PlacementContext* submit_ctx) {
    CHECK(submit_ctx);
    PartitionSet* pset = partition->GetPartitionSet();
    if (pset == nullptr) {
        return Status::Internal("pset not found");
    }

    std::vector<Endpoint> sibling_eps;
    std::vector<PartitionPtr> siblings = pset->GetAllPartitions();
    for (auto& p : siblings) {
        auto state = p->GetState();
        if (state == PartitionState::P_FROZEN) {
            // Note: frozen partition should be ignored
            continue;
        }
        const PlacementSpec& placement = p->GetPlacementActual();
        if (placement.has_server()) {
            sibling_eps.push_back(placement.server());
        }
    }
    std::unordered_map<Endpoint, std::vector<uint64_t>, EndpointHash> candidate_eps;
    for (auto& iter : submit_ctx->Candidates()) {
        auto& candidate = iter.second;
        Server* server = candidate->GetServer();
        if (server != nullptr) {
            candidate_eps[server->GetEndpoint()].push_back(iter.first);
        }
    }
    for (const auto& sibling_ep : sibling_eps) {
        for (const auto& iter : candidate_eps) {
            if (FLAGS_metaserver_placement_host_deduplicate) {
                if (!IsSameHost(sibling_ep, iter.first)) {
                    continue;
                }
            } else {
                // for seperate partitions in local test
                if (sibling_ep != iter.first) {
                    continue;
                }
            }

            for (auto& id : iter.second) {
                submit_ctx->Remove(id);
                submit_ctx->Block(id);
            }
        }
    }
    return submit_ctx->Size() > 0 ? Status::OK()
                                  : Status::Cancelled("empty candidates | host dedup");
}

}  // namespace metaserver
}  // namespace bcache2
