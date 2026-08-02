// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/resource_placement/location_placement_rule.h"

#include <memory>
#include <string>
#include <vector>

#include "common/proto_enhance.h"
#include "metaserver_v2/meta/location.h"
#include "metaserver_v2/meta/partition.h"

namespace bcache2 {
namespace metaserver {

Status LocationPlacementRule::Acquire(const PartitionPtr& partition, PlacementContext* submit_ctx) {
    CHECK(submit_ctx);
    const Location& loc_expect = partition->GetPlacementExpect();
    if (IsEmpty(loc_expect)) {
        return Status::InvalidArgument("empty placement expect");
    }

    auto servers = loc_mgr_->List(loc_expect, [&loc_expect](const auto& s) -> bool {
        return s->GetState() == ServerState::SERVER_NORMAL &&
               s->RealtimeStats().GetLastHeartbeatTimeUs() > 0 &&
               loc_expect.tag() == s->GetLocation().tag();
    });
    if (servers.empty()) {
        return Status::Cancelled("no proper server found");
    }
    for (auto& server : servers) {
        for (auto& node : server->GetNodes()) {
            submit_ctx->Add(node);
        }
    }
    return Status::OK();
}

}  // namespace metaserver
}  // namespace bcache2
