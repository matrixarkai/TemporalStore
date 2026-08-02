// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>
#include <string>

#include "common/proto_enhance.h"
#include "metaserver_v2/meta/location.h"
#include "metaserver_v2/meta/partition.h"
#include "metaserver_v2/resource_placement/placement_rule.h"

namespace bcache2 {
namespace metaserver {

struct HostDeduplicatePlacementRule : public PlacementRule {
    std::string Name() const override { return "HostDeduplicatePlacementRule"; }

    Status Acquire(const PartitionPtr& partition, PlacementContext* submit_ctx) override;
};

}  // namespace metaserver
}  // namespace bcache2

