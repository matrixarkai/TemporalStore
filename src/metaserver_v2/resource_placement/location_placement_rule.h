// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>
#include <string>

#include "metaserver_v2/meta/location.h"
#include "metaserver_v2/meta/partition.h"
#include "metaserver_v2/meta/server.h"
#include "metaserver_v2/resource_placement/placement_rule.h"

namespace bcache2 {
namespace metaserver {

class LocationPlacementRule : public PlacementRule {
 public:
    explicit LocationPlacementRule(LocationManager<Server>* loc_mgr) : loc_mgr_(loc_mgr) {}

    std::string Name() const override { return "LocationPlacementRule"; }

    Status Acquire(const PartitionPtr& partition, PlacementContext* submit_ctx) override;

 private:
    LocationManager<Server>* loc_mgr_{nullptr};
};

}  // namespace metaserver
}  // namespace bcache2

