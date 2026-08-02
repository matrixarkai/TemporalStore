// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>
#include <string>

#include "metaserver_v2/meta/partition.h"
#include "metaserver_v2/resource_placement/placement_rule.h"

namespace bcache2 {
namespace metaserver {

struct SimpleLoadFilterPlacementRule : public PlacementRule {
    explicit SimpleLoadFilterPlacementRule(int max_candidates_remain)
        : max_remain_cnt_(max_candidates_remain) {}

    std::string Name() const override { return "SimpleLoadFilterPlacementRule"; }

    Status Acquire(const PartitionPtr& partition, PlacementContext* submit_ctx) override;

 private:
    const int max_remain_cnt_{0};
};

}  // namespace metaserver
}  // namespace bcache2

