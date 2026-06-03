// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>
#include <vector>

#include "bthread/mutex.h"

#include "metaserver_v2/meta/metabase.h"
#include "metaserver_v2/meta/partition.h"
#include "metaserver_v2/resource_placement/placement_rule.h"

namespace bcache2 {
namespace metaserver {

class PlacementManager {
 public:
    explicit PlacementManager(Metabase*);
    ~PlacementManager();

    void SetDefaultRules();
    void UpdateRules(std::vector<std::shared_ptr<PlacementRule>>);

    Status PlacePartition(const PartitionPtr& partition, NodePtr* node);

 private:
    Metabase* metabase_{nullptr};

    bthread::Mutex mu_;
    std::vector<std::shared_ptr<PlacementRule>> rules_;
};

}  // namespace metaserver
}  // namespace bcache2

