// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <map>
#include <memory>
#include <set>
#include <string>

#include "metaserver_v2/meta/partition.h"
#include "metaserver_v2/meta/server.h"

namespace bcache2 {
namespace metaserver {

struct PlacementContext;

/// Basic abstract
struct PlacementRule {
    virtual ~PlacementRule() = default;

    virtual bool Enabled() const { return true; }
    virtual std::string Name() const = 0;
    virtual Status Acquire(const PartitionPtr& partition, PlacementContext* submit_ctx) = 0;
};

struct PlacementContext {
    explicit PlacementContext(PartitionPtr p);
    ~PlacementContext();

    size_t Size() const;
    void Add(const NodePtr& node);
    void Remove(uint64_t node_id);
    void Remove(const NodePtr& node);
    void Block(uint64_t node_id);

    NodePtr Champion() const;
    const std::map<uint64_t, NodePtr>& Candidates();

    Status AutoAward();

 private:
    const uint64_t pid_;
    const PartitionPtr partition_;
    NodePtr champion_{nullptr};
    std::set<uint64_t> excluded_node_ids_{};
    std::map<uint64_t, NodePtr> candidate_nodes_{};
};

}  // namespace metaserver
}  // namespace bcache2

