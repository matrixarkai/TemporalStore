// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/resource_placement/placement_rule.h"

#include <map>
#include <random>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "common/status.h"

namespace bcache2 {
namespace metaserver {

std::random_device s_rd;
std::default_random_engine s_rng(s_rd());

PlacementContext::PlacementContext(PartitionPtr p) : pid_(p->GetId()), partition_(std::move(p)) {}
PlacementContext::~PlacementContext() {
    for (auto iter : candidate_nodes_) {
        iter.second->RemoveIntentPartition(pid_);
    }
}

void PlacementContext::Add(const NodePtr& node) {
    node->AddIntentPartition(pid_);
    candidate_nodes_.emplace(node->GetId(), node);
}

void PlacementContext::Remove(uint64_t node_id) {
    auto iter = candidate_nodes_.find(node_id);
    if (iter == candidate_nodes_.end()) {
        return;
    }
    iter->second->RemoveIntentPartition(pid_);
    candidate_nodes_.erase(iter);
}

void PlacementContext::Remove(const NodePtr& node) {
    node->RemoveIntentPartition(pid_);
    candidate_nodes_.erase(node->GetId());
}

void PlacementContext::Block(uint64_t node_id) { excluded_node_ids_.insert(node_id); }

size_t PlacementContext::Size() const { return candidate_nodes_.size(); }

const std::map<uint64_t, NodePtr>& PlacementContext::Candidates() { return candidate_nodes_; }

Status PlacementContext::AutoAward() {
    std::vector<NodePtr> flat;
    for (auto iter : candidate_nodes_) {
        const uint64_t id = iter.first;
        if (excluded_node_ids_.count(id) > 0) {
            continue;
        }
        flat.push_back(iter.second);
    }
    if (flat.empty()) {
        return Status::Cancelled("empty candidates");
    }
    int idx = std::uniform_int_distribution<>(0, flat.size() - 1)(s_rng);
    champion_ = flat[idx];
    candidate_nodes_.erase(champion_->GetId());
    return Status::OK();
}

NodePtr PlacementContext::Champion() const { return champion_; }

}  // namespace metaserver
}  // namespace bcache2
