// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/resource_placement/load_filter_placement_rule.h"

#include <memory>
#include <queue>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/proto_enhance.h"
#include "metaserver_v2/meta/location.h"
#include "metaserver_v2/meta/partition.h"

namespace bcache2 {
namespace metaserver {

Status SimpleLoadFilterPlacementRule::Acquire(const PartitionPtr& partition,
                                              PlacementContext* submit_ctx) {
    CHECK(submit_ctx);

    int total_node_cnt = submit_ctx->Size();
    if (total_node_cnt == 0) {
        return Status::Cancelled("empty candidates | load filter");
    }
    if (total_node_cnt < max_remain_cnt_) {
        return Status::OK();
    }

    struct p {
        NodePtr node;
        size_t p_cnt{0};

        p(NodePtr n, uint32_t table_id) : node(std::move(n)) {
            for (uint64_t id : node->GetPartitionIds(true)) {
                partition_id_t pid(id);
                if (pid.GetTableId() == table_id) {
                    p_cnt++;
                }
            }
        }
    };

    auto cmp = [](const p& l, const p& r) -> bool {
        if (l.p_cnt > r.p_cnt) {
            return true;
        } else if (l.p_cnt == r.p_cnt) {
            return l.node->GetPartitionCount(true) > r.node->GetPartitionCount(true);
        }
        return false;
    };
    const partition_id_t pid(partition->GetId());
    const uint32_t table_id = pid.GetTableId();
    std::priority_queue<p, std::vector<p>, decltype(cmp)> pq(cmp);
    auto& candidates = submit_ctx->Candidates();
    for (auto& iter : candidates) {
        pq.push(p(iter.second, table_id));
    }
    for (int i = 0; !pq.empty(); i++) {
        if (i < max_remain_cnt_) {
            pq.pop();
            continue;
        }
        const NodePtr& node = pq.top().node;
        pq.pop();
        submit_ctx->Remove(node);
    }
    return Status::OK();
}

}  // namespace metaserver
}  // namespace bcache2

