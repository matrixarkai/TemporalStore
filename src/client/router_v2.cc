// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "client/router_v2.h"

#include <byte/algorithm/crc64.h>

#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "client/command.h"
#include "common/logging.h"
#include "common/proto_enhance.h"
#include "common/status.h"
#include "protocol/master.pb.h"

namespace bcache2 {
namespace client {

Status RouterV2::GetPartitionSetId(const std::string& key, uint64_t* pset_id) const {
    uint32_t slot_id = GetSlotId(key);
    if (partition_map_.size() < 1) {
        LOG_WARNING("Partition not exist").put("Key", key).put("Slot", slot_id);
        return Status::Internal("Slot not found");
    }
    auto upper = slot_map_.upper_bound(slot_id);
    if (upper == slot_map_.begin()) {
        if (slot_map_.size() == 1) {
            *pset_id = upper->second;
            return Status::OK();
        }
        LOG_WARNING("Partition not found for slot before first range")
            .put("Key", key)
            .put("Slot", slot_id);
        return Status::Internal("Slot not found");
    }
    auto iter = std::prev(upper);
    auto pset = partition_map_.find(iter->second);
    if (pset == partition_map_.end()) {
        LOG_WARNING("Partition set not found").put("Key", key).put("Slot", slot_id);
        return Status::Internal("Partition info not found");
    }
    if (slot_id > pset->second->end_slot_id && slot_map_.size() != 1) {
        LOG_WARNING("Partition not found for slot after range")
            .put("Key", key)
            .put("Slot", slot_id)
            .put("RangeEnd", pset->second->end_slot_id);
        return Status::Internal("Slot not found");
    }
    *pset_id = iter->second;
    return Status::OK();
}

Status RouterV2::GetServerEndpoint(const std::string& key, const PartitionPickOptions& pick_opts,
                                   std::string* endpoint, uint64_t* partition_id) const {
    uint64_t pset_id = 0;
    Status status = GetPartitionSetId(key, &pset_id);
    if (!status.ok()) {
        return status;
    }
    auto iter = partition_map_.find(pset_id);
    if (iter == partition_map_.end()) {
        LOG_WARNING("Partition not found").put("Key", key).put("pset", pset_id);
        return Status::Internal("Partition info not found");
    }

    auto& pset = iter->second;
    if (pick_opts.policy == PartitionPickOptions::Policy::kPrimary || pset->secondaries.empty()) {
        *endpoint = pset->primary.endpoint_str;
        *partition_id = pset->primary.id;
        return Status::OK();
    }

    pset->round_robin_index++;
    if (pick_opts.policy == PartitionPickOptions::Policy::kVdcAffinity) {
        std::vector<const Partition*> candidates;
        if (pset->primary.location.vdc() == pick_opts.affinity_vdc) {
            candidates.push_back(&(pset->primary));
        }
        for (const auto& p : pset->secondaries) {
            if (p.location.vdc() == pick_opts.affinity_vdc) {
                candidates.push_back(&p);
            }
        }

        if (!candidates.empty()) {
            const Partition* partition = candidates[pset->round_robin_index % candidates.size()];
            *endpoint = partition->endpoint_str;
            *partition_id = partition->id;
            return Status::OK();
        }
    }
    const Partition& partition =
        pset->secondaries[pset->round_robin_index % pset->secondaries.size()];
    *endpoint = partition.endpoint_str;
    *partition_id = partition.id;
    return Status::OK();
}

std::string RouterV2::Endpoint2String(const Endpoint& ep) {
    if (af_ == AddressFamily::kIp4) {
        return ep.ip4() + ":" + std::to_string(ep.port());
    }
    return ep.ip6() + ":" + std::to_string(ep.port());
}

Status RouterV2::UpdatePartition(const GetTableTopoResponse& topo) {
    version_ = topo.topo_version();

    decltype(slot_map_) slot_map;
    decltype(partition_map_) partition_map;
    std::unordered_map<uint32_t, Location> id2loc;
    for (auto& loc : topo.location_map()) {
        id2loc[loc.id()] = loc.location();
    }
    for (auto iter = topo.partitions().begin(); iter < topo.partitions().end(); ++iter) {
        auto pset = std::make_shared<PartitionSet>();
        pset->id = iter->partition_id();
        pset->start_slot_id = iter->start_slot();
        pset->end_slot_id = iter->end_slot();

        if (iter->load_infos_size() > 0) {
            for (const auto& load_info : iter->load_infos()) {
                Partition partition;
                partition.id = load_info.partition_id();
                partition.is_primary = load_info.is_master();
                partition.endpoint = load_info.endpoint();
                partition.location = load_info.location();
                partition.endpoint_str = Endpoint2String(load_info.endpoint());
                if (load_info.is_master()) {
                    pset->primary = std::move(partition);
                } else {
                    pset->secondaries.push_back(std::move(partition));
                }
            }
        } else if (iter->compressed_load_infos_size() > 0) {
            for (const auto& load_info : iter->compressed_load_infos()) {
                Partition partition;
                partition.id = load_info.partition_id();
                partition.is_primary = load_info.is_master();
                Status status = ToEndpoint(load_info.endpoint2(), &partition.endpoint);
                BYTE_ASSERT(status.ok());
                partition.location = id2loc[load_info.location_wrapper_id()];
                partition.endpoint_str = Endpoint2String(partition.endpoint);
                if (load_info.is_master()) {
                    pset->primary = std::move(partition);
                } else {
                    pset->secondaries.push_back(std::move(partition));
                }
            }
        } else {
            LOG_WARNING("no load info found").put("pset", pset->id);
            return Status::Internal("no partition found");
        }

        if (pset->primary.id == 0) {
            LOG_WARNING("Partition no primary").put("pset", pset->id);
            return Status::Internal("Partition no primary");
        }
        slot_map.emplace(std::make_pair(pset->start_slot_id, pset->id));
        partition_map.emplace(std::make_pair(pset->id, std::move(pset)));
    }

    std::swap(slot_map_, slot_map);
    std::swap(partition_map_, partition_map);
    return Status::OK();
}

}  // namespace client
}  // namespace bcache2
