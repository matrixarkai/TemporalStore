// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "client/router.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "client/command.h"
#include "common/logging.h"
#include "common/status.h"
#include "protocol/master.pb.h"

namespace bcache2 {
namespace client {

static inline uint32_t Roundup(uint32_t n, uint32_t base) { return (n / base + 1) * base; }

Status RouterV1::GetPartitionId(const std::string& key, uint64_t* partition_id) const {
    uint32_t slot_id = GetSlotId(key);
    if (partition_map_.size() < 1) {
        LOG_WARNING("Partition not exist").put("Key", key).put("Slot", slot_id);
        return Status::Internal("Slot not found");
    }
    // ! assumption here: slots are even distributed across partitions
    uint32_t slot_num_in_partition = kSlotCount / partition_map_.size();
    uint32_t big_size = (kSlotCount % partition_map_.size()) * (slot_num_in_partition + 1);
    uint32_t end_slot_id;
    if (slot_id >= big_size) {
        end_slot_id = Roundup((slot_id - big_size), slot_num_in_partition) + big_size;
    } else {
        end_slot_id = Roundup(slot_id, slot_num_in_partition + 1);
    }
    auto iter = slot_map_.find(end_slot_id);
    if (iter != slot_map_.end()) {
        *partition_id = iter->second;
        return Status::OK();
    }
    LOG_WARNING("Partition id not found").put("Key", key).put("Slot", slot_id);
    return Status::Internal("Slot not found");
}

Status RouterV1::GetServerEndpoint(const std::string& key, const PartitionPickOptions& pick_opts,
                                   std::string* endpoint, uint64_t* partition_id) const {
    Status status = GetPartitionId(key, partition_id);
    if (!status.ok()) {
        return status;
    }
    auto iter = partition_map_.find(*partition_id);
    if (iter == partition_map_.end()) {
        LOG_WARNING("Partition not found").put("Key", key).put("Partition", *partition_id);
        return Status::Internal("Partition info not found");
    }

    std::shared_ptr<Partition> partition = iter->second;
    if (pick_opts.policy == PartitionPickOptions::Policy::kPrimary ||
        partition->read_servers.empty()) {
        *endpoint = partition->write_server;
        return Status::OK();
    }

    partition->round_robin_index =
        (partition->round_robin_index + 1) % partition->read_servers.size();
    *endpoint = partition->read_servers[partition->round_robin_index];
    return Status::OK();
}

Status RouterV1::UpdatePartition(const GetTableTopoResponse& topo) {
    slot_map_.clear();
    partition_map_.clear();
    version_ = topo.topo_version();

    for (auto iter = topo.partitions().begin(); iter < topo.partitions().end(); ++iter) {
        std::shared_ptr<Partition> partition(new Partition());
        partition->round_robin_index = 0;
        partition->id = iter->partition_id();
        partition->begin_slot_id = iter->start_slot();
        partition->end_slot_id = iter->end_slot();

        for (auto server_iter = iter->load_infos().begin(); server_iter < iter->load_infos().end();
             ++server_iter) {
            std::string endpoint = server_iter->host() + ":" + std::to_string(server_iter->port());
            if (server_iter->is_master()) {
                partition->write_server = endpoint;
            } else {
                partition->read_servers.emplace_back(endpoint);
            }
        }
        if (partition->write_server.empty()) {
            LOG_WARNING("Partition no master").put("Partition", partition->id);
            return Status::Internal("Partition no master");
        }
        slot_map_.insert(std::make_pair(partition->end_slot_id, partition->id));
        partition_map_.insert(std::make_pair(partition->id, std::move(partition)));
    }
    return Status::OK();
}

}  // namespace client
}  // namespace bcache2
