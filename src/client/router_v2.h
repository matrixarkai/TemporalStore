// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "client/client.h"
#include "client/router.h"
#include "common/status.h"
#include "protocol/master.pb.h"

namespace bcache2 {
namespace client {

class RouterV2 : public Router {
 public:
    // TODO(wuzhenyu) policy of partition picker:
    //  1) pin vdc/vau
    //  2) round-robin
    //  3) partition weighted
 private:
    struct Partition {
        uint64_t id = 0;
        bool is_primary = false;
        Endpoint endpoint;
        std::string endpoint_str;
        Location location;
    };

    struct PartitionSet {
        uint64_t id = 0;
        uint64_t start_slot_id = 0;
        uint64_t end_slot_id = 0;

        Partition primary;

        int round_robin_index = 0;
        std::vector<Partition> secondaries;
    };

 public:
    Status GetServerEndpoint(const std::string& key, const PartitionPickOptions& pick_opts,
                             std::string* endpoint, uint64_t* partition_id) const override;
    uint64_t GetVersion() const override { return version_; }
    Status UpdatePartition(const GetTableTopoResponse& topo) override;

    void SetAddressFamily(AddressFamily af) { af_ = af; }

 private:
    Status GetPartitionSetId(const std::string& key, uint64_t* pset_id) const;
    std::string Endpoint2String(const Endpoint& ep);

 private:
    uint64_t version_ = 0;
    // start_slot_id : partition_id
    std::map<uint32_t, uint64_t> slot_map_;  // TODO(wuzhenyu) make access O(1) complexity
    std::unordered_map<uint64_t, std::shared_ptr<PartitionSet>> partition_map_;
    AddressFamily af_;
};

}  // namespace client
}  // namespace bcache2
