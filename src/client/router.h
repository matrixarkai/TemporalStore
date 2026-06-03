// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "byte/algorithm/crc64.h"

#include "client/client.h"
#include "client/command.h"
#include "common/cmd_manager.h"
#include "common/status.h"
#include "protocol/master.pb.h"

namespace bcache2 {
namespace client {

class Router {
 public:
    virtual ~Router() {}
    virtual uint64_t GetVersion() const = 0;
    virtual Status UpdatePartition(const GetTableTopoResponse& topo) = 0;
    virtual Status GetServerEndpoint(const std::string& key, const PartitionPickOptions& pick_opts,
                                     std::string* endpoint, uint64_t* partition_id) const = 0;
};

static inline uint64_t GetSlotId(const std::string& key) {
    return byte::crc64_signed(0, key.data(), key.size()) >> 34;
}

// key -> BCache2 Server
class RouterV1 : public Router {
 public:
    struct Partition {
        uint64_t id = 0;
        uint64_t begin_slot_id = 0;
        uint64_t end_slot_id = 0;
        int round_robin_index = 0;
        std::vector<std::string> read_servers;
        std::string write_server;
    };

 public:
    RouterV1() {}
    virtual ~RouterV1() {}

    Status GetServerEndpoint(const std::string& key, const PartitionPickOptions& pick_opts,
                             std::string* endpoint, uint64_t* partition_id) const override;
    uint64_t GetVersion() const override { return version_; }
    Status UpdatePartition(const GetTableTopoResponse& topo) override;

 private:
    Status GetPartitionId(const std::string& key, uint64_t* partition_id) const;

 private:
    static const uint32_t kSlotCount = 1U << 30;

    uint64_t version_ = 0;
    // end_slot_id : partition_id
    std::unordered_map<uint64_t, uint64_t> slot_map_;
    // will be used with brpc DoublyBufferedData, unique_ptr not supported
    std::unordered_map<uint64_t, std::shared_ptr<Partition>> partition_map_;
};

}  // namespace client
}  // namespace bcache2
