// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <brpc/redis.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "server/partition_manager.h"
#include "server/redis_command_handler.h"
#include "server/service.h"

namespace bcache2 {
namespace server {

class RedisServiceImpl : public brpc::RedisService {
 public:
    explicit RedisServiceImpl(PartitionManager* partition_manager);
    virtual ~RedisServiceImpl();

    void RegisterCommand(const std::string& name, RedisCommand::CmdType cmd_type, int artiy,
                         const std::string& sflags, int firstkey, int lastkey, int keystep,
                         std::function<void(RedisCommandHandler*, RedisClientContext*)> handler);
    void InitCommands();

    std::string GetConfig(const std::string& key) const { return config_map_.at(key); }
    void SetConfig(const std::string& key, const std::string& value) { config_map_[key] = value; }
    bool HasConfig(const std::string& key) const { return config_map_.count(key); }
    const std::unordered_map<std::string, std::string>& GetConfigMap() { return config_map_; }
    // these are just placeholder functions
    std::pair<std::string, std::string> GetMaster() {
        return {mock_master_host_, mock_master_port_};
    }
    void SetMaster(const std::string& host, const std::string& port) {
        mock_master_host_ = host;
        mock_master_port_ = port;
    }
    uint64_t GetLoadedPartitionId() { return loaded_partition_id_; }
    void SetLoadedPartitionId(uint64_t partition_id) { loaded_partition_id_ = partition_id; }
    PartitionManager* GetPartitionManager() { return partition_manager_; }

 private:
    std::unordered_map<std::string, std::string> config_map_ = {
        {"maxmemory", "1000"}, {"server-id", "123456789"}, {"requirepass", ""}};
    std::string mock_master_host_;
    std::string mock_master_port_;
    uint64_t loaded_partition_id_ = 0;
    PartitionManager* partition_manager_ = nullptr;
    std::vector<std::unique_ptr<RedisCommandHandler>> command_holder_;
};

}  // namespace server
}  // namespace bcache2
