// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "server/redis_service.h"

namespace bcache2 {
namespace server {

RedisServiceImpl::RedisServiceImpl(PartitionManager* partition_manager)
    : partition_manager_(partition_manager) {}

RedisServiceImpl::~RedisServiceImpl() {}

void RedisServiceImpl::RegisterCommand(
    const std::string& name, RedisCommand::CmdType cmd_type, int artiy, const std::string& sflags,
    int firstkey, int lastkey, int keystep,
    std::function<void(RedisCommandHandler*, RedisClientContext*)> handler) {
    command_holder_.emplace_back(new RedisCommandHandler(
        RedisCommand(name, cmd_type, artiy, sflags, firstkey, lastkey, keystep), this, handler));
    AddCommandHandler(name, command_holder_.back().get());
}

void RedisServiceImpl::InitCommands() {
    RegisterCommand("info", RedisCommand::CmdType::kInfo, -1, "ltR", 0, 0, 0,
                    &RedisCommandHandler::Info);
    RegisterCommand("auth", RedisCommand::CmdType::kAuth, 2, "sltF", 0, 0, 0,
                    &RedisCommandHandler::Auth);
    RegisterCommand("ping", RedisCommand::CmdType::kPing, -1, "tF", 0, 0, 0,
                    &RedisCommandHandler::Ping);
    RegisterCommand("bgsave", RedisCommand::CmdType::kBgSave, -1, "as", 0, 0, 0, nullptr);
    RegisterCommand("config", RedisCommand::CmdType::kConfig, -2, "last", 0, 0, 0,
                    &RedisCommandHandler::Config);
    RegisterCommand("pslotadd", RedisCommand::CmdType::kPSlotAdd, -2, "wc", 0, -1, 1, nullptr);
    RegisterCommand("pslotdel", RedisCommand::CmdType::kPSlotDel, -1, "wc", 0, -1, 1, nullptr);
    RegisterCommand("pslotinfo", RedisCommand::CmdType::kPSlotInfo, -1, "rFc", 0, 0, 0, nullptr);
    RegisterCommand("pslotmigrate", RedisCommand::CmdType::kPSlotMigrate, -2, "wc", 0, -1, 1,
                    nullptr);
    RegisterCommand("pslotcountkeysinslot", RedisCommand::CmdType::kPSlotCountKeysInSlot, 2, "rFc",
                    0, -1, 1, nullptr);
    RegisterCommand("pslotgetkeysinslot", RedisCommand::CmdType::kPSlotGetKeysInSlot, 3, "rc", 0, 0,
                    0, nullptr);
    RegisterCommand("pslotsetstate", RedisCommand::CmdType::kPSlotSetState, -2, "wc", 0, -1, 1,
                    nullptr);
    RegisterCommand("pslotimport", RedisCommand::CmdType::kPSlotImport, -2, "wc", 0, -1, 1,
                    nullptr);
    RegisterCommand("pslotsetversion", RedisCommand::CmdType::kPSlotSetVersion, -2, "wc", 0, -1, 1,
                    nullptr);
    RegisterCommand("pslothashkey", RedisCommand::CmdType::kPSlotHashKey, -1, "rFc", 0, 0, 0,
                    nullptr);
    RegisterCommand("slaveof", RedisCommand::CmdType::kSlaveOf, -3, "ast", 0, 0, 0,
                    &RedisCommandHandler::SlaveOf);
    RegisterCommand("pausewrite", RedisCommand::CmdType::kPauseWrite, 2, "as", 0, 0, 0, nullptr);
    RegisterCommand("flushall", RedisCommand::CmdType::kFlushAll, -1, "wn", 0, 0, 0, nullptr);
    RegisterCommand("partition", RedisCommand::CmdType::kPartition, -3, "aw", 0, 0, 0,
                    &RedisCommandHandler::Partition);
}

}  // namespace server
}  // namespace bcache2
