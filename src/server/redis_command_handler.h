// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <brpc/closure_guard.h>
#include <brpc/redis.h>

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "brpc/policy/redis_protocol.h"
#include "common/logging.h"
#include "server/partition_manager.h"
#include "server/service.h"

namespace bcache2 {
namespace server {

class RedisServiceImpl;
class RedisClientContext;
class RedisCommandHandler;

class RedisCommand {
 public:
    enum class CmdType {
        kInfo,
        kAuth,
        kPing,
        kBgSave,
        kConfig,
        kPSlotAdd,
        kPSlotDel,
        kPSlotInfo,
        kPSlotMigrate,
        kPSlotCountKeysInSlot,
        kPSlotGetKeysInSlot,
        kPSlotSetState,
        kPSlotImport,
        kPSlotSetVersion,
        kPSlotHashKey,
        kSlaveOf,
        kPauseWrite,
        kFlushAll,
        kPartition,
    };

    enum CmdFlag {
        kWrite = 1 << 0,
        kReadonly = 1 << 1,
        kDenyoom = 1 << 2,
        kModule = 1 << 3,
        kAdmin = 1 << 4,
        kPubsub = 1 << 5,
        kNoscript = 1 << 6,
        kRandom = 1 << 7,
        kSortForScript = 1 << 8,
        kLoading = 1 << 9,
        kStale = 1 << 10,
        kSkipMonitor = 1 << 11,
        kAsking = 1 << 12,
        kFast = 1 << 13,
        kModuleGetkeys = 1 << 14,
        kModuleNoCluster = 1 << 15,
        kPslot = 1 << 16,
        kNoLimit = 1 << 17,
    };

    RedisCommand(const std::string& name, CmdType cmd_type, int artiy, const std::string& sflags,
                 int firstkey, int lastkey, int keystep);

    std::string GetName() { return name_; }
    int GetArtiy() { return artiy_; }
    const CmdType GetCmdType() { return cmd_type_; }

 private:
    std::string name_;
    CmdType cmd_type_;
    int artiy_ = -1; /* 参数的个数，正数代表确定几个，负数表示至少几个，包含命令自身 */
    int flags_ = 0;    /* The actual flags, obtained from the 'sflags' field. */
    int firstkey_ = 0; /* The first argument that's a key (0 = no keys) */
    int lastkey_ = 0;  /* The last argument that's a key */
    int keystep_ = 0;  /* The step between first and last key */
};

class RedisClientContext {
 public:
    RedisClientContext(const std::vector<std::string>& args, brpc::RedisReply* reply)
        : reply(reply), args_(args) {}
    brpc::Controller* ctrl = nullptr;
    brpc::RedisReply* reply = nullptr;

    size_t ArgSize() { return args_.size(); }
    int64_t IntArg(int index) { return std::stoull(args_[index]); }
    std::string StrArg(int index) { return args_[index]; }
    void SetGdprAuth(bool) {}

 private:
    const std::vector<std::string>& args_;
};

class RedisCommandHandler : public brpc::RedisCommandHandler {
 public:
    RedisCommandHandler(const RedisCommand& command, RedisServiceImpl* redis_service,
                        std::function<void(RedisCommandHandler*, RedisClientContext*)> handler);

    brpc::RedisCommandHandler::Result Run(const std::vector<const char*>& args,
                                          brpc::RedisReply* output,
                                          bool /*flush_batched*/) override;

    // each function down below handles a command
    void Ping(RedisClientContext* c);
    void Config(RedisClientContext* c);
    void ConfigSet(RedisClientContext* c);
    void SlaveOf(RedisClientContext* c);
    void Info(RedisClientContext* c);
    void Partition(RedisClientContext* c);
    void PartitionLoad(RedisClientContext* c);
    void PartitionUnload(RedisClientContext* c);
    void Auth(RedisClientContext* c);

 private:
    RedisCommand command_;
    std::function<void(RedisCommandHandler*, RedisClientContext*)> handler_;
    PartitionManager* partition_manager_ = nullptr;
    RedisServiceImpl* redis_service_ = nullptr;
};

}  // namespace server
}  // namespace bcache2
