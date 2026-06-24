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
    RegisterCommand("echo", RedisCommand::CmdType::kEcho, 2, "tF", 0, 0, 0,
                    &RedisCommandHandler::Echo);
    RegisterCommand("quit", RedisCommand::CmdType::kQuit, 1, "tF", 0, 0, 0,
                    &RedisCommandHandler::Quit);
    RegisterCommand("client", RedisCommand::CmdType::kClient, -2, "rF", 0, 0, 0,
                    &RedisCommandHandler::Client);
    RegisterCommand("command", RedisCommand::CmdType::kCommand, -1, "r", 0, 0, 0,
                    &RedisCommandHandler::Command);
    RegisterCommand("select", RedisCommand::CmdType::kSelect, 2, "lF", 0, 0, 0,
                    &RedisCommandHandler::Select);
    RegisterCommand("bgsave", RedisCommand::CmdType::kBgSave, -1, "as", 0, 0, 0,
                    &RedisCommandHandler::Unsupported);
    RegisterCommand("config", RedisCommand::CmdType::kConfig, -2, "last", 0, 0, 0,
                    &RedisCommandHandler::Config);
    RegisterCommand("pslotadd", RedisCommand::CmdType::kPSlotAdd, -2, "wc", 0, -1, 1,
                    &RedisCommandHandler::Unsupported);
    RegisterCommand("pslotdel", RedisCommand::CmdType::kPSlotDel, -1, "wc", 0, -1, 1,
                    &RedisCommandHandler::Unsupported);
    RegisterCommand("pslotinfo", RedisCommand::CmdType::kPSlotInfo, -1, "rFc", 0, 0, 0,
                    &RedisCommandHandler::Unsupported);
    RegisterCommand("pslotmigrate", RedisCommand::CmdType::kPSlotMigrate, -2, "wc", 0, -1, 1,
                    &RedisCommandHandler::Unsupported);
    RegisterCommand("pslotcountkeysinslot", RedisCommand::CmdType::kPSlotCountKeysInSlot, 2, "rFc",
                    0, -1, 1, &RedisCommandHandler::Unsupported);
    RegisterCommand("pslotgetkeysinslot", RedisCommand::CmdType::kPSlotGetKeysInSlot, 3, "rc", 0, 0,
                    0, &RedisCommandHandler::Unsupported);
    RegisterCommand("pslotsetstate", RedisCommand::CmdType::kPSlotSetState, -2, "wc", 0, -1, 1,
                    &RedisCommandHandler::Unsupported);
    RegisterCommand("pslotimport", RedisCommand::CmdType::kPSlotImport, -2, "wc", 0, -1, 1,
                    &RedisCommandHandler::Unsupported);
    RegisterCommand("pslotsetversion", RedisCommand::CmdType::kPSlotSetVersion, -2, "wc", 0, -1, 1,
                    &RedisCommandHandler::Unsupported);
    RegisterCommand("pslothashkey", RedisCommand::CmdType::kPSlotHashKey, -1, "rFc", 0, 0, 0,
                    &RedisCommandHandler::Unsupported);
    RegisterCommand("slaveof", RedisCommand::CmdType::kSlaveOf, -3, "ast", 0, 0, 0,
                    &RedisCommandHandler::SlaveOf);
    RegisterCommand("pausewrite", RedisCommand::CmdType::kPauseWrite, 2, "as", 0, 0, 0,
                    &RedisCommandHandler::Unsupported);
    RegisterCommand("flushall", RedisCommand::CmdType::kFlushAll, -1, "wn", 0, 0, 0,
                    &RedisCommandHandler::Unsupported);
    RegisterCommand("partition", RedisCommand::CmdType::kPartition, -3, "aw", 0, 0, 0,
                    &RedisCommandHandler::Partition);
    RegisterCommand("type", RedisCommand::CmdType::kType, 2, "rF", 1, 1, 1,
                    &RedisCommandHandler::Type);
    RegisterCommand("get", RedisCommand::CmdType::kGet, 2, "rF", 1, 1, 1,
                    &RedisCommandHandler::Get);
    RegisterCommand("set", RedisCommand::CmdType::kSet, -3, "wm", 1, 1, 1,
                    &RedisCommandHandler::Set);
    RegisterCommand("setnx", RedisCommand::CmdType::kSetNx, 3, "wm", 1, 1, 1,
                    &RedisCommandHandler::SetNx);
    RegisterCommand("setex", RedisCommand::CmdType::kSetEx, 4, "wm", 1, 1, 1,
                    &RedisCommandHandler::SetEx);
    RegisterCommand("psetex", RedisCommand::CmdType::kPSetEx, 4, "wm", 1, 1, 1,
                    &RedisCommandHandler::PSetEx);
    RegisterCommand("getset", RedisCommand::CmdType::kGetSet, 3, "wm", 1, 1, 1,
                    &RedisCommandHandler::GetSet);
    RegisterCommand("getdel", RedisCommand::CmdType::kGetDel, 2, "w", 1, 1, 1,
                    &RedisCommandHandler::GetDel);
    RegisterCommand("mget", RedisCommand::CmdType::kMGet, -2, "rF", 1, -1, 1,
                    &RedisCommandHandler::MGet);
    RegisterCommand("mset", RedisCommand::CmdType::kMSet, -3, "wm", 1, -1, 2,
                    &RedisCommandHandler::MSet);
    RegisterCommand("del", RedisCommand::CmdType::kDel, -2, "w", 1, -1, 1,
                    &RedisCommandHandler::Del);
    RegisterCommand("unlink", RedisCommand::CmdType::kUnlink, -2, "wF", 1, -1, 1,
                    &RedisCommandHandler::Del);
    RegisterCommand("exists", RedisCommand::CmdType::kExists, -2, "rF", 1, -1, 1,
                    &RedisCommandHandler::Exists);
    RegisterCommand("expire", RedisCommand::CmdType::kExpire, 3, "wF", 1, 1, 1,
                    &RedisCommandHandler::Expire);
    RegisterCommand("pexpire", RedisCommand::CmdType::kPExpire, 3, "wF", 1, 1, 1,
                    &RedisCommandHandler::PExpire);
    RegisterCommand("ttl", RedisCommand::CmdType::kTtl, 2, "rF", 1, 1, 1,
                    &RedisCommandHandler::Ttl);
    RegisterCommand("pttl", RedisCommand::CmdType::kPTtl, 2, "rF", 1, 1, 1,
                    &RedisCommandHandler::PTtl);
    RegisterCommand("persist", RedisCommand::CmdType::kPersist, 2, "wF", 1, 1, 1,
                    &RedisCommandHandler::Persist);
    RegisterCommand("append", RedisCommand::CmdType::kAppend, 3, "wm", 1, 1, 1,
                    &RedisCommandHandler::Append);
    RegisterCommand("strlen", RedisCommand::CmdType::kStrlen, 2, "rF", 1, 1, 1,
                    &RedisCommandHandler::Strlen);
    RegisterCommand("incr", RedisCommand::CmdType::kIncrBy, 2, "wm", 1, 1, 1,
                    &RedisCommandHandler::IncrBy);
    RegisterCommand("incrby", RedisCommand::CmdType::kIncrBy, 3, "wm", 1, 1, 1,
                    &RedisCommandHandler::IncrBy);
    RegisterCommand("decr", RedisCommand::CmdType::kIncrBy, 2, "wm", 1, 1, 1,
                    &RedisCommandHandler::IncrBy);
    RegisterCommand("decrby", RedisCommand::CmdType::kIncrBy, 3, "wm", 1, 1, 1,
                    &RedisCommandHandler::IncrBy);
    RegisterCommand("hset", RedisCommand::CmdType::kHSet, -4, "wm", 1, 1, 1,
                    &RedisCommandHandler::HSet);
    RegisterCommand("hmset", RedisCommand::CmdType::kHSet, -4, "wm", 1, 1, 1,
                    &RedisCommandHandler::HSet);
    RegisterCommand("hget", RedisCommand::CmdType::kHGet, 3, "rF", 1, 1, 1,
                    &RedisCommandHandler::HGet);
    RegisterCommand("hmget", RedisCommand::CmdType::kHMGet, -3, "rF", 1, 1, 1,
                    &RedisCommandHandler::HMGet);
    RegisterCommand("hdel", RedisCommand::CmdType::kHDel, -3, "w", 1, 1, 1,
                    &RedisCommandHandler::HDel);
    RegisterCommand("hexists", RedisCommand::CmdType::kHExists, 3, "rF", 1, 1, 1,
                    &RedisCommandHandler::HExists);
    RegisterCommand("hlen", RedisCommand::CmdType::kHLen, 2, "rF", 1, 1, 1,
                    &RedisCommandHandler::HLen);
    RegisterCommand("hgetall", RedisCommand::CmdType::kHGetAll, 2, "r", 1, 1, 1,
                    &RedisCommandHandler::HGetAll);
    RegisterCommand("hkeys", RedisCommand::CmdType::kHKeys, 2, "r", 1, 1, 1,
                    &RedisCommandHandler::HKeys);
    RegisterCommand("hvals", RedisCommand::CmdType::kHVals, 2, "r", 1, 1, 1,
                    &RedisCommandHandler::HVals);
    RegisterCommand("hincrby", RedisCommand::CmdType::kHIncrBy, 4, "wm", 1, 1, 1,
                    &RedisCommandHandler::HIncrBy);
    RegisterCommand("sadd", RedisCommand::CmdType::kSAdd, -3, "wm", 1, 1, 1,
                    &RedisCommandHandler::SAdd);
    RegisterCommand("srem", RedisCommand::CmdType::kSRem, -3, "w", 1, 1, 1,
                    &RedisCommandHandler::SRem);
    RegisterCommand("smembers", RedisCommand::CmdType::kSMembers, 2, "rS", 1, 1, 1,
                    &RedisCommandHandler::SMembers);
    RegisterCommand("scard", RedisCommand::CmdType::kSCard, 2, "rF", 1, 1, 1,
                    &RedisCommandHandler::SCard);
    RegisterCommand("sismember", RedisCommand::CmdType::kSIsMember, 3, "rF", 1, 1, 1,
                    &RedisCommandHandler::SIsMember);
    RegisterCommand("smismember", RedisCommand::CmdType::kSMIsMember, -3, "rF", 1, 1, 1,
                    &RedisCommandHandler::SMIsMember);
    RegisterCommand("lpush", RedisCommand::CmdType::kLPush, -3, "wm", 1, 1, 1,
                    &RedisCommandHandler::LPush);
    RegisterCommand("rpush", RedisCommand::CmdType::kRPush, -3, "wm", 1, 1, 1,
                    &RedisCommandHandler::RPush);
    RegisterCommand("lpop", RedisCommand::CmdType::kLPop, -2, "w", 1, 1, 1,
                    &RedisCommandHandler::LPop);
    RegisterCommand("rpop", RedisCommand::CmdType::kRPop, -2, "w", 1, 1, 1,
                    &RedisCommandHandler::RPop);
    RegisterCommand("llen", RedisCommand::CmdType::kLLen, 2, "rF", 1, 1, 1,
                    &RedisCommandHandler::LLen);
    RegisterCommand("lindex", RedisCommand::CmdType::kLIndex, 3, "r", 1, 1, 1,
                    &RedisCommandHandler::LIndex);
    RegisterCommand("lrange", RedisCommand::CmdType::kLRange, 4, "r", 1, 1, 1,
                    &RedisCommandHandler::LRange);
    RegisterCommand("ltrim", RedisCommand::CmdType::kLTrim, 4, "w", 1, 1, 1,
                    &RedisCommandHandler::LTrim);
    RegisterCommand("zadd", RedisCommand::CmdType::kZAdd, -4, "wm", 1, 1, 1,
                    &RedisCommandHandler::ZAdd);
    RegisterCommand("zrem", RedisCommand::CmdType::kZRem, -3, "w", 1, 1, 1,
                    &RedisCommandHandler::ZRem);
    RegisterCommand("zcard", RedisCommand::CmdType::kZCard, 2, "rF", 1, 1, 1,
                    &RedisCommandHandler::ZCard);
    RegisterCommand("zscore", RedisCommand::CmdType::kZScore, 3, "rF", 1, 1, 1,
                    &RedisCommandHandler::ZScore);
    RegisterCommand("zrank", RedisCommand::CmdType::kZRank, 3, "rF", 1, 1, 1,
                    &RedisCommandHandler::ZRank);
    RegisterCommand("zrevrank", RedisCommand::CmdType::kZRevRank, 3, "rF", 1, 1, 1,
                    &RedisCommandHandler::ZRevRank);
    RegisterCommand("zrange", RedisCommand::CmdType::kZRange, -4, "r", 1, 1, 1,
                    &RedisCommandHandler::ZRange);
    RegisterCommand("zrevrange", RedisCommand::CmdType::kZRevRange, -4, "r", 1, 1, 1,
                    &RedisCommandHandler::ZRevRange);
    RegisterCommand("zrangebyscore", RedisCommand::CmdType::kZRangeByScore, -4, "r", 1, 1, 1,
                    &RedisCommandHandler::ZRangeByScore);
    RegisterCommand("zcount", RedisCommand::CmdType::kZCount, 4, "rF", 1, 1, 1,
                    &RedisCommandHandler::ZCount);
    for (const char* name : {"scan", "type", "dbsize", "multi", "exec", "discard", "watch",
                             "unwatch", "eval", "publish", "subscribe", "xadd", "xread"}) {
        RegisterCommand(name, RedisCommand::CmdType::kUnsupported, -1, "", 0, 0, 0,
                        &RedisCommandHandler::Unsupported);
    }
}

}  // namespace server
}  // namespace bcache2
