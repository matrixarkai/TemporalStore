// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/assert.h>

#include <memory>
#include <string>
#include <vector>

#include "client/metrics.h"
#include "common/metrics.h"
#include "common/status.h"

namespace bcache2 {
namespace client {

class Command {
 public:
    struct Options {
        MetricsManager* metrics_manager = nullptr;
    };
    enum class OpType {
        kOpHashGet,
        kOpHashSet,
        kOpHashDel,
        kOpFeatureAdd,
        kOpFeatureQuery,
        kOpStringGet,
        kOpStringSet,
        kOpStringSetEx,
        kOpDel,
        kOpExpire,
        kOpTtl,
    };

    enum class OpRwFlag {
        kOpRead,
        kOpWrite,
        kOpAdmin,
    };

    enum class OpKeyFlag {
        kOpNormal,
        kOpMulti,
        kOpBroadcast,
    };

    Command(OpType type, OpKeyFlag key_flag, OpRwFlag rw_flag, const std::string& redis_name,
            const Options& options)
        : type_(type),
          key_flag_(key_flag),
          rw_flag_(rw_flag),
          redis_name_(redis_name),
          options_(options),
          metrics_(options.metrics_manager, kMetricsCmdRequest, {{"cmd", redis_name}}) {}
    ~Command() {}

    bool IsRead() const { return rw_flag_ == OpRwFlag::kOpRead; }
    bool IsWrite() const { return rw_flag_ == OpRwFlag::kOpWrite; }
    bool IsSingleKey() const { return key_flag_ == OpKeyFlag::kOpNormal; }

    OpType GetOpType() const { return type_; }

    RequestMetrics* GetMetrics() { return &metrics_; }

 private:
    OpType type_ = OpType::kOpHashGet;
    OpKeyFlag key_flag_ = OpKeyFlag::kOpNormal;
    OpRwFlag rw_flag_ = OpRwFlag::kOpRead;
    std::string redis_name_;
    Options options_;

    RequestMetrics metrics_;

    DISALLOW_COPY_AND_ASSIGN(Command);
};

class CommandManager {
 public:
    struct Options {
        MetricsManager* metrics_manager = nullptr;
    };

    explicit CommandManager(const Options& options) {
        options_ = options;
        BYTE_ASSERT(options_.metrics_manager != nullptr);
        Command::Options command_options;
        command_options.metrics_manager = options_.metrics_manager;
        commands_.emplace_back(new Command(Command::OpType::kOpHashGet,
                                           Command::OpKeyFlag::kOpNormal,
                                           Command::OpRwFlag::kOpRead, "hget", command_options));
        commands_.emplace_back(new Command(Command::OpType::kOpHashSet,
                                           Command::OpKeyFlag::kOpNormal,
                                           Command::OpRwFlag::kOpWrite, "hset", command_options));
        commands_.emplace_back(new Command(Command::OpType::kOpHashDel,
                                           Command::OpKeyFlag::kOpNormal,
                                           Command::OpRwFlag::kOpWrite, "hdel", command_options));
        commands_.emplace_back(
            new Command(Command::OpType::kOpFeatureAdd, Command::OpKeyFlag::kOpNormal,
                        Command::OpRwFlag::kOpWrite, "feature_add", command_options));
        commands_.emplace_back(
            new Command(Command::OpType::kOpFeatureQuery, Command::OpKeyFlag::kOpNormal,
                        Command::OpRwFlag::kOpRead, "feature_query", command_options));
        commands_.emplace_back(new Command(Command::OpType::kOpStringGet,
                                           Command::OpKeyFlag::kOpNormal,
                                           Command::OpRwFlag::kOpRead, "get", command_options));
        commands_.emplace_back(new Command(Command::OpType::kOpStringSet,
                                           Command::OpKeyFlag::kOpNormal,
                                           Command::OpRwFlag::kOpWrite, "set", command_options));
        commands_.emplace_back(new Command(Command::OpType::kOpStringSetEx,
                                           Command::OpKeyFlag::kOpNormal,
                                           Command::OpRwFlag::kOpWrite, "setex", command_options));
        commands_.emplace_back(new Command(Command::OpType::kOpDel, Command::OpKeyFlag::kOpNormal,
                                           Command::OpRwFlag::kOpWrite, "del", command_options));
        commands_.emplace_back(new Command(Command::OpType::kOpExpire,
                                           Command::OpKeyFlag::kOpNormal,
                                           Command::OpRwFlag::kOpWrite, "expire", command_options));
        commands_.emplace_back(new Command(Command::OpType::kOpTtl, Command::OpKeyFlag::kOpNormal,
                                           Command::OpRwFlag::kOpRead, "ttl", command_options));
        for (size_t i = 0; i < commands_.size(); ++i) {
            BYTE_ASSERT(static_cast<size_t>(commands_[i]->GetOpType()) == i);
        }
    }
    virtual ~CommandManager() {}

    Command* GetCommand(Command::OpType type) const {
        BYTE_ASSERT(static_cast<uint32_t>(type) < commands_.size());
        return commands_[static_cast<uint32_t>(type)].get();
    }

 private:
    Options options_;
    std::vector<std::unique_ptr<Command>> commands_;

    DISALLOW_COPY_AND_ASSIGN(CommandManager);
};

}  // namespace client
}  // namespace bcache2
