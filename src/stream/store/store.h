// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/base/closure.h>

#include <string>
#include <vector>

#include "stream/metrics.h"
#include "stream/stream.h"

namespace bcache2 {

class Controller;
class MetricsManager;

namespace stream {

class Blob {
 public:
    virtual ~Blob() {}

    virtual void Close() = 0;

    virtual void Append(Controller* ctrl, const void* data, size_t size,
                        Closure<void>* callback) = 0;

    virtual void Read(Controller* ctrl, size_t offset, void* data, size_t size,
                      Closure<void>* callback) = 0;
};

class Store {
 public:
    using ConditionData = Env::ConditionData;
    using Condition = Env::Condition;

    struct BlobInfo {
        std::string name;
    };

    enum class OpenMode {
        kRead,
        kWrite,
    };

    struct BlobStat {
        size_t size = 0;
    };

    struct SetConditionOptions {
        Condition condition;
    };

    struct OpenOptions {
        OpenMode mode = OpenMode::kRead;
        StoreRepPolicy rep_policy;
        Condition condition;
        MetricsManager* metrics_manager = nullptr;
    };

    struct DeleteOptions {
        Condition condition;
    };

    struct FreezeOptions {
        Condition condition;
    };

    struct StatOptions {};

    struct RenameOptions {
        Condition condition;
    };

    virtual ~Store() {}

    virtual void SetCondition(Controller* ctrl, const std::string& uri, const ConditionData& data,
                              const SetConditionOptions& options) = 0;

    virtual void StatCondition(Controller* ctrl, const std::string& uri, ConditionData* data) = 0;

    virtual void List(Controller* ctrl, const std::string& path, std::vector<BlobInfo>* files) = 0;

    virtual void Open(Controller* ctrl, const std::string& uri, const OpenOptions& options,
                      Blob** blob) = 0;

    virtual void Delete(Controller* ctrl, const std::string& uri, const DeleteOptions& options) = 0;

    virtual void Freeze(Controller* ctrl, const std::string& uri, const FreezeOptions& options) = 0;

    virtual void Stat(Controller* ctrl, const std::string& uri, const StatOptions& options,
                      BlobStat* stat) = 0;

    virtual void Rename(Controller* ctrl, const std::string& src_uri, const std::string& dst_uri,
                        const RenameOptions& options) = 0;
};

}  // namespace stream
}  // namespace bcache2
