// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/strings/string_view.h>
#include <byte/container/intrusive_list.h>
#include <byte/include/macros.h>
#include <byte/thread/async_thread.h>
#include <matrixobjectstore/matrixobjectstore.h>
#include <sys/uio.h>

#include <array>
#include <memory>
#include <string>
#include <vector>

#include "common/controller.h"
#include "protocol/config.pb.h"
#include "protocol/info.pb.h"
#include "stream/metrics.h"

namespace bcache2 {
namespace stream {

class Stream;

// Env manages a set of streams and conditions
class Env {
 public:
    static const size_t kInlineBlobSize = k_inline_blob_content_size;

    using ConditionData = std::array<char, kInlineBlobSize>;

    struct Condition {
        std::string name;
        ConditionData data{};
    };

    struct OpenOptions {
        StoreRepPolicy rep_policy;
        bool created = false;  // TODO(zkwu): not in use?
        bool readonly = false;
        std::string token;
        MetricsManager* metrics_manager;
    };

    struct DeleteOptions {};

    virtual ~Env() {}

    virtual void SetCondition(Controller* ctrl, const Condition& condition,
                              const std::string& condition_name,
                              const ConditionData& condition_data) = 0;

    virtual void GetCondition(Controller* ctrl, const std::string& condition_name,
                              ConditionData* condition_value) = 0;

    virtual void OpenStream(Controller* ctrl, const Condition& condition, const std::string& uri,
                            const OpenOptions& options, Stream** stream) = 0;

    virtual void DeleteStream(Controller* ctrl, const Condition& condition, const std::string& uri,
                              const DeleteOptions& options) = 0;
};

// traverse a stream
class Iterator {
 public:
    virtual ~Iterator() {}

    virtual Status Next() = 0;
    virtual uint64_t Id() const = 0;
    virtual absl::string_view Data() const = 0;
};

using ScopedIterator = std::unique_ptr<Iterator>;

struct Stats {
    uint64_t start_record_id = 0;
    uint64_t usage_bytes = 0;
    uint64_t length = 0;
    uint64_t persistent_length = 0;
};

// logically of infinite length
// Append, Read, Truncate operations
// 1. Log based impl
// 2. In memory impl (for testing)

class Stream {
 public:
    virtual ~Stream() {}

    virtual Status Load() = 0;

    virtual void Append(Controller* ctrl, const void* data, size_t size, uint64_t* id,
                        Closure<void>* callback) = 0;

    virtual void AppendV(std::vector<std::string> data, uint64_t* id) = 0;
    virtual void Append(std::string, uint64_t* id) = 0;
    virtual void Commit(Controller* ctrl, Closure<void>* callback) = 0;

    virtual void Truncate(uint64_t id) = 0;

    virtual void Read(Controller* ctrl, uint64_t id, void* data, size_t size,
                      Closure<void>* callback) = 0;

    virtual ScopedIterator NewIterator(size_t start_id, size_t end_id) = 0;

    virtual Stats Stat() = 0;
    virtual void Close(Closure<void>* callback) = 0;

    virtual void UpdateConfig(const StreamConfig& config) = 0;

    virtual StreamInfo GetInfo() = 0;
    virtual Status RestoreInfo(const StreamInfo& info) = 0;
    virtual void ReapMetrics() const = 0;
};

}  // namespace stream
}  // namespace bcache2
