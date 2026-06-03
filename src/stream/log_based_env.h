// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/string/format.h>
#include <byte/thread/async_thread.h>

#include <memory>
#include <string>

#include "common/controller.h"
#include "common/metrics.h"
#include "stream/store_layer.h"
#include "stream/stream.h"

namespace bcache2 {
namespace stream {

// Nothing but seems to be another wrapper on top of StoreLayer & Log Based Streams
// Dispatch operations to StoreLayer and create Log Streams
class LogBasedEnv : public Env {
 public:
    struct Options {
        byte::AsyncThreadPool* background_pool = nullptr;
        StoreLayer* store_layer = nullptr;
    };

    LogBasedEnv();
    virtual ~LogBasedEnv();
    void Init(const Options& options);

    void SetCondition(Controller* ctrl, const Condition& condition,
                      const std::string& condition_name,
                      const ConditionData& condition_data) override;

    void GetCondition(Controller* ctrl, const std::string& condition_name,
                      ConditionData* condition_value) override;

    void OpenStream(Controller* ctrl, const Condition& condition, const std::string& uri,
                    const OpenOptions& options, Stream** stream) override;

    void DeleteStream(Controller* ctrl, const Condition& condition, const std::string& uri,
                      const DeleteOptions& options) override;

 private:
    friend class StreamBaseImpl;
    friend class StreamImpl;
    friend class StreamReaderImpl;

    Options options_;
    byte::AsyncThreadPool* background_pool_ = nullptr;
    StoreLayer* store_layer_ = nullptr;

    DISALLOW_COPY_AND_ASSIGN(LogBasedEnv);
};

}  // namespace stream
}  // namespace bcache2
