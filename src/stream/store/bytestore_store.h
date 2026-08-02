// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/base/closure.h>
#include <byte/include/assert.h>
#include <bytestore/bytestore.h>
#include <common/logging.h>

#include <memory>
#include <string>
#include <vector>

#include "common/controller.h"
#include "common/metrics.h"
#include "common/time.h"
#include "stream/metrics.h"
#include "stream/store/store.h"

namespace bcache2 {
namespace stream {

class BlobImpl : public Blob {
 public:
    BlobImpl(const std::string& uri, MetricsManager* metrics_manager, bytestore_blob* blob);
    virtual ~BlobImpl();

    void Close() override;

    void Append(Controller* ctrl, const void* data, size_t size, Closure<void>* callback) override;

    void Read(Controller* ctrl, size_t offset, void* data, size_t size,
              Closure<void>* callback) override;

 private:
    struct Task {
        Controller* ctrl = nullptr;
        Closure<void>* callback = nullptr;
        byte::AsyncThread* thread = nullptr;
        bool read = false;
        TimeCost cost;
        BlobMetrics* metrics = nullptr;
    };

    static void IoCallback(ssize_t size, struct bytestore_message* message, void* args);

    std::string uri_;
    bytestore_blob* blob_ = nullptr;
    BlobMetrics metrics_;

    DISALLOW_COPY_AND_ASSIGN(BlobImpl);
};

class ByteStoreImpl : public Store {
 public:
    ByteStoreImpl();
    virtual ~ByteStoreImpl();

    void SetCondition(Controller* ctrl, const std::string& uri, const ConditionData& data,
                      const SetConditionOptions& options) override;

    void StatCondition(Controller* ctrl, const std::string& uri, ConditionData* data) override;

    void List(Controller* ctrl, const std::string& path, std::vector<BlobInfo>* files) override;

    void Open(Controller* ctrl, const std::string& uri, const OpenOptions& options,
              Blob** blob) override;

    void Delete(Controller* ctrl, const std::string& uri, const DeleteOptions& options) override;

    void Freeze(Controller* ctrl, const std::string& uri, const FreezeOptions& options) override;

    void Stat(Controller* ctrl, const std::string& uri, const StatOptions& options,
              BlobStat* stat) override;

    void Rename(Controller* ctrl, const std::string& src_uri, const std::string& dst_uri,
                const RenameOptions& options) override;

 private:
    static void SetCondition(const Condition& condition, bytestore_blob_condition* blob_condition);

    DISALLOW_COPY_AND_ASSIGN(ByteStoreImpl);
};

}  // namespace stream
}  // namespace bcache2
