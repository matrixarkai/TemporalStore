// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/macros.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "brpc/channel.h"
#include "common/controller.h"
#include "protocol/info.pb.h"
#include "protocol/server.pb.h"
#include "stream/stream.h"

namespace bcache2 {
namespace partition {

class Partition;

// Read-only stream implementation used by secondary partitions. Metadata still
// comes from GetInfo/RestoreInfo; stream payloads are pulled from the primary.
// This must cover index, oplog, and page-zone streams. Oplog only contains
// recent mutations; dumped historical pages are read through PARTITION_STREAM_PAGE
// from the current primary's local/shared backing store.
class RemotePartitionStream : public stream::Stream {
 public:
    struct Options {
        Partition* partition = nullptr;
        PartitionStreamKind stream_kind = PARTITION_STREAM_INDEX;
        uint32_t zone_id = 0;
    };

    explicit RemotePartitionStream(const Options& options);
    ~RemotePartitionStream() override {}

    Status Load() override;

    void Append(Controller* ctrl, const void* data, size_t size, uint64_t* id,
                Closure<void>* callback) override;
    void AppendV(std::vector<std::string> data, uint64_t* id) override;
    void Append(std::string data, uint64_t* id) override;
    void Commit(Controller* ctrl, Closure<void>* callback) override;
    void Truncate(uint64_t id) override;

    void Read(Controller* ctrl, uint64_t id, void* data, size_t size,
              Closure<void>* callback) override;
    stream::ScopedIterator NewIterator(size_t start_id, size_t end_id) override;

    stream::Stats Stat() override;
    void Close(Closure<void>* callback) override;

    void UpdateConfig(const StreamConfig& config) override {}

    StreamInfo GetInfo() override { return stream_info_; }
    Status RestoreInfo(const StreamInfo& info) override;
    void ReapMetrics() const override {}

 private:
    class RemoteIterator;

    Status EnsureChannel();
    uint64_t PrimaryPartitionId() const;

    Options options_;
    std::unique_ptr<brpc::Channel> channel_;
    std::string remote_ip_;
    uint16_t remote_port_ = 0;
    uint64_t primary_partition_id_ = 0;
    StreamInfo stream_info_;

    DISALLOW_COPY_AND_ASSIGN(RemotePartitionStream);
};

}  // namespace partition
}  // namespace bcache2
