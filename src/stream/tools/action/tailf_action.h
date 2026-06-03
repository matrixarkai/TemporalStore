// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <inttypes.h>

#include <string>
#include <vector>

#include "partition/condition.h"
#include "stream/log_based_util.h"
#include "stream/store_layer.h"
#include "stream/tools/action/action.h"
#include "stream/tools/flags.h"
#include "stream/tools/utils.h"

namespace bcache2 {
namespace stream {
namespace tool {

class TailfAction : public Action {
 public:
    TailfAction(Stream* stream, partition::ConditionInfoObserver* condition_os,
                const std::string& uri, DataSchema schema)
        : stream_(stream), condition_os_(condition_os), uri_(uri), schema_(schema) {}
    ~TailfAction() {}

    Status Run() override {
        uint64_t offset = stream_->Stat().persistent_length;
        if (FLAGS_verbose) {
            printf("#Start Offset: %" PRIu64 "\n", offset);
        }

        PrintSchemaTitle(schema_);

        stream::ScopedIterator iter = stream_->NewIterator(offset, UINT64_MAX);
        while (true) {
            Status status = iter->Next();
            if (!status.ok() && !status.IsOutOfRange()) {
                if (FLAGS_verbose) {
                    printf("#Iterate failed: %s", status.ToString().c_str());
                }
                sleep(1);
                continue;
            }

            if (status.IsOutOfRange()) {
                if (GetCurrentTimeInMs() - last_update_stream_ms_ < 1000) {
                    // backoff
                    sleep(1);
                    continue;
                }

                StreamInfo stream_info;
                status = GetStreamInfo(condition_os_->RemoteIpStr(), condition_os_->RemotePort(),
                                       uri_, condition_os_->PartitionId(), &stream_info);
                if (status.ok()) {
                    stream_->RestoreInfo(stream_info);
                    last_update_stream_ms_ = GetCurrentTimeInMs();
                    continue;
                }

                // failed
                if (FLAGS_verbose) {
                    printf("#Get stream info failed: %s", status.ToString().c_str());
                }
                sleep(1);
                continue;
            }

            absl::string_view data = iter->Data();
            PrintSchema(schema_, data);
        }

        // never return
        return Status::OK();
    }

 private:
    Stream* stream_ = nullptr;
    partition::ConditionInfoObserver* condition_os_ = nullptr;
    std::string uri_;
    DataSchema schema_;
    uint64_t last_update_stream_ms_ = 0;
};

}  // namespace tool
}  // namespace stream
}  // namespace bcache2
