// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <inttypes.h>

#include <string>
#include <vector>

#include "stream/log_based_util.h"
#include "stream/store_layer.h"
#include "stream/tools/action/action.h"
#include "stream/tools/flags.h"
#include "stream/tools/utils.h"

namespace bcache2 {
namespace stream {
namespace tool {

class ScanAction : public Action {
 public:
    ScanAction(Stream* stream, const std::string& uri, DataSchema schema)
        : stream_(stream), uri_(uri), schema_(schema) {}
    ~ScanAction() {}

    Status Run() override {
        uint64_t offset = stream_->Stat().start_record_id;
        uint64_t persistent_length = stream_->Stat().persistent_length;
        if (FLAGS_verbose) {
            printf("#Start Offset: %" PRIu64 "\n", offset);
            printf("#End Offset: %" PRIu64 "\n", persistent_length);
        }

        PrintSchemaTitle(schema_);

        stream::ScopedIterator iter = stream_->NewIterator(offset, persistent_length);
        Status status;
        while ((status = iter->Next()).ok()) {
            absl::string_view data = iter->Data();
            PrintSchema(schema_, data);
        }

        if (!status.IsOutOfRange()) {
            return Status::Internal("Error occured during iterate: " + status.ToString());
        }
        return Status::OK();
    }

 private:
    Stream* stream_ = nullptr;
    std::string uri_;
    DataSchema schema_;
};

}  // namespace tool
}  // namespace stream
}  // namespace bcache2
