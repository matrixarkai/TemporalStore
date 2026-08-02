// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <inttypes.h>

#include <string>
#include <vector>

#include "partition/storage/zone.h"
#include "stream/log_based_stream.h"
#include "stream/tools/action/action.h"
#include "stream/tools/utils.h"

namespace bcache2 {
namespace stream {
namespace tool {

class ReadAction : public Action {
 public:
    ReadAction(Stream* stream, DataSchema schema, const std::string& uri, uint64_t address,
               size_t size)
        : stream_(stream), schema_(schema), uri_(uri), address_(address), size_(size) {}
    ~ReadAction() {}

    Status Run() override {
        if (schema_ != DataSchema::Oplog && schema_ != DataSchema::Page) {
            return Status::InvalidArgument("Invalid schema");
        }

        uint64_t zone_id = 0;
        uint64_t zone_offset = address_;
        if (schema_ == DataSchema::Page) {
            zone_id = partition::ExtractZoneId(address_);
            zone_offset = partition::ExtractZoneOffset(address_);
            std::string page_uri = "page" + std::to_string(zone_id) + "/";
            if (uri_.find(page_uri) == std::string::npos) {
                return Status::InvalidArgument("Incorrect page uri, need " + page_uri);
            }
        }

        BYTE_ASSERT(IsCoContext());
        Controller ctrl;
        std::string buffer;
        buffer.resize(size_);
        SYNC_CALL(stream_->Read, &ctrl, zone_offset, &buffer[0], size_);
        if (!ctrl.status().ok()) {
            return Status::Internal("Read failed: " + ctrl.status().ToString());
        }

        if (schema_ == DataSchema::Page) {
            storage::PageHeader header;
            uint16_t header_size = *reinterpret_cast<const uint16_t*>(buffer.data());
            BYTE_ASSERT(header.ParseFromArray(buffer.data() + sizeof(uint16_t), header_size));
            printf("%s", header.DebugString().c_str());
            printf("data: %s\n", buffer.substr(sizeof(uint16_t) + header_size).c_str());
        }
        if (schema_ == DataSchema::Oplog) {
            storage::OpLog oplog;
            BYTE_ASSERT(oplog.ParseFromArray(buffer.data(), buffer.size()));
            printf("%s\n", oplog.DebugString().c_str());
        }

        return Status::OK();
    }

 private:
    Stream* stream_ = nullptr;
    DataSchema schema_;
    std::string uri_;
    uint64_t address_ = 0;
    uint64_t size_ = 0;
};

}  // namespace tool
}  // namespace stream
}  // namespace bcache2
