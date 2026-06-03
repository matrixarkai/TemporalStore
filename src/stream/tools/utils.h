// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/strings/string_view.h>
#include <byte/include/assert.h>

#include <string>
#include <vector>

#include "brpc/channel.h"
#include "brpc/controller.h"
#include "common/status.h"
#include "protocol/info.pb.h"
#include "protocol/server.pb.h"

namespace bcache2 {
namespace stream {
namespace tool {

inline Status GetStreamInfo(const std::string& ip, int port, const std::string& stream_uri,
                            uint64_t partition_id, StreamInfo* stream_info) {
    brpc::Channel channel;
    if (channel.Init(ip.c_str(), port, nullptr) != 0) {
        return Status::InvalidArgument("Invalid remote addr");
    }

    brpc::Controller ctrl;
    ServerService_Stub stub(&channel);
    GetInfoRequest request;
    GetInfoResponse response;
    request.set_partition_id(partition_id);

    stub.GetInfo(&ctrl, &request, &response, nullptr);

    if (ctrl.Failed()) {
        return Status::DeadlineExceeded(ctrl.ErrorText());
    }

    if (response.status().code() != kOK) {
        return Status::FromRpcStatus(response.status());
    }

    if (response.partition_info().stage() != PartitionLoadStage::LOADED) {
        return Status::FailedPrecondition("partition is loading");
    }

    const PartitionInfo& remote_info = response.partition_info();
    if (remote_info.index_info().stream_info().uri() == stream_uri) {
        stream_info->CopyFrom(remote_info.index_info().stream_info());
        return Status::OK();
    }
    if (remote_info.op_logger_info().stream_info().uri() == stream_uri) {
        stream_info->CopyFrom(remote_info.op_logger_info().stream_info());
        return Status::OK();
    }
    for (auto& zone_info : remote_info.page_store_info().zones()) {
        if (zone_info.stream_info().uri() == stream_uri) {
            stream_info->CopyFrom(zone_info.stream_info());
            return Status::OK();
        }
    }

    return Status::NotFound("Uri not found");
}

enum class DataSchema {
    Oplog,
    IndexLog,
    Page,
    Unknown,
};

inline std::string SchemaToString(DataSchema schema) {
    switch (schema) {
    case DataSchema::Oplog:
        return "Oplog";
    case DataSchema::IndexLog:
        return "IndexLog";
    case DataSchema::Page:
        return "Page";
    default:
        return "Unknown";
    }
}

inline std::string TimestampToString(uint64_t timestamp_ms) {
    char buf[32];
    time_t timestamp = timestamp_ms / 1000;
    struct tm local_time;
    localtime_r(&timestamp, &local_time);
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &local_time);

    std::string ret = buf;
    snprintf(buf, sizeof(buf), "%03d", static_cast<int>(timestamp_ms % 1000));
    ret = ret + ":" + buf;
    return ret;
}

inline void PrintSchemaTitle(DataSchema schema) {
    switch (schema) {
    case DataSchema::Oplog: {
        std::vector<std::string> headers{
            "TIMESTAMP",      "SEQUENCE", "SLOT_ID",   "OBJECT_KEY", "KEY",  "VALUE", "DELETED",
            "OBJECT_DELETED", "PAGE_LOG", "OBJECT_ID", "PAGE_ID",    "PAGE", "TTL",   "META_LOG"};
        for (size_t idx = 0; idx < headers.size(); ++idx) {
            printf("%s(%" PRIu64 ")\t", headers[idx].c_str(), idx + 1);
        }
        printf("\n");
        return;
    }
    case DataSchema::IndexLog: {
        std::vector<std::string> headers{"SEQUENCE", "SLOT_ID", "OPLOG_SEQUENCE", "OBJECT_ID",
                                         "PAGE_ID",  "ADDRESS", "SIZE",           "IN_LOG",
                                         "DELETED",  "TTL",     "META_VERSION",   "START_OPLOG_ID"};
        for (size_t idx = 0; idx < headers.size(); ++idx) {
            printf("%s(%" PRIu64 ")\t", headers[idx].c_str(), idx + 1);
        }
        printf("\n");
        return;
    }
    case DataSchema::Page: {
        std::vector<std::string> headers{"SLOT_ID",       "OBJECT_ID", "PAGE_ID",
                                         "RAW_DATA_SIZE", "DATA_SIZE", "TIMESTAMP_MS",
                                         "KEY",           "MODEL_ID",  "COMPRESS"};
        for (size_t idx = 0; idx < headers.size(); ++idx) {
            printf("%s(%" PRIu64 ")\t", headers[idx].c_str(), idx + 1);
        }
        printf("\n");
        return;
    }
    default:
        BYTE_ASSERT(false);
    }
}

inline void PrintSchema(DataSchema schema, const absl::string_view& data) {
    switch (schema) {
    case DataSchema::Oplog: {
        storage::OpLog oplog;
        BYTE_ASSERT(oplog.ParseFromArray(data.data(), data.size()));
        for (auto& item : oplog.item()) {
            printf("%s\t", TimestampToString(item.timestamp_ms()).c_str());
            printf("%" PRIu64 "\t", oplog.sequence());
            printf("%" PRIu64 "\t", item.slot_id());
            printf("%s\t", item.object_key().c_str());
            printf("%s\t", item.key().c_str());
            printf("%s\t", item.value().c_str());
            printf("%s\t", item.deleted() ? "TRUE" : "FALSE");
            printf("%s\t", item.object_deleted() ? "TRUE" : "FALSE");
            printf("%s\t", item.page_log() ? "TRUE" : "FALSE");
            printf("%" PRIu32 "\t", item.object_id());
            printf("%" PRIu32 "\t", item.page_id());
            printf("%s\t", item.page().c_str());
            printf("%" PRIu64 "\t", item.ttl());
            printf("%s\t", item.meta_log() ? "TRUE" : "FALSE");
            printf("\n");
        }
        return;
    }
    case DataSchema::IndexLog: {
        storage::IndexLog index_log;
        BYTE_ASSERT(index_log.ParseFromArray(data.data(), data.size()));
        if (index_log.item_size() == 0) {
            // add empty item to print
            index_log.add_item();
        }
        for (auto& item : index_log.item()) {
            printf("%" PRIu64 "\t", index_log.sequence());
            printf("%" PRIu64 "\t", index_log.slot_id());
            printf("%" PRIu64 "\t", index_log.oplog_sequence());
            printf("%" PRIu32 "\t", item.object_id());
            printf("%" PRIu32 "\t", item.page_id());
            printf("%" PRIu64 "\t", item.address());
            printf("%" PRIu32 "\t", item.size());
            printf("%s\t", item.in_log() ? "TRUE" : "FALSE");
            printf("%s\t", item.deleted() ? "TRUE" : "FALSE");

            for (int i = 0; i < index_log.object_item_size(); ++i) {
                if (i > 0) {
                    printf(",");
                }
                printf("%" PRIu32 ":%" PRIu64, index_log.object_item(i).object_id(),
                       index_log.object_item(i).ttl());
            }
            printf("\t");

            printf("%" PRIu64 "\t", index_log.meta_item().version());
            printf("%" PRIu64 "\t", index_log.meta_item().start_oplog_id());
            printf("\n");
        }
        return;
    }
    case DataSchema::Page: {
        storage::PageHeader header;
        uint16_t header_size = *reinterpret_cast<const uint16_t*>(data.data());
        BYTE_ASSERT(header.ParseFromArray(data.data() + sizeof(uint16_t), header_size));

        printf("%" PRIu64 "\t", header.slot_id());
        printf("%" PRIu32 "\t", header.object_id());
        printf("%" PRIu32 "\t", header.page_id());
        printf("%" PRIu64 "\t", header.raw_data_size());
        printf("%" PRIu64 "\t", header.data_size());
        printf("%" PRIu64 "\t", header.timestamp_ms());
        printf("%s\t", header.key().c_str());
        printf("%" PRIu32 "\t", header.model_id());
        printf("%s\t", storage::PageHeader_CompressType_Name(header.compress()).c_str());
        printf("\n");
        return;
    }
    default:
        BYTE_ASSERT(false);
    }
}

}  // namespace tool
}  // namespace stream
}  // namespace bcache2
