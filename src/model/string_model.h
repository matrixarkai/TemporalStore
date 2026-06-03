// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/assert.h>

#include <list>
#include <string>
#include <utility>
#include <vector>

#include "common/allocator.h"
#include "model/common.h"
#include "model/model_context.h"
#include "partition/cmd_context.h"

namespace bcache2 {
namespace model {

class StringModel {
 public:
    Status Init(Allocator* allocator, std::vector<partition::PageInfo> pages) {
        value_ = String(Allocator::StlWrapper<char>(allocator));
        // pages contain log-like data, of which only the last k-v pair matters
        for (auto& page : pages) {
            google::protobuf::io::ArrayInputStream input(page.data.data(), page.data.size());
            google::protobuf::io::CodedInputStream stream(&input);
            std::string data;
            while (stream.CurrentPosition() < static_cast<int>(page.data.size())) {
                std::string key_data;
                std::string value_data;
                uint8_t cluster_id = 0;
                bool deleted = 0;
                uint64_t timestamp = 0;
                if (!ReadKvItemFromStream(&stream, &key_data, &value_data, &cluster_id, &deleted,
                                          &timestamp)) {
                    return Status::DataLoss("Invalid kv item");
                }
                BYTE_ASSERT(!deleted);
                value_.assign(value_data.data(), value_data.size());
            }
        }
        return Status::OK();
    }

    static Status BlindDumpNewPages(const std::vector<partition::PageIndex>& pages,
                                    const std::vector<partition::LogItem>& logs,
                                    std::vector<std::pair<uint16_t, std::string>>* new_pages) {
        // changes are kept in the op log, therefore, no need to dump pages
        return Status::OK();
    }

    std::vector<std::pair<uint16_t, std::string>> DumpNewPages(
        const std::vector<partition::PageIndex>& pages, /* stock pages */
        const std::vector<partition::LogItem>& logs /* incremental logs */) {
        // changes are kept in the op log, therefore, no need to dump pages
        return {};
    }

    static std::pair<std::vector<partition::PageIndex>, bool> CompactPagesHint(
        std::vector<partition::PageIndex> pages) {
        // we keep data in single page, so no need do compaction
        return {};
    }

    static std::vector<partition::PageInfo> CompactPages(
        const std::vector<partition::PageInfo>& pages, bool remove_tombstone) {
        // we keep data in single page, so no need do compaction
        return {};
    }

    Status Apply(Allocator* allocator, std::string key, std::string value, uint64_t cluster_id,
                 uint64_t timestamp, bool deleted) {
        if (UNLIKELY(deleted)) {
            return Status::DataLoss("");
        }
        value_.assign(value.data(), value.size());  // still in memory
        return Status::OK();
    }

    Status Merge(Allocator* allocator, std::string key, std::string value, uint64_t cluster_id,
                 uint64_t timestamp, bool deleted) {
        BYTE_ASSERT(false);  // TODO(zhangyuan.42): Support CRDT
        return Status::Unimplemented("");
    }

    void SetValue(partition::CmdContext* ctx, std::string value, uint64_t ttl) {
        value_.assign(value.data(), value.size());

        // TODO(wangtai.10): better
        // create a new page, only containing this change
        std::string page;
        google::protobuf::io::StringOutputStream output(&page);
        google::protobuf::io::CodedOutputStream stream(&output);
        WriteKvItemToStream(&stream, "", value_, 0, 0, 0);
        stream.Trim();
        // page_id == 0, to reuse the same page in this slot
        if (ctx == nullptr) {
            ModelContext* ctx = GetModelContext();
            ctx->op_logger->WritePage(ctx->cmd_context, ctx->slot_id, ctx->object->Key(),
                                      ctx->object->ModelId(), ctx->object->ObjectId(), 0,
                                      std::move(page), ttl);
        } else {
            ctx->op_logger->WritePage(ctx, ctx->slot_id, ctx->object.Key(), ctx->object.ModelId(),
                                      ctx->object.ObjectId(), 0, std::move(page), ttl);
        }
    }

    const std::string GetValue() const { return std::string(value_.data(), value_.size()); }

 private:
    using String = std::basic_string<char, std::char_traits<char>, Allocator::StlWrapper<char>>;

    // FIXME(wangtai.10): copy
    String value_;
};

}  // namespace model
}  // namespace bcache2
