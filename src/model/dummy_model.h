// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/container/btree_map.h>
#include <byte/include/assert.h>

#include <list>
#include <string>
#include <utility>
#include <vector>

#include "common/allocator.h"
#include "common/ring_array.h"
#include "common/status.h"
#include "partition/storage/page_store.h"
#include "partition/storage/slot_context_manager.h"

namespace bcache2 {
namespace model {

// dummy model, for test and demonstrates how model api looks like
class DummyModel {
 public:
    Status Init(Allocator* allocator, std::vector<partition::PageInfo> pages) {
        return Status::OK();
    }

    static Status BlindDumpNewPages(const std::vector<partition::PageIndex>& pages,
                                    const std::vector<partition::LogItem>& logs,
                                    std::vector<std::pair<uint16_t, std::string>>* new_pages) {
        return Status::OK();
    }

    // only new/dirty pages will be dumped here
    // this can help reduce the number of pages to write
    std::vector<std::pair<uint16_t, std::string>> DumpNewPages(
        const std::vector<partition::PageIndex>& pages,
        const std::vector<partition::LogItem>& logs) {
        return {};
    }

    static std::pair<std::vector<partition::PageIndex>, bool> CompactPagesHint(
        std::vector<partition::PageIndex> pages) {
        return {};
    }

    static std::vector<partition::PageInfo> CompactPages(
        const std::vector<partition::PageInfo>& pages, bool remove_tombstone) {
        return {};
    }

    // everything should be represented as a KV model
    // so every change is just a KV pair
    Status Apply(Allocator* allocator, std::string key, std::string value, uint64_t cluster_id,
                 uint64_t timestamp, bool deleted) {
        return Status::OK();
    }
    // for conflict resolution
    Status Merge(Allocator* allocator, std::string key, std::string value, uint64_t cluster_id,
                 uint64_t timestamp, bool deleted) {
        return Status::OK();
    }

    void Set(std::string key, std::string value) { map_[key] = value; }
    std::string Get(std::string key) {
        auto it = map_.find(key);
        return it == map_.end() ? "" : it->second;
    }
    size_t Size() { return map_.size(); }

 private:
    absl::btree_map<std::string, std::string> map_;
};

}  // namespace model
}  // namespace bcache2
