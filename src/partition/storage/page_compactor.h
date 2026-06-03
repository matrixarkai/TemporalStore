// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>
#include <vector>

#include "model/model_manager.h"
#include "partition/index/index.h"
#include "partition/metrics.h"
#include "partition/storage/slot_store.h"

namespace bcache2 {
namespace partition {

class Partition;

class PageCompactor {
 public:
    PageCompactor(Partition* partition, Index* index, SlotStore* slot_store,
                  MetricsManager* metrics_manager);
    ~PageCompactor() {}

    void CompactPages(Controller* ctrl, Closure<void>* callback);

 private:
    struct CompactTask {
        uint64_t slot_id = 0;
        uint8_t model_id = 0;
        uint16_t object_id = 0;
        bool remove_tombstone = false;
        bool update_index = false;
        std::vector<SlotPageIndex> load_page_indexes;
        std::vector<PageInfo> load_pages;
        std::vector<PageInfo> dump_pages;
        Controller ctrl;
    };
    struct CompactContext {
        Controller* ctrl = nullptr;
        Closure<void>* callback = nullptr;
        std::vector<CompactTask> compact_tasks;
        size_t finish_count = 0;
    };

    void OnLoadPagesDone(CompactContext* ctx, CompactTask* task);
    void OnDumpPagesDone(CompactContext* ctx, CompactTask* task);

    Partition* partition_ = nullptr;
    Index* index_ = nullptr;
    SlotStore* slot_store_ = nullptr;
    std::unique_ptr<Index::SlotIterator> slot_iterator_;

    PageCompactionMetrics metrics_;

    DISALLOW_COPY_AND_ASSIGN(PageCompactor);
};

}  // namespace partition
}  // namespace bcache2
