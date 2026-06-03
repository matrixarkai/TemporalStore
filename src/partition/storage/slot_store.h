// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/container/btree_map.h>
#include <byte/base/closure.h>
#include <byte/include/macros.h>
#include <byte/string/format.h>
#include <stdint.h>

#include <iomanip>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/metrics.h"
#include "common/time.h"
#include "common/time_tracer.h"
#include "partition/index/index.h"
#include "partition/storage/page_store.h"

namespace bcache2 {

class Controller;

namespace partition {

class Index;
class PageStore;
class OpLogger;
class Partition;
class ObjectManager;

// slot and pages
class SlotStore {
 public:
    SlotStore(Partition* partition, Index* index, PageStore* page_store, OpLogger* op_logger,
              SlotContextManager* slot_context_manager, MetricsManager* metrics_manager);

    // FIXME(wangtai.10): fix dependency cycle
    void SetObjectManager(ObjectManager* object_manager) { object_manager_ = object_manager; }

    void DumpSlots(Controller* ctrl, std::vector<uint64_t> slots, Closure<void>* callback);
    void DumpPages(Controller* ctrl, std::vector<PageInfo>* pages,

                   Closure<void>* callback);  // we'll fill page_size and page_address for each page
    void LoadSlot(Controller* ctrl, uint64_t slot_id,
                  std::map<std::string, ObjectPages>* object_pages, Closure<void>* callback);
    void LoadPages(Controller* ctrl, const std::vector<SlotPageIndex>& indexes,
                   std::vector<PageInfo>* pages, Closure<void>* callback);

 private:
    // for DumpSlots
    struct DumpSlotTask {
        SlotNode* slot = nullptr;
        uint64_t slot_id = 0;
        uint64_t last_oplog_sequence = 0;
        std::vector<PageInfo> pages;
        std::vector<bool> creation;  // creation[i] indicates if pages[i] is new page
        std::vector<storage::IndexLog::ObjectItem> metas;
    };
    struct DumpSlotsContext {
        Controller* ctrl = nullptr;
        Closure<void>* callback = nullptr;
        std::vector<std::unique_ptr<DumpSlotTask>> dump_tasks;
        size_t finish_slot = 0;
        size_t total_slot = 0;
        bool oplog_committed = false;
        bool page_committed = false;
        TimeTracer tracer;
    };

    // for LoadPages
    struct LoadPagesContext {
        Controller* ctrl = nullptr;
        std::vector<SlotPageIndex> indexes;
        std::vector<Controller> ctrls;
        std::vector<PageInfo>* pages = nullptr;
        size_t complete_count = 0;
        Closure<void>* callback = nullptr;
    };

    // for LoadSlot
    struct LoadSlotContext {
        uint64_t slot_id = 0;
        Controller* ctrl = nullptr;
        std::map<std::string, ObjectPages>* object_pages = nullptr;
        Closure<void>* callback = nullptr;
        std::vector<SlotPageIndex> indexes;
        std::vector<PageInfo> pages;
        int call_count = 0;
        int complete_call_count = 0;
        TimeCost cost;
    };

    void DumpSlotInMemory(DumpSlotsContext* context, DumpSlotTask* task);
    void DumpSlotNotInMemory(DumpSlotsContext* context, DumpSlotTask* task);
    void OnLoadObjectDone(Controller* ctrl, uint64_t slot_id, Closure<void>* callback);
    void OnDumpSlotDone(DumpSlotsContext* context, DumpSlotTask* task);
    void OnCommitDone(Controller* ctrl, DumpSlotsContext* context, bool oplog);
    void UpdateIndex(DumpSlotsContext* context, DumpSlotTask* task);
    void OnLoadSlotDone(LoadSlotContext* context);
    void OnLoadPagesDone(LoadPagesContext* context, size_t index);

    Partition* partition_ = nullptr;
    Index* index_ = nullptr;
    PageStore* page_store_ = nullptr;
    OpLogger* op_logger_ = nullptr;
    ObjectManager* object_manager_ = nullptr;
    SlotContextManager* slot_context_manager_ = nullptr;

    SlotStoreMetrics metrics_;

    DISALLOW_COPY_AND_ASSIGN(SlotStore);
};

}  // namespace partition
}  // namespace bcache2
