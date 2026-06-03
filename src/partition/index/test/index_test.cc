// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/index/index.h"

#include <gtest/gtest.h>

#include "common/metrics.h"
#include "partition/allocator_manager.h"
#include "partition/index/layout/layout.h"
#include "partition/partition.h"
#include "partition/storage/slot_context_manager.h"
#include "stream/log_based_env.h"
#include "stream/store_layer.h"
#include "test/common/temp_dir.h"

namespace bcache2 {
namespace partition {

class IndexTest : public testing::Test {
 public:
    void SetUp() override {
        byte::AsyncThreadPoolOptions tp_options;
        tp_options.name_ = "test";
        background_pool_.reset(new byte::AsyncThreadPool());
        ASSERT_TRUE(background_pool_->Init(tp_options));
        ASSERT_TRUE(background_pool_->Start());

        metrics_manager_.reset(new MetricsManager({}, ""));

        store_layer_.reset(new stream::StoreLayer(background_pool_.get()));

        env_.reset(new stream::LogBasedEnv());
        stream::LogBasedEnv::Options env_options;
        env_options.background_pool = background_pool_.get();
        env_options.store_layer = store_layer_.get();
        env_->Init(env_options);

        stream::LogBasedEnv::Condition condition;

        uri_ = "file://" + temp_dir_.GetDir() + "/public/stream/";

        stream::LogBasedEnv::OpenOptions options;
        options.token = "test";
        options.metrics_manager = metrics_manager_.get();
        Controller ctrl;
        stream::Stream* stream = nullptr;
        env_->OpenStream(&ctrl, condition, uri_, options, &stream);
        ASSERT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();
        ASSERT_TRUE(stream != nullptr);

        allocator_manager_.reset(new AllocatorManager(metrics_manager_.get()));

        slot_context_manager_.reset(
            new SlotContextManager(metrics_manager_.get(), allocator_manager_.get()));

        partition_.reset(new Partition(Partition::Options()));
        index_.reset(new Index(partition_.get(), allocator_manager_.get(),
                               slot_context_manager_.get(), metrics_manager_.get()));
        Index::Options opts;
        opts.stream = stream;
        index_->Init(opts);
        Status status = index_->Load();
        ASSERT_TRUE(status.ok()) << status;

        uint8_t kObjectCountArray[] = {1, 3};
        uint16_t kPageCountArray[] = {1, 3, 2, 6};
        for (uint64_t slot_id = 100; slot_id < 200; ++slot_id) {
            SlotNode* slot = index_->GetOrCreateSlotWithStorage(slot_id);
            slot->SetInMemory(true);
            ASSERT_NE(slot, nullptr);
            for (uint8_t object_id = 0; object_id < kObjectCountArray[slot_id % 2]; ++object_id) {
                Object object;
                slot->NewObject(Layout::WriteOptions(index_->slot_node_allocator_), 1,
                                "key" + std::to_string(object_id), &object);
                for (uint16_t page_id = 0; page_id < kPageCountArray[slot_id % 4]; ++page_id) {
                    PageInfo page;
                    page.header.set_object_id(object_id);
                    page.header.set_page_id(page_id);
                    page.address = slot_id * 10;
                    page.size = 10;
                    page.header.set_page_in_log(false);
                    index_->UpdateSlotPages(slot_id, slot_id * 10, {page});
                }
            }
            ASSERT_EQ(slot->GetObjectNum(), kObjectCountArray[slot_id % 2]);
            if (slot_id >= 150) {
                index_->MarkSlotDataDirty(slot_id, slot_id * 100, 1);
            }
        }
        SlotNode* slot = index_->GetOrCreateSlotWithStorage(200);
        ASSERT_NE(slot, nullptr);
        ASSERT_EQ(index_->slot_context_manager_->DirtySlotsNum(), 50);
    }
    void TearDown() override {
        SYNC_CALL0(index_->Close);
        background_pool_->Stop();
    }

 protected:
    void Commit() {
        Controller ctrl;
        SYNC_CALL(index_->Commit, &ctrl);
        ASSERT_TRUE(ctrl.status().ok()) << ctrl.status();
    }
    void Reload() {
        SYNC_CALL0(index_->Close);

        stream::LogBasedEnv::Condition condition;
        stream::LogBasedEnv::OpenOptions options;
        options.token = "test";
        options.metrics_manager = metrics_manager_.get();
        Controller ctrl;
        stream::Stream* stream = nullptr;
        env_->OpenStream(&ctrl, condition, uri_, options, &stream);
        ASSERT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();
        ASSERT_TRUE(stream != nullptr);

        index_.reset(new Index(partition_.get(), allocator_manager_.get(),
                               slot_context_manager_.get(), metrics_manager_.get()));
        Index::Options opts;
        opts.stream = stream;
        index_->Init(opts);
        Status status = index_->Load();
        ASSERT_TRUE(status.ok()) << status;
    }

    TempDir temp_dir_;
    std::unique_ptr<byte::AsyncThreadPool> background_pool_;
    std::unique_ptr<stream::StoreLayer> store_layer_;
    std::unique_ptr<stream::LogBasedEnv> env_;
    std::unique_ptr<MetricsManager> metrics_manager_;
    std::string uri_;
    std::unique_ptr<AllocatorManager> allocator_manager_;
    std::unique_ptr<SlotContextManager> slot_context_manager_;
    std::unique_ptr<Index> index_;
    std::unique_ptr<Partition> partition_;
};

TEST_F(IndexTest, CreateSlotWithStorage) {
    // First create
    SlotNode* slot = index_->GetOrCreateSlotWithStorage(300);
    auto it = index_->slot_map_.find(300);
    ASSERT_TRUE(it != index_->slot_map_.end());
    EXPECT_EQ(it->first, 300);
    EXPECT_EQ(slot, &it->second);
    EXPECT_FALSE(slot->Loading());
    EXPECT_FALSE(slot->InMemory());
    EXPECT_FALSE(slot->Dirty());
    EXPECT_FALSE(slot->MetaDirty());
    EXPECT_EQ(slot->GetPageNum(), 0);
    EXPECT_EQ(slot->GetObjectNum(), 0);
    EXPECT_EQ(slot->GetMinTtl(), 0);

    // Second create
    // EXPECT_DEBUG_DEATH(index_->GetOrCreateSlotWithStorage(300), "");
}

TEST_F(IndexTest, GetOrCreateSlot) {
    // First
    SlotNode* slot = index_->GetOrCreateSlot(300);
    auto it = index_->slot_map_.find(300);
    ASSERT_TRUE(it != index_->slot_map_.end());
    EXPECT_EQ(it->first, 300);
    EXPECT_EQ(slot, &it->second);
    EXPECT_FALSE(slot->Loading());
    EXPECT_TRUE(slot->InMemory());
    EXPECT_FALSE(slot->Dirty());
    EXPECT_FALSE(slot->MetaDirty());
    EXPECT_EQ(slot->GetPageNum(), 0);
    EXPECT_EQ(slot->GetObjectNum(), 0);
    EXPECT_EQ(slot->GetMinTtl(), 0);

    // Second
    SlotNode* new_slot = index_->GetOrCreateSlot(300);
    EXPECT_EQ(new_slot, slot);
}

TEST_F(IndexTest, GetSlot) {
    SlotNode* slot = index_->GetSlot(100);
    EXPECT_NE(slot, nullptr);

    slot = index_->GetSlot(200);
    EXPECT_NE(slot, nullptr);

    slot = index_->GetSlot(300);
    EXPECT_EQ(slot, nullptr);
}

/*
TEST_F(IndexTest, TryDeleteSlot) {
    index_->TryDeleteSlot(100);
    auto it = index_->slot_map_.find(100);
    ASSERT_TRUE(it != index_->slot_map_.end());

    index_->TryDeleteSlot(200);
    it = index_->slot_map_.find(200);
    ASSERT_TRUE(it == index_->slot_map_.end());
}
*/

TEST_F(IndexTest, EvictSlot) {
    {
        SlotNode* slot = index_->GetOrCreateSlotWithStorage(300);
        ASSERT_NE(slot, nullptr);
        PageIndex page;
        page.object_id = 0;
        page.page_id = 0;
        page.address = 100;
        page.page_size = 10;
        page.page_in_log = false;
        Status status = slot->NewPage(Layout::WriteOptions(index_->slot_node_allocator_), page);
        ASSERT_TRUE(status.ok()) << status;
        Object object;
        status =
            slot->NewObject(Layout::WriteOptions(index_->slot_node_allocator_), 1, "key1", &object);
        ASSERT_TRUE(status.ok()) << status;
        slot->SetInMemory(true);

        status = index_->EvictSlot(300);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(slot->GetPageNum(), 1);
        EXPECT_EQ(slot->GetObjectNum(), 0);
    }
    {
        SlotNode* slot = index_->GetOrCreateSlotWithStorage(400);
        ASSERT_NE(slot, nullptr);
        PageIndex page;
        page.object_id = 0;
        page.page_id = 0;
        page.address = 100;
        page.page_size = 10;
        page.page_in_log = false;
        Status status = slot->NewPage(Layout::WriteOptions(index_->slot_node_allocator_), page);
        ASSERT_TRUE(status.ok()) << status;
        Object object;
        status =
            slot->NewObject(Layout::WriteOptions(index_->slot_node_allocator_), 1, "key1", &object);
        ASSERT_TRUE(status.ok()) << status;
        slot->SetInMemory(true);
        slot->SetDirty(true);

        status = index_->EvictSlot(400);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(slot->GetPageNum(), 1);
        EXPECT_EQ(slot->GetObjectNum(), 0);
    }
}

TEST_F(IndexTest, SlotIterator) {
    std::unique_ptr<Index::SlotIterator> iter = index_->NewSlotIterator();
    std::vector<uint64_t> slots;
    bool has_more = iter->Scan(10, [&slots](uint64_t slot_id, SlotNode* slot) -> bool {
        slots.push_back(slot_id);
        return true;
    });
    EXPECT_TRUE(has_more);
    ASSERT_EQ(slots.size(), 10);
    EXPECT_EQ(slots[0], 100);
    EXPECT_EQ(slots[9], 109);

    slots.clear();
    has_more = iter->Scan(10, [&slots](uint64_t slot_id, SlotNode* slot) -> bool {
        slots.push_back(slot_id);
        return slot_id < 115;
    });
    EXPECT_TRUE(has_more);
    ASSERT_EQ(slots.size(), 6);
    EXPECT_EQ(slots[0], 110);
    EXPECT_EQ(slots[5], 115);

    slots.clear();
    has_more = iter->Scan(1000, [&slots](uint64_t slot_id, SlotNode* slot) -> bool {
        slots.push_back(slot_id);
        return true;
    });
    EXPECT_FALSE(has_more);
    ASSERT_EQ(slots.size(), 85);
    EXPECT_EQ(slots[0], 116);
    EXPECT_EQ(slots[84], 200);
}

TEST_F(IndexTest, GetSlotPages) {
    std::vector<PageIndex> pages = index_->GetSlotPages(101, false);
    ASSERT_EQ(pages.size(), 9);
    EXPECT_EQ(pages[0].object_id, 0);
    EXPECT_EQ(pages[0].page_id, 0);
    EXPECT_EQ(pages[8].object_id, 2);
    EXPECT_EQ(pages[8].page_id, 2);

    PageInfo page;
    page.header.set_object_id(0);
    page.header.set_page_id(0);
    page.address = 1000;
    page.size = 0;
    page.header.set_page_in_log(true);
    index_->MarkSlotPageDirty(101, 1000, 1, {page});

    pages = index_->GetSlotPages(101, false);
    ASSERT_EQ(pages.size(), 8);
    EXPECT_EQ(pages[0].object_id, 0);
    EXPECT_EQ(pages[0].page_id, 1);
    EXPECT_EQ(pages[7].object_id, 2);
    EXPECT_EQ(pages[7].page_id, 2);
}

TEST_F(IndexTest, TouchSlot) {
    ASSERT_EQ(index_->slot_map_[100].GetLastUsed(), 0);
    index_->TouchSlot(100);
    ASSERT_NE(index_->slot_map_[100].GetLastUsed(), 0);
}

TEST_F(IndexTest, MarkSlotDataDirty) {
    // First mark
    SlotNode* slot = index_->GetSlot(100);
    ASSERT_FALSE(slot->Dirty());
    index_->MarkSlotDataDirty(100, 1000, 1);
    ASSERT_TRUE(slot->Dirty());
    ASSERT_EQ(index_->slot_context_manager_->DirtySlotsNum(), 51);
    EXPECT_EQ(100, index_->slot_context_manager_->dirty_slot_list_.front().slot_id);
    EXPECT_EQ(1000, slot_context_manager_->GetSlotFirstDirtyLogId(
                        index_->slot_context_manager_->dirty_slot_list_.front().slot_id));

    // Second mark
    index_->MarkSlotDataDirty(100, 2000, 1);
    ASSERT_EQ(index_->slot_context_manager_->DirtySlotsNum(), 51);
    EXPECT_EQ(100, index_->slot_context_manager_->dirty_slot_list_.front().slot_id);
    EXPECT_EQ(1000, slot_context_manager_->GetSlotFirstDirtyLogId(
                        index_->slot_context_manager_->dirty_slot_list_.front().slot_id));
}

TEST_F(IndexTest, MarkSlotPageDirty) {
    // Mark pages that already exist
    PageInfo page;
    page.header.set_object_id(0);
    page.header.set_page_id(0);
    page.address = 10000;
    page.size = 10;
    page.header.set_page_in_log(true);
    index_->MarkSlotPageDirty(100, 10000, 1, {page});
    SlotNode* slot = index_->GetSlot(100);
    ASSERT_TRUE(slot->Dirty());
    PageIndex tmp_page;
    Status status = slot->FindPage(0, 0, &tmp_page);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(tmp_page.dirty);
    ASSERT_EQ(index_->slot_context_manager_->DirtySlotsNum(), 51);
    EXPECT_EQ(100, index_->slot_context_manager_->dirty_slot_list_.front().slot_id);
    EXPECT_EQ(10000, slot_context_manager_->GetSlotFirstDirtyLogId(
                         index_->slot_context_manager_->dirty_slot_list_.front().slot_id));

    // Mark pages that did not exist before
    page.header.set_page_id(100);
    index_->MarkSlotPageDirty(100, 10000, 1, {page});
    status = slot->FindPage(0, 100, &tmp_page);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(tmp_page.dirty);
    ASSERT_EQ(index_->slot_context_manager_->DirtySlotsNum(), 51);
    EXPECT_EQ(100, index_->slot_context_manager_->dirty_slot_list_.front().slot_id);
    EXPECT_EQ(10000, slot_context_manager_->GetSlotFirstDirtyLogId(
                         index_->slot_context_manager_->dirty_slot_list_.front().slot_id));

    // Mark pages that has already been marked
    index_->MarkSlotPageDirty(100, 10000, 1, {page});
    ASSERT_EQ(index_->slot_context_manager_->DirtySlotsNum(), 51);
    EXPECT_EQ(100, index_->slot_context_manager_->dirty_slot_list_.front().slot_id);
    EXPECT_EQ(10000, slot_context_manager_->GetSlotFirstDirtyLogId(
                         index_->slot_context_manager_->dirty_slot_list_.front().slot_id));
}

TEST_F(IndexTest, MarkSlotObjectTtlDirty) {
    // First mark
    SlotNode* slot = index_->GetSlot(100);
    ASSERT_FALSE(slot->MetaDirty());
    index_->MarkSlotObjectTtlDirty(100, 1000, 1, 0, 560);
    ASSERT_EQ(slot->GetObjectTtl(0), 560);
    ASSERT_TRUE(slot->MetaDirty());
    ASSERT_EQ(index_->slot_context_manager_->DirtySlotsNum(), 51);
    EXPECT_EQ(100, index_->slot_context_manager_->dirty_slot_list_.front().slot_id);
    EXPECT_EQ(1000, slot_context_manager_->GetSlotFirstDirtyLogId(
                        index_->slot_context_manager_->dirty_slot_list_.front().slot_id));

    // Second mark
    index_->MarkSlotObjectTtlDirty(100, 2000, 1, 0, 570);
    ASSERT_EQ(slot->GetObjectTtl(0), 570);
    ASSERT_EQ(index_->slot_context_manager_->DirtySlotsNum(), 51);
    EXPECT_EQ(100, index_->slot_context_manager_->dirty_slot_list_.front().slot_id);
    EXPECT_EQ(1000, slot_context_manager_->GetSlotFirstDirtyLogId(
                        index_->slot_context_manager_->dirty_slot_list_.front().slot_id));
}

TEST_F(IndexTest, MarkSlotObjectDeleted) {
    // First mark
    SlotNode* slot = index_->GetSlot(102);
    ASSERT_FALSE(slot->MetaDirty());
    index_->MarkSlotObjectDeleted(102, 1000, 1, 0);
    ASSERT_TRUE(slot->MetaDirty());
    for (uint16_t page_id = 0; page_id < 2; ++page_id) {
        PageIndex page;
        Status status = slot->FindPage(0, page_id, &page);
        ASSERT_TRUE(page.dirty);
        ASSERT_EQ(page.page_size, 0);
        ASSERT_TRUE(page.page_in_log);
        ASSERT_EQ(1000, page.address);
    }
    ASSERT_EQ(index_->slot_context_manager_->DirtySlotsNum(), 51);
    EXPECT_EQ(102, index_->slot_context_manager_->dirty_slot_list_.front().slot_id);
    EXPECT_EQ(1000, slot_context_manager_->GetSlotFirstDirtyLogId(
                        index_->slot_context_manager_->dirty_slot_list_.front().slot_id));

    // Second mark
    index_->MarkSlotObjectDeleted(102, 1000, 1, 0);
    ASSERT_EQ(index_->slot_context_manager_->DirtySlotsNum(), 51);
    EXPECT_EQ(102, index_->slot_context_manager_->dirty_slot_list_.front().slot_id);
    EXPECT_EQ(1000, slot_context_manager_->GetSlotFirstDirtyLogId(
                        index_->slot_context_manager_->dirty_slot_list_.front().slot_id));
}

TEST_F(IndexTest, GetDirtySlots) {}

TEST_F(IndexTest, GetSlotMarkedPages) {
    index_->MarkSlotObjectDeleted(101, 1000, 1, 0);
    std::vector<PageIndex> pages;
    index_->GetSlotMarkedPages(101, &pages);
    ASSERT_EQ(pages.size(), 3);
}

TEST_F(IndexTest, GetSlotMarkedPagesAndMetas) {
    index_->MarkSlotObjectDeleted(101, 1000, 1, 0);
    index_->MarkSlotObjectTtlDirty(101, 1000, 1, 0, 560);
    std::vector<PageIndex> pages;
    std::vector<storage::IndexLog::ObjectItem> metas;
    index_->GetSlotMarkedPagesAndMetas(101, &pages, &metas);
    SlotNode* slot = index_->GetSlot(101);
    ASSERT_EQ(slot->GetObjectNum(), 3);
    ASSERT_EQ(pages.size(), 3);
    ASSERT_EQ(metas.size(), 1);
    ASSERT_EQ(metas[0].object_id(), 0);
    ASSERT_EQ(metas[0].ttl(), 560);
}

/*
TEST_F(IndexTest, ClearSlotLogDirty) {
    SlotNode* slot = index_->GetSlot(199);
    index_->MarkSlotObjectTtlDirty(199, 1000, 0, 560);
    ASSERT_TRUE(slot->Dirty());
    index_->ClearSlotDataDirty(199);
    ASSERT_FALSE(slot->DataDirty());
    ASSERT_TRUE(slot->MetaDirty());
}
*/

TEST_F(IndexTest, ClearSlotDirty) {
    index_->MarkSlotDataDirty(101, 1000, 1);
    index_->MarkSlotObjectDeleted(101, 1000, 1, 0);
    index_->MarkSlotObjectTtlDirty(101, 1000, 1, 0, 560);
    SlotNode* slot = index_->GetSlot(101);
    ASSERT_TRUE(slot->Dirty());
    ASSERT_TRUE(slot->MetaDirty());
    index_->ClearSlotDirty(101);
    ASSERT_FALSE(slot->Dirty());
    ASSERT_FALSE(slot->MetaDirty());
}

TEST_F(IndexTest, UpdateSlotPages_NewPage) {
    PageInfo page;
    page.header.set_object_id(0);
    page.header.set_page_id(100);
    page.address = 2000;
    page.size = 10;
    page.header.set_page_in_log(false);
    index_->UpdateSlotPages(100, 2000, {page});

    PageIndex tmp_page;
    SlotNode* slot = index_->GetSlot(100);
    Status status = slot->FindPage(0, 100, &tmp_page);
    ASSERT_EQ(tmp_page.address, 2000);

    Commit();
    Reload();
    slot = index_->GetSlot(100);
    status = slot->FindPage(0, 100, &tmp_page);
    ASSERT_EQ(tmp_page.address, 2000);
}

TEST_F(IndexTest, UpdateSlotPages_UpdatePage) {
    PageInfo page;
    page.header.set_object_id(0);
    page.header.set_page_id(0);
    page.address = 2000;
    page.size = 10;
    page.header.set_page_in_log(false);
    index_->UpdateSlotPages(100, 2000, {page});
    PageIndex tmp_page;
    SlotNode* slot = index_->GetSlot(100);
    Status status = slot->FindPage(0, 0, &tmp_page);
    ASSERT_EQ(tmp_page.address, 2000);

    Commit();
    Reload();
    slot = index_->GetSlot(100);
    status = slot->FindPage(0, 0, &tmp_page);
    ASSERT_EQ(tmp_page.address, 2000);
}

TEST_F(IndexTest, UpdateSlotPages_DeletePage) {
    PageInfo page;
    page.header.set_object_id(0);
    page.header.set_page_id(0);
    page.address = 2000;
    page.size = 0;
    page.header.set_page_in_log(false);
    index_->UpdateSlotPages(101, 2000, {page});
    PageIndex tmp_page;
    SlotNode* slot = index_->GetSlot(101);
    Status status = slot->FindPage(0, 0, &tmp_page);
    ASSERT_TRUE(status.IsNotFound()) << status;

    Commit();
    Reload();
    slot = index_->GetSlot(101);
    status = slot->FindPage(0, 0, &tmp_page);
    ASSERT_TRUE(status.IsNotFound()) << status;
}

TEST_F(IndexTest, UpdateSlotPages_UpdateDirtyPage) {
    PageInfo page;
    page.header.set_object_id(0);
    page.header.set_page_id(0);
    page.address = 1000;
    page.size = 10;
    page.header.set_page_in_log(true);
    index_->MarkSlotPageDirty(101, 1000, 1, {page});

    page.header.set_object_id(0);
    page.header.set_page_id(0);
    page.address = 2000;
    page.size = 100;
    page.header.set_page_in_log(false);
    index_->UpdateSlotPages(101, 2000, {page});
    PageIndex tmp_page;
    SlotNode* slot = index_->GetSlot(101);
    Status status = slot->FindPage(0, 0, &tmp_page);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(tmp_page.address, 1000);

    Commit();
    Reload();
    slot = index_->GetSlot(101);
    status = slot->FindPage(0, 0, &tmp_page);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(tmp_page.address, 1010);
}

TEST_F(IndexTest, UpdateSlotPagesAndMetas) {
    index_->MarkSlotObjectTtlDirty(100, 2000, 1, 100, 570);
    std::vector<storage::IndexLog::ObjectItem> metas;
    storage::IndexLog::ObjectItem item;
    item.set_object_id(0);
    item.set_ttl(570);
    metas.push_back(item);
    index_->UpdateSlotPagesAndMetas(100, 2000, {}, metas);
    // SlotNode* slot = index_->GetSlot(100);
    // EXPECT_EQ(slot->GetObjectTtl(0), 570);

    Commit();
    Reload();
    SlotNode* slot = index_->GetSlot(100);
    EXPECT_EQ(slot->GetObjectTtl(0), 570);
}

TEST_F(IndexTest, UpdateSlotPagesAndMetas_DirtyMeta) {
    index_->MarkSlotObjectTtlDirty(100, 1000, 1, 0, 560);
    std::vector<storage::IndexLog::ObjectItem> metas;
    storage::IndexLog::ObjectItem item;
    item.set_object_id(0);
    item.set_ttl(570);
    metas.push_back(item);
    index_->UpdateSlotPagesAndMetas(100, 2000, {}, metas);
    SlotNode* slot = index_->GetSlot(100);
    EXPECT_EQ(slot->GetObjectTtl(0), 560);

    Commit();
    Reload();
    slot = index_->GetSlot(100);
    EXPECT_EQ(slot->GetObjectTtl(0), 570);
}

TEST_F(IndexTest, DumpLogId) {
    index_->SetDumpedLogId(111);
    EXPECT_EQ(index_->GetDumpedLogId(), 111);

    Commit();
    Reload();
    EXPECT_EQ(index_->GetDumpedLogId(), 111);
}

TEST_F(IndexTest, UpdateZoneInfo) {
    storage::IndexLog::ZoneInfo zone_info;
    zone_info.set_init_time_ms(1111);
    index_->UpdateZoneInfo(100, zone_info);
    EXPECT_EQ(index_->meta_.zones().at(100).init_time_ms(), 1111);

    Commit();
    Reload();
    EXPECT_EQ(index_->meta_.zones().at(100).init_time_ms(), 1111);
}

TEST_F(IndexTest, DeleteZoneInfo) {
    storage::IndexLog::ZoneInfo zone_info;
    zone_info.set_init_time_ms(1111);
    index_->UpdateZoneInfo(100, zone_info);
    EXPECT_EQ(index_->meta_.zones().at(100).init_time_ms(), 1111);

    index_->DeleteZoneInfo(100);
    EXPECT_TRUE(index_->meta_.zones().find(100) == index_->meta_.zones().end());

    Commit();
    Reload();
    EXPECT_TRUE(index_->meta_.zones().find(100) == index_->meta_.zones().end());
}

TEST_F(IndexTest, GetZoneInfo) {
    storage::IndexLog::ZoneInfo zone_info;
    zone_info.set_init_time_ms(1111);
    index_->UpdateZoneInfo(100, zone_info);
    EXPECT_EQ(index_->meta_.zones().at(100).init_time_ms(), 1111);

    bool exist = index_->GetZoneInfo(100, &zone_info);
    EXPECT_TRUE(exist);
    EXPECT_EQ(zone_info.init_time_ms(), 1111);

    exist = index_->GetZoneInfo(101, &zone_info);
    EXPECT_FALSE(exist);
}

TEST_F(IndexTest, ZoneStats_InPageStore) {
    uint32_t init_bytes1 = index_->GetZoneStats().GetPageStoreUsedBytes(100);
    uint32_t init_bytes2 = index_->GetZoneStats().GetPageStoreUsedBytes(200);
    PageInfo page;
    page.header.set_object_id(0);
    page.header.set_page_id(0);
    page.address = MakeZoneAddress(100, 10);
    page.size = 10;
    page.header.set_page_in_log(false);
    index_->UpdateSlotPages(100, 1000, {page});
    EXPECT_EQ(index_->GetZoneStats().GetPageStoreUsedBytes(100), init_bytes1 + 10U);

    page.address = MakeZoneAddress(200, 10);
    page.size = 100;
    index_->UpdateSlotPages(100, 1000, {page});
    EXPECT_EQ(index_->GetZoneStats().GetPageStoreUsedBytes(100), init_bytes1);
    EXPECT_EQ(index_->GetZoneStats().GetPageStoreUsedBytes(200), init_bytes2 + 100);

    page.address = MakeZoneAddress(100, 10);
    page.size = 300;
    page.header.set_page_in_log(true);
    index_->MarkSlotPageDirty(100, 1000, 1, {page});
    EXPECT_EQ(index_->GetZoneStats().GetPageStoreUsedBytes(100), init_bytes1);
    EXPECT_EQ(index_->GetZoneStats().GetPageStoreUsedBytes(200), init_bytes2);

    Commit();
    Reload();
    EXPECT_EQ(index_->GetZoneStats().GetPageStoreUsedBytes(100), init_bytes1);
    EXPECT_EQ(index_->GetZoneStats().GetPageStoreUsedBytes(200), init_bytes2 + 100);

    page.size = 0;
    index_->UpdateSlotPages(100, 1000, {page});
    EXPECT_EQ(index_->GetZoneStats().GetPageStoreUsedBytes(100), init_bytes1);
    EXPECT_EQ(index_->GetZoneStats().GetPageStoreUsedBytes(200), init_bytes2);

    Commit();
    Reload();
    EXPECT_EQ(index_->GetZoneStats().GetPageStoreUsedBytes(100), init_bytes1);
    EXPECT_EQ(index_->GetZoneStats().GetPageStoreUsedBytes(200), init_bytes2);
}

TEST_F(IndexTest, ZoneStats_InOpLogger) {}

}  // namespace partition
}  // namespace bcache2
