// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/index/slot_node.h"

#include <gtest/gtest.h>

#include "model/dummy_model.h"

namespace bcache2 {
namespace partition {
namespace test {

using model::DummyModel;

class SlotNodeTest : public testing::Test {
 public:
    void SetUp() override {}

    void TearDown() override {
        slot.ClearObjects(SlotNode::WriteOptions(&allocator));
        slot.ClearPages(SlotNode::WriteOptions(&allocator));
    }

 protected:
    void ClearSlot() {
        for (auto& page : slot.GetPages()) {
            Status status =
                slot.DeletePage(SlotNode::WriteOptions(&allocator), page.object_id, page.page_id);
            ASSERT_TRUE(status.ok());
        }
        for (auto& object : slot.GetObjects()) {
            Status status = slot.DeleteObject(SlotNode::WriteOptions(&allocator), object.Key());
            ASSERT_TRUE(status.ok());
        }
        ASSERT_EQ(slot.GetPageNum(), 0);
        ASSERT_EQ(slot.GetObjectNum(), 0);
    }
    SlotNode slot;
    Allocator allocator;
};

TEST_F(SlotNodeTest, Empty) {
    ASSERT_FALSE(slot.Loading());
    ASSERT_FALSE(slot.Dirty());
    ASSERT_FALSE(slot.InMemory());
    ASSERT_EQ(slot.GetPageNum(), 0);
    ASSERT_TRUE(slot.GetPages().empty());
    ASSERT_EQ(slot.GetObjectNum(), 0);
    ASSERT_TRUE(slot.GetObjects().empty());
    ASSERT_EQ(slot.pointer_, 0);
}

TEST_F(SlotNodeTest, PageManagement) {
    bool loading = true;
    bool dirty = false;
    bool in_memory = true;
    slot.SetLoading(loading);
    slot.SetDirty(dirty);
    slot.SetInMemory(in_memory);
    BYTE_DEFER({
        ASSERT_EQ(slot.Loading(), loading);
        ASSERT_EQ(slot.Dirty(), dirty);
        ASSERT_EQ(slot.InMemory(), in_memory);
    });

    // no page now
    ASSERT_EQ(slot.CurrentLayout(), LayoutType::kSinglePage);
    PageIndex page;
    Status status = slot.FindPage(0, 0, nullptr);
    ASSERT_TRUE(status.IsNotFound());

    // prefill some pages
    size_t prefill_num = 10;
    for (size_t i = 0; i < prefill_num; ++i) {
        PageIndex page;
        page.object_id = i;
        page.page_id = i;
        page.page_size = i;
        page.address = i + 1;
        page.page_in_log = true;
        Status status = slot.NewPage(SlotNode::WriteOptions(&allocator), page);
        ASSERT_TRUE(status.ok());
    }

    // find not exist page
    status = slot.FindPage(128, 128, &page);
    ASSERT_TRUE(status.IsNotFound());

    // new page
    page.object_id = 128;
    page.page_id = 128;
    page.page_size = 10;
    page.address = 20;
    status = slot.NewPage(SlotNode::WriteOptions(&allocator), page);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(slot.GetPageNum(), prefill_num + 1);

    // dup new page
    status = slot.NewPage(SlotNode::WriteOptions(&allocator), page);
    ASSERT_TRUE(status.IsAlreadyExists());

    {
        // dup new page, but update_if_exist is set
        SlotNode::WriteOptions opts(&allocator);
        opts.update_if_exist = true;
        page.page_size = 111111;
        status = slot.NewPage(opts, page);
        ASSERT_TRUE(status.ok());

        PageIndex new_page;
        status = slot.FindPage(page.object_id, page.page_id, &new_page);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(new_page.page_size, page.page_size);
    }

    // change page
    page.dirty = 1;
    status =
        slot.UpdatePage(SlotNode::WriteOptions(&allocator), page.object_id, page.page_id, page);
    ASSERT_TRUE(status.ok());

    // find page and check page correctness
    PageIndex new_page;
    status = slot.FindPage(page.object_id, page.page_id, &new_page);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(new_page.object_id, page.object_id);
    ASSERT_EQ(new_page.page_id, page.page_id);
    ASSERT_EQ(new_page.page_size, page.page_size);
    ASSERT_EQ(new_page.address, page.address);
    ASSERT_EQ(new_page.page_in_log, page.page_in_log);
    ASSERT_EQ(new_page.dirty, page.dirty);

    // delete page
    status = slot.DeletePage(SlotNode::WriteOptions(&allocator), page.object_id, page.page_id);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(slot.GetPageNum(), prefill_num);
    ASSERT_EQ(slot.GetPages().size(), prefill_num);

    // dup delete page
    status = slot.DeletePage(SlotNode::WriteOptions(&allocator), page.object_id, page.page_id);
    ASSERT_TRUE(status.IsNotFound());

    // there is no page now
    status = slot.FindPage(page.object_id, page.page_id, nullptr);
    ASSERT_TRUE(status.IsNotFound());

    // clear pages
    std::vector<PageIndex> pages = slot.GetPages();
    for (size_t i = 0; i < pages.size(); ++i) {
        status = slot.DeletePage(SlotNode::WriteOptions(&allocator), pages[i].object_id,
                                 pages[i].page_id);
        ASSERT_TRUE(status.ok());
        if (i == pages.size() - 1) {
            ASSERT_EQ(slot.CurrentLayout(), LayoutType::kSinglePage) << slot.GetPageNum();
            ASSERT_EQ(slot.GetPageNum(), 0);
        }
        if (i < pages.size() - 2) {
            ASSERT_EQ(slot.CurrentLayout(), LayoutType::kMultiPageObject) << slot.GetPageNum();
        }
    }
}

TEST_F(SlotNodeTest, SinglePageManagement) {
    {
        // no page now
        PageIndex page;
        Status status = slot.FindPage(0, 0, nullptr);
        ASSERT_TRUE(status.IsNotFound());
    }

    {
        // find not exist page
        PageIndex page;
        Status status = slot.FindPage(0, 0, &page);
        ASSERT_TRUE(status.IsNotFound());
    }

    // new page
    PageIndex page;
    page.object_id = 0;
    page.page_id = 0;
    page.page_size = 1290;
    page.address = 20031;
    Status status = slot.NewPage(SlotNode::WriteOptions(&allocator), page);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(slot.GetPageNum(), 1);
    ASSERT_EQ(slot.GetPages().front().address, page.address);
    ASSERT_EQ(slot.CurrentLayout(), LayoutType::kSinglePage);

    // dup new object
    status = slot.NewPage(SlotNode::WriteOptions(&allocator), page);
    ASSERT_TRUE(status.IsAlreadyExists());
    ASSERT_EQ(slot.CurrentLayout(), LayoutType::kSinglePage);

    // change page
    page.dirty = 1;
    status =
        slot.UpdatePage(SlotNode::WriteOptions(&allocator), page.object_id, page.page_id, page);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(slot.CurrentLayout(), LayoutType::kSinglePage);

    // find page and check page correctness
    status = slot.FindPage(0, 0, &page);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(page.object_id, 0);
    ASSERT_EQ(page.page_id, 0);
    ASSERT_EQ(page.page_size, 1290);
    ASSERT_EQ(page.address, 20031);
    ASSERT_FALSE(page.page_in_log);
    ASSERT_TRUE(page.dirty);
    ASSERT_EQ(slot.CurrentLayout(), LayoutType::kSinglePage);

    // delete page
    status = slot.DeletePage(SlotNode::WriteOptions(&allocator), 0, 0);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(slot.GetPageNum(), 0);
    ASSERT_TRUE(slot.GetPages().empty());
    ASSERT_EQ(slot.CurrentLayout(), LayoutType::kSinglePage);

    // dup delete page
    status = slot.DeletePage(SlotNode::WriteOptions(&allocator), 0, 0);
    ASSERT_TRUE(status.IsNotFound());
    ASSERT_EQ(slot.CurrentLayout(), LayoutType::kSinglePage);

    // there is no page now
    status = slot.FindPage(0, 0, nullptr);
    ASSERT_TRUE(status.IsNotFound());
    ASSERT_EQ(slot.CurrentLayout(), LayoutType::kSinglePage);
}

TEST_F(SlotNodeTest, ObjectManagement) {
    const std::string key = "test_ley";

    bool loading = true;
    bool dirty = false;
    bool in_memory = true;
    slot.SetLoading(loading);
    slot.SetDirty(dirty);
    slot.SetInMemory(in_memory);
    BYTE_DEFER({
        ASSERT_EQ(slot.Loading(), loading);
        ASSERT_EQ(slot.Dirty(), dirty);
        ASSERT_EQ(slot.InMemory(), in_memory);
    });

    // no object now
    ASSERT_EQ(slot.CurrentLayout(), LayoutType::kSinglePage);
    Object obj;
    Status status = slot.FindObject("test", nullptr);
    ASSERT_TRUE(status.IsNotFound());

    // prefill some objects
    size_t prefill_num = 10;
    for (size_t i = 0; i < prefill_num; ++i) {
        Status status = slot.NewObject(SlotNode::WriteOptions(&allocator),
                                       model::ModelManager::GetModelId<DummyModel>(),
                                       std::to_string(i), nullptr);
        ASSERT_TRUE(status.ok());
        if (i == 0) {
            ASSERT_EQ(slot.CurrentLayout(), LayoutType::kSingleObject);
        }
        if (i > 0) {
            ASSERT_EQ(slot.CurrentLayout(), LayoutType::kMultiPageObject);
        }
    }

    // new object
    status = slot.NewObject(SlotNode::WriteOptions(&allocator),
                            model::ModelManager::GetModelId<DummyModel>(), key, &obj);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(obj.KeyLen(), key.size());
    ASSERT_EQ(obj.Key(), key);
    ASSERT_EQ(obj.ModelId(), model::ModelManager::GetModelId<DummyModel>());
    ASSERT_FALSE(obj.Trivial());
    ASSERT_EQ(slot.GetObjectNum(), prefill_num + 1);

    // dup new object
    status = slot.NewObject(SlotNode::WriteOptions(&allocator),
                            model::ModelManager::GetModelId<DummyModel>(), key, &obj);
    ASSERT_TRUE(status.IsAlreadyExists());

    // check model correctness
    model::DummyModel* model = obj.Model<model::DummyModel>();
    model->Set("key1", "value1");
    model->Set("key2", "value2");
    model->Set("key3", "value3");
    ASSERT_EQ(model->Size(), 3);
    ASSERT_EQ(model->Get("key1"), "value1");
    ASSERT_EQ(model->Get("key2"), "value2");
    ASSERT_EQ(model->Get("key3"), "value3");

    // find object and check model correctness
    status = slot.FindObject(key, &obj);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(obj.KeyLen(), key.size());
    ASSERT_EQ(obj.Key(), key);
    ASSERT_EQ(obj.ModelId(), model::ModelManager::GetModelId<DummyModel>());
    ASSERT_FALSE(obj.Trivial());
    model = obj.Model<model::DummyModel>();
    ASSERT_EQ(model->Size(), 3);
    ASSERT_EQ(model->Get("key1"), "value1");
    ASSERT_EQ(model->Get("key2"), "value2");
    ASSERT_EQ(model->Get("key3"), "value3");

    // delete object
    status = slot.DeleteObject(SlotNode::WriteOptions(&allocator), key);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(slot.GetObjectNum(), prefill_num);

    // dup delete object
    status = slot.DeleteObject(SlotNode::WriteOptions(&allocator), key);
    ASSERT_TRUE(status.IsNotFound());

    // there is no object now
    status = slot.FindObject(key, &obj);
    ASSERT_TRUE(status.IsNotFound());

    // clear objects
    std::vector<Object> objects = slot.GetObjects();
    std::vector<std::string> object_keys;
    for (size_t i = 0; i < objects.size(); ++i) {
        object_keys.emplace_back(objects[i].Key());
    }
    for (size_t i = 0; i < objects.size(); ++i) {
        status = slot.DeleteObject(SlotNode::WriteOptions(&allocator), object_keys[i]);
        ASSERT_TRUE(status.ok());
        if (i == objects.size() - 1) {
            ASSERT_EQ(slot.CurrentLayout(), LayoutType::kSinglePage) << slot.GetPageNum();
            ASSERT_EQ(slot.GetPageNum(), 0);
        }
        if (i < objects.size() - 2) {
            ASSERT_EQ(slot.CurrentLayout(), LayoutType::kMultiPageObject) << slot.GetPageNum();
        }
    }
}

TEST_F(SlotNodeTest, ClearObjects) {
    // prefill some objects
    size_t prefill_num = 10;
    for (size_t i = 0; i < prefill_num; ++i) {
        Status status = slot.NewObject(SlotNode::WriteOptions(&allocator),
                                       model::ModelManager::GetModelId<DummyModel>(),
                                       std::to_string(i), nullptr);
        ASSERT_TRUE(status.ok());
    }

    // clear objects
    slot.ClearObjects(SlotNode::WriteOptions(&allocator));
    ASSERT_EQ(slot.GetObjectNum(), 0);
    ASSERT_TRUE(slot.GetObjects().empty());
}

TEST_F(SlotNodeTest, Transform) {
    bool loading = true;
    bool dirty = false;
    bool in_memory = true;
    slot.SetLoading(loading);
    slot.SetDirty(dirty);
    slot.SetInMemory(in_memory);
    BYTE_DEFER({
        ASSERT_EQ(slot.Loading(), loading);
        ASSERT_EQ(slot.Dirty(), dirty);
        ASSERT_EQ(slot.InMemory(), in_memory);
    });

    // no page and object
    ASSERT_EQ(slot.CurrentLayout(), LayoutType::kSinglePage);

    // new object
    Status status = slot.NewObject(SlotNode::WriteOptions(&allocator),
                                   model::ModelManager::GetModelId<DummyModel>(), "test", nullptr);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(slot.CurrentLayout(), LayoutType::kSingleObject);

    slot.SetLastUsed(SlotNode::WriteOptions(&allocator), 0x123456);

    // new page and page id is 0
    // now we have 1 page and 1 object
    PageIndex page;
    page.object_id = 0;
    page.page_id = 0;
    page.address = 1230123;
    status = slot.NewPage(SlotNode::WriteOptions(&allocator), page);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(slot.CurrentLayout(), LayoutType::kSinglePageObject);
    ASSERT_EQ(slot.GetLastUsed(), 0x123456);

    // new page
    // now we have 1 object and 2 pages
    page.object_id = 0;
    page.page_id = 1;
    page.address = 3230123;
    status = slot.NewPage(SlotNode::WriteOptions(&allocator), page);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(slot.CurrentLayout(), LayoutType::kMultiPageObject);
    ASSERT_EQ(slot.GetLastUsed(), 0x123456);

    // delete page
    // now we have 1 object and 1 page
    status = slot.DeletePage(SlotNode::WriteOptions(&allocator), page.object_id, page.page_id);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(slot.CurrentLayout(), LayoutType::kSinglePageObject);
    ASSERT_EQ(slot.GetLastUsed(), 0x123456);

    // delete object
    // now we have 1 page
    status = slot.DeleteObject(SlotNode::WriteOptions(&allocator), "test");
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(slot.CurrentLayout(), LayoutType::kSinglePage);

    // clear slot and new page with page id != 0
    // now we have 1 page but page id != 0
    ClearSlot();
    page.object_id = 0;
    page.page_id = 1;
    status = slot.NewPage(SlotNode::WriteOptions(&allocator), page);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(slot.CurrentLayout(), LayoutType::kMultiPageObject);
}

TEST_F(SlotNodeTest, Ttl) {
    size_t prefill_num = 10;
    for (size_t i = 0; i < prefill_num; ++i) {
        Status status = slot.NewObject(SlotNode::WriteOptions(&allocator),
                                       model::ModelManager::GetModelId<DummyModel>(),
                                       std::to_string(i), nullptr);
        ASSERT_TRUE(status.ok());
        slot.SetObjectTtl(SlotNode::WriteOptions(&allocator), i, i);
        ASSERT_EQ(slot.GetObjectTtl(i), i);
        ASSERT_EQ(slot.GetObjectTtls().size(), i + 1);
        ASSERT_EQ(slot.GetObjectTtls()[i].second, slot.GetObjectTtl(i));
    }

    for (size_t i = 0; i < prefill_num; ++i) {
        ASSERT_EQ(slot.GetObjectTtl(i), i);
    }

    ASSERT_EQ(slot.GetMinTtl(), 0);

    for (size_t i = 0; i < prefill_num; ++i) {
        slot.SetObjectTtl(SlotNode::WriteOptions(&allocator), i, i + 10);
        ASSERT_EQ(slot.GetObjectTtl(i), i + 10);
        ASSERT_EQ(slot.GetObjectTtls()[i].second, i + 10);
    }

    ASSERT_EQ(slot.GetMinTtl(), 10);

    for (size_t i = 0; i < prefill_num / 2; ++i) {
        Status status = slot.DeleteObject(SlotNode::WriteOptions(&allocator), std::to_string(i));
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(slot.GetObjectTtl(i), 0);
        ASSERT_EQ(slot.GetObjectTtls()[i].second, 0);
    }

    for (size_t i = 0; i < prefill_num / 2; ++i) {
        ASSERT_EQ(slot.GetObjectTtl(i), 0);
    }

    slot.ClearObjectTtl(SlotNode::WriteOptions(&allocator));

    for (size_t i = 0; i < prefill_num; ++i) {
        ASSERT_EQ(slot.GetObjectTtl(i), 0);
    }

    slot.ClearObjects(SlotNode::WriteOptions(&allocator));
    ASSERT_EQ(slot.GetObjectNum(), 0);
    ASSERT_TRUE(slot.GetObjects().empty());
}

}  // namespace test
}  // namespace partition
}  // namespace bcache2
