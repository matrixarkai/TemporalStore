// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <gtest/gtest.h>

#include "model/dummy_model.h"
#include "model/model_manager.h"
#include "partition/index/index.h"
#include "partition/index/layout/layout_manager.h"
#include "partition/index/layout/multi_page_object.h"
#include "partition/index/layout/single_object.h"
#include "partition/index/layout/single_page_object.h"

namespace bcache2 {
namespace partition {
namespace test {

using model::DummyModel;

class SingleObjectTest : public testing::Test {
 public:
    void SetUp() override {
        buf = LayoutManager::GenRawLayoutBuf(
            LayoutType::kSingleObject, &allocator,
            Object::ComputeRawObjectSize(key.size(),
                                         model::ModelManager::GetModelId<DummyModel>()));
        layout = SingleObject(buf, &allocator);
        layout.ConstructFrom({}, {}, 0);
        ASSERT_EQ(layout.CurrentLayout(), LayoutType::kSingleObject);
    }

    void TearDown() override { layout.Destroy(); }

 protected:
    uint8_t* buf = nullptr;
    SingleObject layout;
    Allocator allocator;
    const std::string key = "test_key";
};

TEST_F(SingleObjectTest, Empty) {
    ASSERT_EQ(layout.GetPageNum(), 0);
    ASSERT_TRUE(layout.GetPages().empty());
    ASSERT_EQ(layout.GetObjectNum(), 0);
    ASSERT_TRUE(layout.GetObjects().empty());
}

TEST_F(SingleObjectTest, PageManagement) {
    Status status = layout.NewPage(Layout::WriteOptions(&allocator), PageIndex{});
    ASSERT_TRUE(status.IsFailedPrecondition());

    status = layout.DeletePage(0, 0);
    ASSERT_TRUE(status.IsNotFound());

    status = layout.FindPage(0, 0, nullptr);
    ASSERT_TRUE(status.IsNotFound());

    status = layout.ClearPages();
    ASSERT_TRUE(status.ok());
}

TEST_F(SingleObjectTest, ObjectManagement) {
    // no object now
    Object obj;
    Status status = layout.FindObject("test", nullptr);
    ASSERT_TRUE(status.IsNotFound());

    // new object
    status = layout.NewObject(Layout::WriteOptions(nullptr),
                              model::ModelManager::GetModelId<DummyModel>(), key, &obj);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(obj.KeyLen(), key.size());
    ASSERT_EQ(obj.Key(), key);
    ASSERT_EQ(obj.ModelId(), model::ModelManager::GetModelId<DummyModel>());
    ASSERT_FALSE(obj.Trivial());
    ASSERT_EQ(layout.GetObjectNum(), 1);
    ASSERT_EQ(layout.GetObjects().front().RawBuf(), obj.RawBuf());

    // dup new object
    status = layout.NewObject(Layout::WriteOptions(nullptr),
                              model::ModelManager::GetModelId<DummyModel>(), key, &obj);
    ASSERT_TRUE(status.IsAlreadyExists());

    // dup new object
    status = layout.NewObject(Layout::WriteOptions(nullptr),
                              model::ModelManager::GetModelId<DummyModel>(), key + "1", &obj);
    ASSERT_TRUE(status.IsFailedPrecondition());

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
    status = layout.FindObject(key, &obj);
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
    status = layout.DeleteObject(key);
    ASSERT_TRUE(status.IsUnmatched());
    ASSERT_EQ(layout.GetObjectNum(), 0);
    ASSERT_TRUE(layout.GetObjects().empty());

    // dup delete object
    status = layout.DeleteObject(key);
    ASSERT_TRUE(status.IsNotFound());

    // there is no object now
    status = layout.FindObject(key, &obj);
    ASSERT_TRUE(status.IsNotFound());
}

TEST_F(SingleObjectTest, ClearObjects) {
    // new object
    Object obj;
    Status status = layout.NewObject(Layout::WriteOptions(nullptr),
                                     model::ModelManager::GetModelId<DummyModel>(), key, &obj);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(layout.GetObjectNum(), 1);

    // clear object
    status = layout.ClearObjects();
    ASSERT_TRUE(status.IsUnmatched());
    ASSERT_EQ(layout.GetObjectNum(), 0);
    ASSERT_TRUE(layout.GetObjects().empty());
}

TEST_F(SingleObjectTest, ConstructFrom) {
    // make an object
    Object obj;
    Status status = layout.NewObject(Layout::WriteOptions(nullptr),
                                     model::ModelManager::GetModelId<DummyModel>(), key, &obj);
    ASSERT_TRUE(status.ok());
    auto model = obj.Model<model::DummyModel>();
    model->Set("key1", "value1");
    model->Set("key2", "value2");
    model->Set("key3", "value3");

    // create a new layout and construct
    auto* new_buf = LayoutManager::GenRawLayoutBuf(
        LayoutType::kSingleObject, &allocator,
        Object::ComputeRawObjectSize(key.size(), model::ModelManager::GetModelId<DummyModel>()));
    auto new_layout = SingleObject(new_buf, &allocator);

    new_layout.ConstructFrom({}, {obj}, layout.GetLastUsed());
    BYTE_DEFER(new_layout.Destroy());
    ASSERT_EQ(new_layout.GetObjects().size(), 1);
    ASSERT_EQ(new_layout.GetObjectNum(), 1);
    ASSERT_EQ(new_layout.GetPages().size(), 0);
    ASSERT_EQ(new_layout.GetPageNum(), 0);

    // check object
    Object new_obj;
    status = new_layout.FindObject(key, &new_obj);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(new_obj.KeyLen(), key.size());
    ASSERT_EQ(new_obj.Key(), key);
    ASSERT_EQ(new_obj.ModelId(), model::ModelManager::GetModelId<DummyModel>());
    ASSERT_FALSE(new_obj.Trivial());
    ASSERT_EQ(new_layout.GetObjectNum(), 1);
    ASSERT_EQ(new_layout.GetObjects().front().RawBuf(), new_obj.RawBuf());
    model = new_obj.Model<model::DummyModel>();
    ASSERT_EQ(model->Size(), 3);
    ASSERT_EQ(model->Get("key1"), "value1");
    ASSERT_EQ(model->Get("key2"), "value2");
    ASSERT_EQ(model->Get("key3"), "value3");
}

class SinglePageObjectTest : public testing::Test {
 public:
    void SetUp() override {
        buf = LayoutManager::GenRawLayoutBuf(
            LayoutType::kSinglePageObject, &allocator,
            Object::ComputeRawObjectSize(key.size(),
                                         model::ModelManager::GetModelId<DummyModel>()));
        layout = SinglePageObject(buf, &allocator);
        layout.ConstructFrom({}, {}, 0);
        ASSERT_EQ(layout.CurrentLayout(), LayoutType::kSinglePageObject);
    }

    void TearDown() override { layout.Destroy(); }

 protected:
    uint8_t* buf = nullptr;
    SinglePageObject layout;
    Allocator allocator;
    const std::string key = "test_key";
};

TEST_F(SinglePageObjectTest, Empty) {
    ASSERT_EQ(layout.GetPageNum(), 0);
    ASSERT_TRUE(layout.GetPages().empty());
    ASSERT_EQ(layout.GetObjectNum(), 0);
    ASSERT_TRUE(layout.GetObjects().empty());
}

TEST_F(SinglePageObjectTest, PageManagement) {
    {
        // no page now
        PageIndex page;
        Status status = layout.FindPage(0, 0, nullptr);
        ASSERT_TRUE(status.IsNotFound());
    }

    {
        // page id > 0
        PageIndex page;
        page.page_id = 1;
        Status status = layout.NewPage(Layout::WriteOptions(&allocator), page);
        ASSERT_TRUE(status.IsFailedPrecondition());
    }

    {
        // object id > 0
        PageIndex page;
        page.object_id = 1;
        Status status = layout.NewPage(Layout::WriteOptions(&allocator), page);
        ASSERT_TRUE(status.IsFailedPrecondition());
    }

    {
        // find not exist page
        PageIndex page;
        Status status = layout.FindPage(0, 0, &page);
        ASSERT_TRUE(status.IsNotFound());
    }

    // new page
    PageIndex page;
    page.object_id = 0;
    page.page_id = 0;
    page.page_size = 1290;
    page.address = 20031;
    Status status = layout.NewPage(Layout::WriteOptions(&allocator), page);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(layout.GetPageNum(), 1);
    ASSERT_EQ(layout.GetPages().front().address, page.address);

    // dup new page
    status = layout.NewPage(Layout::WriteOptions(&allocator), page);
    ASSERT_TRUE(status.IsAlreadyExists());

    {
        // dup new page, but update_if_exist is set
        Layout::WriteOptions opts(&allocator);
        opts.update_if_exist = true;
        page.page_size = 111111;
        status = layout.NewPage(opts, page);
        ASSERT_TRUE(status.ok());

        PageIndex new_page;
        status = layout.FindPage(page.object_id, page.page_id, &new_page);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(new_page.page_size, page.page_size);
    }

    // change page
    page.dirty = 1;
    status = layout.UpdatePage(page.object_id, page.page_id, page);
    ASSERT_TRUE(status.ok());

    // find page and check page correctness
    PageIndex new_page;
    status = layout.FindPage(page.object_id, page.page_id, &new_page);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(new_page.object_id, page.object_id);
    ASSERT_EQ(new_page.page_id, page.page_id);
    ASSERT_EQ(new_page.page_size, page.page_size);
    ASSERT_EQ(new_page.address, page.address);
    ASSERT_EQ(new_page.page_in_log, page.page_in_log);
    ASSERT_EQ(new_page.dirty, page.dirty);

    // delete page
    status = layout.DeletePage(page.object_id, page.page_id);
    ASSERT_TRUE(status.IsUnmatched());
    ASSERT_EQ(layout.GetPageNum(), 0);
    ASSERT_TRUE(layout.GetPages().empty());

    // dup delete page
    status = layout.DeletePage(page.object_id, page.page_id);
    ASSERT_TRUE(status.IsNotFound());

    // there is no page now
    status = layout.FindPage(page.object_id, page.page_id, nullptr);
    ASSERT_TRUE(status.IsNotFound());
}

TEST_F(SinglePageObjectTest, ClearPages) {
    // new page
    PageIndex page;
    Status status = layout.NewPage(Layout::WriteOptions(&allocator), page);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(layout.GetPageNum(), 1);

    // clear page
    status = layout.ClearPages();
    ASSERT_TRUE(status.IsUnmatched());
    ASSERT_EQ(layout.GetPageNum(), 0);
    ASSERT_TRUE(layout.GetPages().empty());
}

TEST_F(SinglePageObjectTest, ObjectManagement) {
    // no object now
    Object obj;
    Status status = layout.FindObject("test", nullptr);
    ASSERT_TRUE(status.IsNotFound());

    // new object
    status = layout.NewObject(Layout::WriteOptions(nullptr),
                              model::ModelManager::GetModelId<DummyModel>(), key, &obj);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(obj.KeyLen(), key.size());
    ASSERT_EQ(obj.Key(), key);
    ASSERT_EQ(obj.ModelId(), model::ModelManager::GetModelId<DummyModel>());
    ASSERT_FALSE(obj.Trivial());
    ASSERT_EQ(layout.GetObjectNum(), 1);
    ASSERT_EQ(layout.GetObjects().front().RawBuf(), obj.RawBuf());

    // dup new object
    status = layout.NewObject(Layout::WriteOptions(nullptr),
                              model::ModelManager::GetModelId<DummyModel>(), key, &obj);
    ASSERT_TRUE(status.IsAlreadyExists());

    // dup new object
    status = layout.NewObject(Layout::WriteOptions(nullptr),
                              model::ModelManager::GetModelId<DummyModel>(), key + "1", &obj);
    ASSERT_TRUE(status.IsFailedPrecondition());

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
    status = layout.FindObject(key, &obj);
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
    status = layout.DeleteObject(key);
    ASSERT_TRUE(status.IsUnmatched());
    ASSERT_EQ(layout.GetObjectNum(), 0);
    ASSERT_TRUE(layout.GetObjects().empty());

    // dup delete object
    status = layout.DeleteObject(key);
    ASSERT_TRUE(status.IsNotFound());

    // there is no object now
    status = layout.FindObject(key, &obj);
    ASSERT_TRUE(status.IsNotFound());
}

TEST_F(SinglePageObjectTest, ClearObjects) {
    // new object
    Object obj;
    Status status = layout.NewObject(Layout::WriteOptions(nullptr),
                                     model::ModelManager::GetModelId<DummyModel>(), key, &obj);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(layout.GetObjectNum(), 1);

    // clear object
    status = layout.ClearObjects();
    ASSERT_TRUE(status.IsUnmatched());
    ASSERT_EQ(layout.GetObjectNum(), 0);
    ASSERT_TRUE(layout.GetObjects().empty());
}

TEST_F(SinglePageObjectTest, ConstructFrom) {
    // make an object
    Object obj;
    Status status = layout.NewObject(Layout::WriteOptions(nullptr),
                                     model::ModelManager::GetModelId<DummyModel>(), key, &obj);
    ASSERT_TRUE(status.ok());
    auto model = obj.Model<model::DummyModel>();
    model->Set("key1", "value1");
    model->Set("key2", "value2");
    model->Set("key3", "value3");

    // make page
    PageIndex page;
    page.page_size = 100;
    page.address = 200;
    page.page_in_log = 1;
    status = layout.NewPage(Layout::WriteOptions(&allocator), page);
    ASSERT_TRUE(status.ok());

    // create a new layout and construct
    auto* new_buf = LayoutManager::GenRawLayoutBuf(
        LayoutType::kSinglePageObject, &allocator,
        Object::ComputeRawObjectSize(key.size(), model::ModelManager::GetModelId<DummyModel>()));
    auto new_layout = SinglePageObject(new_buf, &allocator);

    new_layout.ConstructFrom({page}, {obj}, 0);
    BYTE_DEFER(new_layout.Destroy());
    ASSERT_EQ(new_layout.GetObjects().size(), 1);
    ASSERT_EQ(new_layout.GetObjectNum(), 1);
    ASSERT_EQ(new_layout.GetPages().size(), 1);
    ASSERT_EQ(new_layout.GetPageNum(), 1);

    // check object
    Object new_obj;
    status = new_layout.FindObject(key, &new_obj);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(new_obj.KeyLen(), key.size());
    ASSERT_EQ(new_obj.Key(), key);
    ASSERT_EQ(new_obj.ModelId(), model::ModelManager::GetModelId<DummyModel>());
    ASSERT_FALSE(new_obj.Trivial());
    ASSERT_EQ(new_layout.GetObjectNum(), 1);
    ASSERT_EQ(new_layout.GetObjects().front().RawBuf(), new_obj.RawBuf());
    model = new_obj.Model<model::DummyModel>();
    ASSERT_EQ(model->Size(), 3);
    ASSERT_EQ(model->Get("key1"), "value1");
    ASSERT_EQ(model->Get("key2"), "value2");
    ASSERT_EQ(model->Get("key3"), "value3");

    // check page
    PageIndex new_page;
    status = new_layout.FindPage(0, 0, &new_page);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(new_page.object_id, 0);
    ASSERT_EQ(new_page.page_id, 0);
    ASSERT_EQ(new_page.page_size, 100);
    ASSERT_EQ(new_page.address, 200);
    ASSERT_TRUE(new_page.page_in_log);
    ASSERT_FALSE(new_page.dirty);
}

class MultiPageObjectTest : public testing::Test {
 public:
    void SetUp() override {
        buf = LayoutManager::GenRawLayoutBuf(LayoutType::kMultiPageObject, &allocator, 0);
        layout = MultiPageObject(buf, &allocator);
        layout.ConstructFrom({}, {}, 0);
        ASSERT_EQ(layout.CurrentLayout(), LayoutType::kMultiPageObject);
    }

    void TearDown() override { layout.Destroy(); }

 protected:
    uint8_t* buf = nullptr;
    MultiPageObject layout;
    Allocator allocator;
    std::string key = "test";
};

TEST_F(MultiPageObjectTest, Empty) {
    ASSERT_EQ(layout.GetPageNum(), 0);
    ASSERT_TRUE(layout.GetPages().empty());
    ASSERT_EQ(layout.GetObjectNum(), 0);
    ASSERT_TRUE(layout.GetObjects().empty());
}

TEST_F(MultiPageObjectTest, PageManagement) {
    // no page now
    PageIndex page;
    Status status = layout.FindPage(0, 0, nullptr);
    ASSERT_TRUE(status.IsNotFound());

    // prefill some pages
    size_t prefill_num = 10;
    for (size_t i = 0; i < prefill_num; ++i) {
        PageIndex page;
        page.object_id = i;
        page.page_id = i;
        page.page_size = i;
        page.address = i;
        page.page_in_log = true;
        Status status = layout.NewPage(Layout::WriteOptions(&allocator), page);
        ASSERT_TRUE(status.ok());
    }

    // find not exist page
    status = layout.FindPage(128, 128, &page);
    ASSERT_TRUE(status.IsNotFound());

    // new page
    page.object_id = 128;
    page.page_id = 128;
    page.page_size = 10;
    page.address = 20;
    status = layout.NewPage(Layout::WriteOptions(&allocator), page);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(layout.GetPageNum(), prefill_num + 1);

    // dup new page
    status = layout.NewPage(Layout::WriteOptions(&allocator), page);
    ASSERT_TRUE(status.IsAlreadyExists());

    {
        // dup new page, but update_if_exist is set
        Layout::WriteOptions opts(&allocator);
        opts.update_if_exist = true;
        page.page_size = 111111;
        status = layout.NewPage(opts, page);
        ASSERT_TRUE(status.ok());

        PageIndex new_page;
        status = layout.FindPage(page.object_id, page.page_id, &new_page);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(new_page.page_size, page.page_size);
    }

    // change page
    page.dirty = 1;
    status = layout.UpdatePage(page.object_id, page.page_id, page);
    ASSERT_TRUE(status.ok());

    // find page and check page correctness
    PageIndex new_page;
    status = layout.FindPage(page.object_id, page.page_id, &new_page);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(new_page.object_id, page.object_id);
    ASSERT_EQ(new_page.page_id, page.page_id);
    ASSERT_EQ(new_page.page_size, page.page_size);
    ASSERT_EQ(new_page.address, page.address);
    ASSERT_EQ(new_page.page_in_log, page.page_in_log);
    ASSERT_EQ(new_page.dirty, page.dirty);

    // delete page
    status = layout.DeletePage(page.object_id, page.page_id);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(layout.GetPageNum(), prefill_num);
    ASSERT_EQ(layout.GetPages().size(), prefill_num);

    // dup delete page
    status = layout.DeletePage(page.object_id, page.page_id);
    ASSERT_TRUE(status.IsNotFound());

    // there is no page now
    status = layout.FindPage(page.object_id, page.page_id, nullptr);
    ASSERT_TRUE(status.IsNotFound());

    // clear pages
    std::vector<PageIndex> pages = layout.GetPages();
    for (size_t i = 0; i < pages.size(); ++i) {
        status = layout.DeletePage(pages[i].object_id, pages[i].page_id);
        if (pages.size() - i <= 2) {
            ASSERT_TRUE(status.IsUnmatched());
            break;
        } else {
            ASSERT_TRUE(status.ok()) << i << " " << pages.size();
        }
    }
}

TEST_F(MultiPageObjectTest, ObjectManagement) {
    // no object now
    Object obj;
    Status status = layout.FindObject("test", nullptr);
    ASSERT_TRUE(status.IsNotFound());

    // prefill some objects
    size_t prefill_num = 10;
    for (size_t i = 0; i < prefill_num; ++i) {
        Status status = layout.NewObject(Layout::WriteOptions(nullptr),
                                         model::ModelManager::GetModelId<DummyModel>(),
                                         std::to_string(i), nullptr);
        ASSERT_TRUE(status.ok());
    }

    // new object
    status = layout.NewObject(Layout::WriteOptions(nullptr),
                              model::ModelManager::GetModelId<DummyModel>(), key, &obj);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(obj.KeyLen(), key.size());
    ASSERT_EQ(obj.Key(), key);
    ASSERT_EQ(obj.ModelId(), model::ModelManager::GetModelId<DummyModel>());
    ASSERT_FALSE(obj.Trivial());
    ASSERT_EQ(layout.GetObjectNum(), prefill_num + 1);

    // dup new object
    status = layout.NewObject(Layout::WriteOptions(nullptr),
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
    status = layout.FindObject(key, &obj);
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
    status = layout.DeleteObject(key);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(layout.GetObjectNum(), prefill_num);

    // dup delete object
    status = layout.DeleteObject(key);
    ASSERT_TRUE(status.IsNotFound());

    // there is no object now
    status = layout.FindObject(key, &obj);
    ASSERT_TRUE(status.IsNotFound());

    // clear objects
    std::vector<Object> objects = layout.GetObjects();
    for (size_t i = 0; i < objects.size(); ++i) {
        status = layout.DeleteObject(objects[i].Key());
        if (i + 1 == objects.size() || objects[i + 1].ObjectId() == 0) {
            ASSERT_TRUE(status.IsUnmatched());
            break;
        } else {
            ASSERT_TRUE(status.ok()) << i << " " << objects.size();
        }
    }
}

TEST_F(MultiPageObjectTest, ClearPages) {
    {
        BYTE_DEFER({
            layout.ClearObjects();
            layout.ClearPages();
        });

        // prefill some pages
        size_t prefill_num = 10;
        for (size_t i = 0; i < prefill_num; ++i) {
            PageIndex page;
            page.object_id = i;
            Status status = layout.NewPage(Layout::WriteOptions(&allocator), page);
            ASSERT_TRUE(status.ok());
        }

        // prefill some objects
        for (size_t i = 0; i < prefill_num; ++i) {
            Status status = layout.NewObject(Layout::WriteOptions(nullptr),
                                             model::ModelManager::GetModelId<DummyModel>(),
                                             std::to_string(i), nullptr);
            ASSERT_TRUE(status.ok());
        }

        // clear objects
        Status status = layout.ClearPages();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(layout.GetPageNum(), 0);
        ASSERT_TRUE(layout.GetPages().empty());
    }

    {
        BYTE_DEFER({
            layout.ClearObjects();
            layout.ClearPages();
        });

        // prefill some pages
        size_t prefill_num = 10;
        for (size_t i = 0; i < prefill_num; ++i) {
            PageIndex page;
            page.object_id = i;
            Status status = layout.NewPage(Layout::WriteOptions(&allocator), page);
            ASSERT_TRUE(status.ok());
        }

        // clear objects
        Status status = layout.ClearPages();
        ASSERT_TRUE(status.IsUnmatched());
        ASSERT_EQ(layout.GetPageNum(), 0);
        ASSERT_TRUE(layout.GetPages().empty());
    }
}

TEST_F(MultiPageObjectTest, ClearObjects) {
    {
        BYTE_DEFER({
            layout.ClearObjects();
            layout.ClearPages();
        });

        // prefill some objects
        size_t prefill_num = 10;
        for (size_t i = 0; i < prefill_num; ++i) {
            Status status = layout.NewObject(Layout::WriteOptions(nullptr),
                                             model::ModelManager::GetModelId<DummyModel>(),
                                             std::to_string(i), nullptr);
            ASSERT_TRUE(status.ok());
        }

        // prefill some pages
        for (size_t i = 0; i < prefill_num; ++i) {
            PageIndex page;
            page.object_id = i;
            Status status = layout.NewPage(Layout::WriteOptions(&allocator), page);
            ASSERT_TRUE(status.ok());
        }

        // clear objects
        Status status = layout.ClearObjects();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(layout.GetObjectNum(), 0);
        ASSERT_TRUE(layout.GetObjects().empty());
    }

    {
        BYTE_DEFER({
            layout.ClearObjects();
            layout.ClearPages();
        });

        // prefill some objects
        size_t prefill_num = 10;
        for (size_t i = 0; i < prefill_num; ++i) {
            Status status = layout.NewObject(Layout::WriteOptions(nullptr),
                                             model::ModelManager::GetModelId<DummyModel>(),
                                             std::to_string(i), nullptr);
            ASSERT_TRUE(status.ok());
        }

        // clear objects
        Status status = layout.ClearObjects();
        ASSERT_TRUE(status.IsUnmatched());
        ASSERT_EQ(layout.GetObjectNum(), 0);
        ASSERT_TRUE(layout.GetObjects().empty());
    }
}

TEST_F(MultiPageObjectTest, ConstructFrom) {
    // prefill pages and objects
    size_t prefill_num = 10;
    for (size_t i = 0; i < prefill_num; ++i) {
        PageIndex page;
        page.object_id = i;
        page.page_id = i;
        page.page_size = i;
        page.address = i;
        page.page_in_log = true;
        Status status = layout.NewPage(Layout::WriteOptions(&allocator), page);
        ASSERT_TRUE(status.ok());
    }
    for (size_t i = 0; i < prefill_num; ++i) {
        Status status = layout.NewObject(Layout::WriteOptions(nullptr),
                                         model::ModelManager::GetModelId<DummyModel>(),
                                         std::to_string(i), nullptr);
        ASSERT_TRUE(status.ok());
    }

    // make an sentry object
    Object obj;
    Status status = layout.NewObject(Layout::WriteOptions(nullptr),
                                     model::ModelManager::GetModelId<DummyModel>(), key, &obj);
    ASSERT_TRUE(status.ok());
    auto model = obj.Model<model::DummyModel>();
    model->Set("key1", "value1");
    model->Set("key2", "value2");
    model->Set("key3", "value3");

    // make a sentry page
    PageIndex page;
    page.object_id = 128;
    page.page_id = 128;
    page.page_size = 100;
    page.address = 200;
    page.page_in_log = true;
    status = layout.NewPage(Layout::WriteOptions(&allocator), page);
    ASSERT_TRUE(status.ok());

    // create a new layout and construct
    auto* new_buf = LayoutManager::GenRawLayoutBuf(LayoutType::kMultiPageObject, &allocator, 0);
    auto new_layout = MultiPageObject(new_buf, &allocator);

    new_layout.ConstructFrom(layout.GetPages(), layout.GetObjects(), 0);
    BYTE_DEFER(new_layout.Destroy());
    ASSERT_EQ(new_layout.GetObjects().size(), prefill_num + 1);
    ASSERT_EQ(new_layout.GetObjectNum(), prefill_num + 1);
    ASSERT_EQ(new_layout.GetPages().size(), prefill_num + 1);
    ASSERT_EQ(new_layout.GetPageNum(), prefill_num + 1);

    // check object
    Object new_obj;
    status = new_layout.FindObject(key, &new_obj);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(new_obj.KeyLen(), key.size());
    ASSERT_EQ(new_obj.Key(), key);
    ASSERT_EQ(new_obj.ModelId(), model::ModelManager::GetModelId<DummyModel>());
    ASSERT_FALSE(new_obj.Trivial());
    ASSERT_EQ(new_layout.GetObjectNum(), prefill_num + 1);
    model = new_obj.Model<model::DummyModel>();
    ASSERT_EQ(model->Size(), 3);
    ASSERT_EQ(model->Get("key1"), "value1");
    ASSERT_EQ(model->Get("key2"), "value2");
    ASSERT_EQ(model->Get("key3"), "value3");

    // check page
    PageIndex new_page;
    status = new_layout.FindPage(128, 128, &new_page);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(new_page.object_id, 128);
    ASSERT_EQ(new_page.page_id, 128);
    ASSERT_EQ(new_page.page_size, 100);
    ASSERT_EQ(new_page.address, 200);
    ASSERT_TRUE(new_page.page_in_log);
    ASSERT_FALSE(new_page.dirty);
}

}  // namespace test
}  // namespace partition
}  // namespace bcache2
