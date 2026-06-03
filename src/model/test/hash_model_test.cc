// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "model/hash_model.h"

#include <gtest/gtest.h>

#include <iostream>
#include <map>

#include "absl/container/btree_map.h"
#include "butil/fast_rand.h"
#include "model/common.h"
#include "model/feature_model.h"
#include "model/property.h"
#include "partition/cmd_context.h"
#include "partition/storage/object.h"
#include "partition/storage/op_logger.h"
#include "partition/storage/page_store.h"

DECLARE_uint64(model_max_page_id);
DECLARE_uint64(model_size_tiered_compaction_max_ignore_bucket_size);
DECLARE_uint64(model_size_tiered_compaction_min_bucket_size);
DECLARE_uint64(model_size_tiered_compaction_bucket_step);
DECLARE_uint64(model_size_tiered_compaction_max_threshold);

namespace bcache2 {
namespace model {
namespace test {

static MetricsManager metrics_manager{{}, ""};

class DummyOplogger : public partition::OpLogger {
 public:
    DummyOplogger() : OpLogger(nullptr, nullptr, nullptr, &metrics_manager) {}
    void WriteKvLog(partition::CmdContext* ctx, uint64_t slot_id, uint64_t object_id,
                    const absl::string_view& object_key, uint16_t model_id, std::string key,
                    std::string value, model::Property property) override {
        ctx_vec_.emplace_back(std::make_pair(key, value));
    }
    std::vector<std::pair<std::string, std::string>> ctx_vec_;
};

static Allocator allocator;

TEST(HashModelTest, HighModel) {
    std::string object_key = "test_key";
    std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
        object_key.size(), model::ModelManager::GetModelId<HashModel>())]);
    std::unique_ptr<DummyOplogger> op_logger(new DummyOplogger());

    partition::Object obj(0, buf.get());
    obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<HashModel>(), object_key);
    model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator, {});

    partition::CmdContext ctx;
    ctx.object = obj;
    ctx.op_logger = op_logger.get();

    std::string key = "key";
    auto my_hash_orSet = ctx.object.Model<HashModel>()->OrSet();
    my_hash_orSet.Set(&ctx, key, "value");
    std::string newKey = SerializeToString(key);
    std::vector<std::pair<std::string, std::string>>::iterator my_it;
    for (my_it = op_logger->ctx_vec_.begin(); my_it != op_logger->ctx_vec_.end(); my_it++) {
        if (my_it->first.compare(newKey) == 0) {
            std::cout << "val is " << my_it->second << std::endl;
            break;
        }
    }
    ASSERT_TRUE(my_it != op_logger->ctx_vec_.end());

    std::string val = "";
    my_hash_orSet.Get(key, &val);
    ASSERT_EQ(val.compare("value"), 0);
}

TEST(HashModelTest, LowModel) {
    std::string object_key = "test_key";
    std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
        object_key.size(), model::ModelManager::GetModelId<HashModel>())]);
    std::unique_ptr<DummyOplogger> op_logger(new DummyOplogger());

    partition::Object obj(0, buf.get());
    obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<HashModel>(), object_key);

    partition::CmdContext ctx;
    ctx.object = obj;
    ctx.op_logger = op_logger.get();

    std::vector<partition::PageInfo> pages;
    std::string page;
    uint64_t cnt = 2;
    uint32_t cluster_id_start = 1000;
    uint64_t ts_start = 10000;
    std::string cur_key = "";
    std::string cur_val("hash val");

    google::protobuf::io::StringOutputStream output(&page);
    google::protobuf::io::CodedOutputStream stream(&output);

    for (uint64_t i = 1; i < cnt; i++) {
        cur_key = std::to_string(i);
        std::string key = SerializeToString<std::string>(cur_key);
        std::string value = SerializeToString<std::string>(cur_val);
        WriteKvItemToStream(&stream, key, value, cluster_id_start + i, 0, ts_start);
    }
    stream.Trim();
    partition::PageInfo page_info;
    page_info.data = page;
    pages.emplace_back(page_info);

    Status st =
        model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator, std::move(pages));
    std::cout << "model1 st is " << st.ToString() << std::endl;
    ASSERT_TRUE(st.ok());
    auto myHashModel1 = obj.Model<HashModel>();
    for (uint64_t i = 1; i < cnt; i++) {
        std::string val = "";
        st = myHashModel1->OrSet().Get(std::to_string(i), &val);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(val.compare(cur_val), 0);
    }

    std::string apply_key = "777";
    std::string apply_val1 = "v777";
    Property pt;
    Property new_pt;
    pt.page_id = 0;
    pt.cluster_id = 99;
    pt.deleted = false;
    pt.timestamp = 999;
    std::string newKey = SerializeToString<std::string>(apply_key);
    std::string newVal = SerializeToString<std::string>(apply_val1);

    st = myHashModel1->Apply(&allocator, newKey, newVal, pt.cluster_id, pt.timestamp, pt.deleted);
    std::cout << "model1 apply st is " << st.ToString() << std::endl;
    ASSERT_TRUE(st.ok());

    std::string apply_val2 = "";
    st = myHashModel1->OrSet().Get(apply_key, &apply_val2);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(apply_val2.compare(apply_val1), 0);

    st = myHashModel1->OrSet().GetProperty(apply_key, &new_pt);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(new_pt.page_id, pt.page_id);
    ASSERT_EQ(new_pt.cluster_id, pt.cluster_id);
    ASSERT_EQ(new_pt.timestamp, pt.timestamp);
    ASSERT_EQ(new_pt.deleted, pt.deleted);

    std::vector<partition::LogItem> logs;
    auto my_pages = myHashModel1->DumpNewPages({}, logs);
    ASSERT_EQ(my_pages.size(), 1);
    page_info.data = my_pages[0].second;
    std::unique_ptr<uint8_t[]> buf2(new uint8_t[partition::Object::ComputeRawObjectSize(
        object_key.size(), model::ModelManager::GetModelId<HashModel>())]);
    partition::Object obj2(0, buf2.get());
    obj2.ConstructWithValues(buf2.get(), model::ModelManager::GetModelId<HashModel>(), object_key);
    st = model::ModelManager::Init(obj2.ModelId(), obj2.RawModelBuf(), &allocator, {page_info});
    std::cout << "model2 init st is " << st.ToString() << std::endl;
    auto myHashModel2 = obj2.Model<HashModel>();
    ASSERT_TRUE(st.ok());
    std::string apply_val3 = "";
    myHashModel2->OrSet().Get(apply_key, &apply_val3);
    std::cout << "after model2 init, apply_val is " << apply_val3 << std::endl;
    ASSERT_EQ(apply_val3.compare(apply_val1), 0);
}

TEST(HashModelTest, DumpNewPages) {
    {
        // dump all (first dump)
        std::string object_key = "test_key";
        std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
            object_key.size(), model::ModelManager::GetModelId<HashModel>())]);
        std::unique_ptr<DummyOplogger> op_logger(new DummyOplogger());

        partition::Object obj(0, buf.get());
        obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<HashModel>(),
                                object_key);
        model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator, {});

        Controller ctrl;
        partition::CmdContext ctx;
        ctx.object = obj;
        ctx.op_logger = op_logger.get();
        ctx.ctrl = &ctrl;

        auto model = ctx.object.Model<HashModel>();
        auto my_hash_orSet = model->OrSet();
        for (int i = 0; i < 1000; ++i) {
            my_hash_orSet.Set(&ctx, "key" + std::to_string(i), "key" + std::to_string(i));
        }

        std::vector<partition::LogItem> logs;
        auto new_pages = model->DumpNewPages({}, logs);
        ASSERT_EQ(new_pages.size(), 1);
        ASSERT_EQ(new_pages[0].first, 0);  // page_id is 0
        ASSERT_GT(new_pages[0].second.size(), 512);
    }

    {
        // dump all (total page size < FLAGS_model_size_tiered_compaction_min_bucket_size)
        std::string object_key = "test_key";
        std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
            object_key.size(), model::ModelManager::GetModelId<HashModel>())]);
        std::unique_ptr<DummyOplogger> op_logger(new DummyOplogger());

        partition::Object obj(0, buf.get());
        obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<HashModel>(),
                                object_key);
        model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator, {});

        partition::CmdContext ctx;
        ctx.object = obj;
        ctx.op_logger = op_logger.get();

        auto model = ctx.object.Model<HashModel>();
        auto my_hash_orSet = model->OrSet();
        for (int i = 0; i < 1000; ++i) {
            my_hash_orSet.Set(&ctx, "key" + std::to_string(i), "key" + std::to_string(i));
        }

        std::vector<partition::PageIndex> pages;
        for (int i = 0; i < 5; ++i) {
            partition::PageIndex page_index;
            page_index.page_id = i;
            page_index.page_size = FLAGS_model_size_tiered_compaction_min_bucket_size / 10;
            pages.emplace_back(page_index);
        }
        std::vector<partition::LogItem> logs;
        auto new_pages = model->DumpNewPages(pages, logs);
        ASSERT_EQ(new_pages.size(), 5);
        for (auto& new_page : new_pages) {
            if (new_page.first == 0) {
                // all data dump in page 0
                ASSERT_GT(new_page.second.size(), 512);
            } else {
                // delete other pages
                ASSERT_EQ(new_page.second.size(), 0);
            }
        }
    }

    {
        // dump all (page_id > max_page_id)
        std::string object_key = "test_key";
        std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
            object_key.size(), model::ModelManager::GetModelId<HashModel>())]);
        std::unique_ptr<DummyOplogger> op_logger(new DummyOplogger());

        partition::Object obj(0, buf.get());
        obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<HashModel>(),
                                object_key);
        model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator, {});

        partition::CmdContext ctx;
        ctx.object = obj;
        ctx.op_logger = op_logger.get();

        auto model = ctx.object.Model<HashModel>();
        auto my_hash_orSet = model->OrSet();
        for (int i = 0; i < 1000; ++i) {
            my_hash_orSet.Set(&ctx, "key" + std::to_string(i), "key" + std::to_string(i));
        }

        std::vector<partition::PageIndex> pages;
        for (int i = 0; i < FLAGS_model_max_page_id + 1; ++i) {
            partition::PageIndex page_index;
            page_index.page_id = i;
            page_index.page_size = 1;
            pages.emplace_back(page_index);
        }
        std::vector<partition::LogItem> logs;
        auto new_pages = model->DumpNewPages(pages, logs);
        ASSERT_EQ(new_pages.size(), FLAGS_model_max_page_id + 1);
        for (auto& new_page : new_pages) {
            if (new_page.first == 0) {
                // all data dump in page 0
                ASSERT_GT(new_page.second.size(), FLAGS_model_max_page_id);
            } else {
                // delete other pages
                ASSERT_EQ(new_page.second.size(), 0);
            }
        }
    }

    {
        // dump logs
        std::string object_key = "test_key";
        std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
            object_key.size(), model::ModelManager::GetModelId<HashModel>())]);
        std::unique_ptr<DummyOplogger> op_logger(new DummyOplogger());

        partition::Object obj(0, buf.get());
        obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<HashModel>(),
                                object_key);
        model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator, {});

        partition::CmdContext ctx;
        ctx.object = obj;
        ctx.op_logger = op_logger.get();

        auto model = ctx.object.Model<HashModel>();
        auto my_hash_orSet = model->OrSet();
        for (int i = 0; i < 1000; ++i) {
            my_hash_orSet.Set(&ctx, "key" + std::to_string(i), "key" + std::to_string(i));
        }

        std::vector<partition::PageIndex> pages;
        for (int i = 0; i < 10; ++i) {
            if (butil::fast_rand_in(0, 1) == 0) {
                partition::PageIndex page_index;
                page_index.page_id = i;
                page_index.page_size = FLAGS_model_size_tiered_compaction_min_bucket_size + 1;
                page_index.model_id = 3;
                pages.emplace_back(page_index);
            }
        }
        std::vector<partition::LogItem> logs;
        partition::LogItem item;
        item.log.set_key(SerializeToString(std::string("key1111")));
        item.log.set_value(SerializeToString(std::string("value1111")));
        logs.push_back(item);
        item.log.Clear();
        item.log.set_key(SerializeToString(std::string("key2222")));
        item.log.set_value(SerializeToString(std::string("value2222")));
        logs.push_back(item);
        item.log.Clear();
        item.log.set_meta_log(true);
        logs.push_back(item);
        auto new_pages = model->DumpNewPages(pages, logs);
        ASSERT_EQ(new_pages.size(), 1);

        // new object to check logs value
        std::vector<partition::PageInfo> pages_for_init;
        for (size_t i = 0; i < new_pages.size(); ++i) {
            partition::PageInfo page_info;
            page_info.header.set_version(i);
            page_info.header.set_page_id(new_pages[i].first);
            page_info.data = new_pages[i].second;
            pages_for_init.emplace_back(page_info);
        }
        std::unique_ptr<uint8_t[]> buf2(new uint8_t[partition::Object::ComputeRawObjectSize(
            object_key.size(), model::ModelManager::GetModelId<HashModel>())]);
        partition::Object obj2(0, buf2.get());
        obj2.ConstructWithValues(buf2.get(), model::ModelManager::GetModelId<HashModel>(),
                                 object_key);
        Status status = model::ModelManager::Init(obj2.ModelId(), obj2.RawModelBuf(), &allocator,
                                                  {pages_for_init});
        ASSERT_TRUE(status.ok()) << status;
        auto new_model = obj2.Model<HashModel>();
        ASSERT_EQ(new_model->data_.size(), 2);
        ASSERT_EQ(new_model->data_["key1111"].second, "value1111");
        ASSERT_EQ(new_model->data_["key2222"].second, "value2222");
    }

    {
        // dump logs (object_delete in logs)
        std::string object_key = "test_key";
        std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
            object_key.size(), model::ModelManager::GetModelId<HashModel>())]);
        std::unique_ptr<DummyOplogger> op_logger(new DummyOplogger());

        partition::Object obj(0, buf.get());
        obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<HashModel>(),
                                object_key);
        model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator, {});

        partition::CmdContext ctx;
        ctx.object = obj;
        ctx.op_logger = op_logger.get();

        auto model = ctx.object.Model<HashModel>();
        auto my_hash_orSet = model->OrSet();
        for (int i = 0; i < 1000; ++i) {
            my_hash_orSet.Set(&ctx, "key" + std::to_string(i), "key" + std::to_string(i));
        }

        std::vector<partition::PageIndex> pages;
        for (int i = 0; i < 10; ++i) {
            if (butil::fast_rand_in(0, 1) == 0) {
                partition::PageIndex page_index;
                page_index.page_id = i;
                page_index.page_size = FLAGS_model_size_tiered_compaction_min_bucket_size + 1;
                page_index.model_id = 3;
                pages.emplace_back(page_index);
            }
        }
        std::vector<partition::LogItem> logs;
        partition::LogItem item;
        item.log.set_key(SerializeToString(std::string("key1111")));
        item.log.set_value(SerializeToString(std::string("value1111")));
        logs.push_back(item);
        item.log.Clear();
        item.log.set_key(SerializeToString(std::string("key2222")));
        item.log.set_value(SerializeToString(std::string("value2222")));
        logs.push_back(item);
        item.log.Clear();
        item.log.set_object_deleted(true);
        logs.push_back(item);
        item.log.Clear();
        item.log.set_key(SerializeToString(std::string("new_key")));
        item.log.set_value(SerializeToString(std::string("new_value")));
        logs.push_back(item);
        auto new_pages = model->DumpNewPages(pages, logs);
        ASSERT_EQ(new_pages.size(), 1);

        // new object to check logs value
        std::vector<partition::PageInfo> pages_for_init;
        for (size_t i = 0; i < new_pages.size(); ++i) {
            partition::PageInfo page_info;
            page_info.header.set_version(i);
            page_info.header.set_page_id(new_pages[i].first);
            page_info.data = new_pages[i].second;
            pages_for_init.emplace_back(page_info);
        }
        std::unique_ptr<uint8_t[]> buf2(new uint8_t[partition::Object::ComputeRawObjectSize(
            object_key.size(), model::ModelManager::GetModelId<HashModel>())]);
        partition::Object obj2(0, buf2.get());
        obj2.ConstructWithValues(buf2.get(), model::ModelManager::GetModelId<HashModel>(),
                                 object_key);
        Status status = model::ModelManager::Init(obj2.ModelId(), obj2.RawModelBuf(), &allocator,
                                                  {pages_for_init});
        ASSERT_TRUE(status.ok()) << status;
        auto new_model = obj2.Model<HashModel>();
        ASSERT_EQ(new_model->data_.size(), 1);
        ASSERT_EQ(new_model->data_["new_key"].second, "new_value");
    }

    {
        // dump logs (all logs is meta_log)
        std::string object_key = "test_key";
        std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
            object_key.size(), model::ModelManager::GetModelId<HashModel>())]);
        std::unique_ptr<DummyOplogger> op_logger(new DummyOplogger());

        partition::Object obj(0, buf.get());
        obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<HashModel>(),
                                object_key);
        model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator, {});

        partition::CmdContext ctx;
        ctx.object = obj;
        ctx.op_logger = op_logger.get();

        auto model = ctx.object.Model<HashModel>();
        auto my_hash_orSet = model->OrSet();
        for (int i = 0; i < 1000; ++i) {
            my_hash_orSet.Set(&ctx, "key" + std::to_string(i), "key" + std::to_string(i));
        }

        std::vector<partition::PageIndex> pages;
        for (int i = 0; i < 10; ++i) {
            if (butil::fast_rand_in(0, 1) == 0) {
                partition::PageIndex page_index;
                page_index.page_id = i;
                page_index.page_size = FLAGS_model_size_tiered_compaction_min_bucket_size + 1;
                page_index.model_id = 3;
                pages.emplace_back(page_index);
            }
        }
        std::vector<partition::LogItem> logs;
        partition::LogItem item;
        item.log.set_meta_log(true);
        logs.push_back(item);
        logs.push_back(item);
        logs.push_back(item);
        logs.push_back(item);
        auto new_pages = model->DumpNewPages(pages, logs);
        ASSERT_EQ(new_pages.size(), 0);
    }

    {
        // dump logs (model is empty)
        std::string object_key = "test_key";
        std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
            object_key.size(), model::ModelManager::GetModelId<HashModel>())]);
        std::unique_ptr<DummyOplogger> op_logger(new DummyOplogger());

        partition::Object obj(0, buf.get());
        obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<HashModel>(),
                                object_key);
        model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator, {});

        partition::CmdContext ctx;
        ctx.object = obj;
        ctx.op_logger = op_logger.get();

        auto model = ctx.object.Model<HashModel>();
        auto my_hash_orSet = model->OrSet();
        std::vector<partition::LogItem> logs;
        auto new_pages = model->DumpNewPages({}, logs);
        ASSERT_EQ(new_pages.size(), 1);
    }
}

TEST(HashModelTest, Init) {
    std::vector<partition::PageInfo> pages;

    {
        // key and value
        std::string page;
        google::protobuf::io::StringOutputStream output(&page);
        google::protobuf::io::CodedOutputStream stream(&output);
        WriteKvItemToStream(&stream, SerializeToString(std::string("key1")),
                            SerializeToString(std::string("value1")), 0, false, 0);
        stream.Trim();
        partition::PageInfo page_info;
        page_info.data = page;
        page_info.header.set_version(0);
        page_info.header.set_page_id(0);
        pages.emplace_back(page_info);
    }

    {
        // key and value with tombstone
        std::string page;
        google::protobuf::io::StringOutputStream output(&page);
        google::protobuf::io::CodedOutputStream stream(&output);
        WriteKvItemToStream(&stream, SerializeToString(std::string("key2")),
                            SerializeToString(std::string("value20")), 0, false, 0);
        WriteKvItemToStream(&stream, SerializeToString(std::string("key2")),
                            SerializeToString(std::string("value21")), 0, true, 0);
        stream.Trim();
        partition::PageInfo page_info;
        page_info.data = page;
        page_info.header.set_version(1);
        page_info.header.set_page_id(1);
        pages.emplace_back(page_info);
    }

    {
        // key and value with tombstone and rewrite
        std::string page;
        google::protobuf::io::StringOutputStream output(&page);
        google::protobuf::io::CodedOutputStream stream(&output);
        WriteKvItemToStream(&stream, SerializeToString(std::string("key3")),
                            SerializeToString(std::string("value30")), 0, false, 0);
        WriteKvItemToStream(&stream, SerializeToString(std::string("key3")),
                            SerializeToString(std::string("value31")), 0, true, 0);
        WriteKvItemToStream(&stream, SerializeToString(std::string("key3")),
                            SerializeToString(std::string("value32")), 0, false, 0);
        stream.Trim();
        partition::PageInfo page_info;
        page_info.data = page;
        page_info.header.set_version(2);
        page_info.header.set_page_id(2);
        pages.emplace_back(page_info);
    }

    std::string object_key = "test_key";
    std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
        object_key.size(), model::ModelManager::GetModelId<HashModel>())]);
    std::unique_ptr<DummyOplogger> op_logger(new DummyOplogger());

    partition::Object obj(0, buf.get());
    obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<HashModel>(), object_key);
    model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator, pages);

    auto model = obj.Model<HashModel>();
    ASSERT_EQ(model->data_.size(), 2);
    ASSERT_EQ(model->data_["key1"].second, "value1");
    ASSERT_EQ(model->data_["key3"].second, "value32");
}

TEST(HashModelTest, CompactPagesHint) {
    {
        // all pages smaller than this number of bytes are put into the same bucket
        std::string object_key = "test_key";
        std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
            object_key.size(), model::ModelManager::GetModelId<HashModel>())]);
        std::unique_ptr<DummyOplogger> op_logger(new DummyOplogger());

        partition::Object obj(0, buf.get());
        obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<HashModel>(),
                                object_key);
        model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator, {});

        partition::CmdContext ctx;
        ctx.object = obj;
        ctx.op_logger = op_logger.get();

        auto model = ctx.object.Model<HashModel>();

        std::vector<partition::PageIndex> pages;
        for (int i = 0; i < 10; ++i) {
            partition::PageIndex page;
            page.page_id = i;
            page.page_size =
                butil::fast_rand_in(1UL, FLAGS_model_size_tiered_compaction_min_bucket_size - 1);
            pages.emplace_back(page);
        }
        auto res = model->CompactPagesHint(pages);
        // we only have single bucket
        ASSERT_EQ(res.first.size(), 10);
        ASSERT_TRUE(res.second);
    }

    {
        // single page
        std::string object_key = "test_key";
        std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
            object_key.size(), model::ModelManager::GetModelId<HashModel>())]);
        std::unique_ptr<DummyOplogger> op_logger(new DummyOplogger());

        partition::Object obj(0, buf.get());
        obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<HashModel>(),
                                object_key);
        model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator, {});

        partition::CmdContext ctx;
        ctx.object = obj;
        ctx.op_logger = op_logger.get();

        auto model = ctx.object.Model<HashModel>();

        std::vector<partition::PageIndex> pages;
        partition::PageIndex page;
        page.page_id = 0;
        page.page_size = 100000;
        pages.emplace_back(page);
        auto res = model->CompactPagesHint(pages);
        // we only have single bucket
        ASSERT_EQ(res.first.size(), 0);
    }

    {
        // tirvial
        std::string object_key = "test_key";
        std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
            object_key.size(), model::ModelManager::GetModelId<HashModel>())]);
        std::unique_ptr<DummyOplogger> op_logger(new DummyOplogger());

        partition::Object obj(0, buf.get());
        obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<HashModel>(),
                                object_key);
        model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator, {});

        partition::CmdContext ctx;
        ctx.object = obj;
        ctx.op_logger = op_logger.get();

        auto model = ctx.object.Model<HashModel>();

        std::vector<partition::PageIndex> pages;
        uint64_t rand_start = 10000;
        uint64_t rand_end = 20000;
        for (int i = 0; i < 20; ++i) {
            partition::PageIndex page;
            page.page_id = i;
            page.page_size = butil::fast_rand_in(rand_start, rand_end);
            rand_start -= 500;
            rand_end -= 500;
            pages.emplace_back(page);
        }
        model->CompactPagesHint(pages);
    }

    {
        // ignore bucket size > FLAGS_model_size_tiered_compaction_max_ignore_bucket_size
        std::string object_key = "test_key";
        std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
            object_key.size(), model::ModelManager::GetModelId<HashModel>())]);
        std::unique_ptr<DummyOplogger> op_logger(new DummyOplogger());

        partition::Object obj(0, buf.get());
        obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<HashModel>(),
                                object_key);
        model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator, {});

        partition::CmdContext ctx;
        ctx.object = obj;
        ctx.op_logger = op_logger.get();

        auto model = ctx.object.Model<HashModel>();

        std::vector<partition::PageIndex> pages;
        for (int i = 0; i < 100; ++i) {
            partition::PageIndex page;
            page.page_id = i;
            page.page_size = FLAGS_model_size_tiered_compaction_max_ignore_bucket_size + 1;
            pages.emplace_back(page);
        }
        auto res = model->CompactPagesHint(pages);
        ASSERT_TRUE(res.first.empty());
    }
}

TEST(HashModelTest, CompactPages) {
    std::vector<partition::PageInfo> pages;

    {  // key and value
        std::string page;
        google::protobuf::io::StringOutputStream output(&page);
        google::protobuf::io::CodedOutputStream stream(&output);
        WriteKvItemToStream(&stream, SerializeToString(std::string("key1")),
                            SerializeToString(std::string("value1")), 0, false, 0);
        stream.Trim();
        partition::PageInfo page_info;
        page_info.data = page;
        page_info.header.set_slot_id(100);
        page_info.header.set_object_id(10);
        page_info.header.set_page_id(10);
        page_info.header.set_key("key");
        page_info.header.set_model_id(20);
        page_info.header.set_oplog_sequence(11);
        page_info.header.set_version(0);
        pages.emplace_back(page_info);
    }

    {
        // key and value with tombstone
        std::string page;
        google::protobuf::io::StringOutputStream output(&page);
        google::protobuf::io::CodedOutputStream stream(&output);
        WriteKvItemToStream(&stream, SerializeToString(std::string("key2")),
                            SerializeToString(std::string("value20")), 0, false, 0);
        WriteKvItemToStream(&stream, SerializeToString(std::string("key2")),
                            SerializeToString(std::string("value21")), 0, true, 0);
        stream.Trim();
        partition::PageInfo page_info;
        page_info.data = page;
        page_info.header.set_slot_id(100);
        page_info.header.set_object_id(10);
        page_info.header.set_page_id(13);
        page_info.header.set_key("key");
        page_info.header.set_model_id(20);
        page_info.header.set_oplog_sequence(12);
        page_info.header.set_version(1);
        pages.emplace_back(page_info);
    }

    {
        // key and value with tombstone and rewrite
        std::string page;
        google::protobuf::io::StringOutputStream output(&page);
        google::protobuf::io::CodedOutputStream stream(&output);
        WriteKvItemToStream(&stream, SerializeToString(std::string("key3")),
                            SerializeToString(std::string("value30")), 0, false, 0);
        WriteKvItemToStream(&stream, SerializeToString(std::string("key3")),
                            SerializeToString(std::string("value31")), 0, true, 0);
        WriteKvItemToStream(&stream, SerializeToString(std::string("key3")),
                            SerializeToString(std::string("value32")), 0, false, 0);
        stream.Trim();
        partition::PageInfo page_info;
        page_info.data = page;
        page_info.header.set_slot_id(100);
        page_info.header.set_object_id(10);
        page_info.header.set_page_id(15);
        page_info.header.set_key("key");
        page_info.header.set_model_id(20);
        page_info.header.set_oplog_sequence(13);
        page_info.header.set_version(2);
        pages.emplace_back(page_info);
    }

    {
        // empty data after compaction
        std::string object_key = "test_key";
        std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
            object_key.size(), model::ModelManager::GetModelId<HashModel>())]);
        std::unique_ptr<DummyOplogger> op_logger(new DummyOplogger());

        partition::Object obj(0, buf.get());
        obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<HashModel>(),
                                object_key);
        model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator, {});

        partition::CmdContext ctx;
        ctx.object = obj;
        ctx.op_logger = op_logger.get();

        std::vector<partition::PageInfo> pages;
        std::string page;
        google::protobuf::io::StringOutputStream output(&page);
        google::protobuf::io::CodedOutputStream stream(&output);
        WriteKvItemToStream(&stream, SerializeToString(std::string("key1")),
                            SerializeToString(std::string("value1")), 0, false, 0);
        WriteKvItemToStream(&stream, SerializeToString(std::string("key1")),
                            SerializeToString(std::string("")), 0, true, 0);
        WriteKvItemToStream(&stream, SerializeToString(std::string("key1")),
                            SerializeToString(std::string("")), 0, true, 0);
        WriteKvItemToStream(&stream, SerializeToString(std::string("key1")),
                            SerializeToString(std::string("")), 0, true, 0);
        WriteKvItemToStream(&stream, SerializeToString(std::string("key1")),
                            SerializeToString(std::string("")), 0, true, 0);
        WriteKvItemToStream(&stream, SerializeToString(std::string("key1")),
                            SerializeToString(std::string("")), 0, true, 0);
        WriteKvItemToStream(&stream, SerializeToString(std::string("key1")),
                            SerializeToString(std::string("")), 0, true, 0);
        WriteKvItemToStream(&stream, SerializeToString(std::string("key1")),
                            SerializeToString(std::string("")), 0, true, 0);
        stream.Trim();
        partition::PageInfo page_info;
        page_info.data = page;
        page_info.header.set_slot_id(100);
        page_info.header.set_object_id(10);
        page_info.header.set_page_id(10);
        page_info.header.set_key("key");
        page_info.header.set_model_id(20);
        page_info.header.set_oplog_sequence(11);
        page_info.header.set_version(0);
        pages.emplace_back(page_info);

        auto model = ctx.object.Model<HashModel>();
        auto new_pages = model->CompactPages(pages, true);
        ASSERT_EQ(new_pages.size(), 1);  // we merge all pages to one now
        auto new_page = new_pages[0];
        ASSERT_EQ(new_page.data.size(), 0);
    }

    {
        // remove tombstone
        std::string object_key = "test_key";
        std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
            object_key.size(), model::ModelManager::GetModelId<HashModel>())]);
        std::unique_ptr<DummyOplogger> op_logger(new DummyOplogger());

        partition::Object obj(0, buf.get());
        obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<HashModel>(),
                                object_key);
        model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator, {});

        partition::CmdContext ctx;
        ctx.object = obj;
        ctx.op_logger = op_logger.get();

        auto model = ctx.object.Model<HashModel>();
        auto new_pages = model->CompactPages(pages, true);
        ASSERT_EQ(new_pages.size(), 1);  // we merge all pages to one now
        auto new_page = new_pages[0];
        ASSERT_EQ(new_page.header.slot_id(), 100);
        ASSERT_EQ(new_page.header.object_id(), 10);
        ASSERT_EQ(new_page.header.page_id(), 10);
        ASSERT_EQ(new_page.header.key(), "key");
        ASSERT_EQ(new_page.header.model_id(), 20);
        ASSERT_EQ(new_page.header.oplog_sequence(), 13);
        ASSERT_EQ(new_page.header.version(), 2);

        BtreeMap<std::string, std::pair<Property, std::string>,
                 std::allocator<std::pair<const std::string, std::string>>>
            merge_data;
        google::protobuf::io::ArrayInputStream input(new_page.data.data(), new_page.data.size());
        google::protobuf::io::CodedInputStream stream(&input);
        while (stream.CurrentPosition() < static_cast<int>(new_page.data.size())) {
            std::string key;
            std::string value;
            std::string key_data;
            std::string value_data;
            uint8_t cluster_id = 0;
            bool deleted = 0;
            uint64_t timestamp = 0;
            ASSERT_TRUE(ReadKvItemFromStream(&stream, &key_data, &value_data, &cluster_id, &deleted,
                                             &timestamp));
            ASSERT_TRUE(ParseFromString(key_data, &key));
            ASSERT_TRUE(ParseFromString(value_data, &value));

            Property property;
            property.page_id = new_page.header.page_id();
            property.cluster_id = cluster_id;
            property.deleted = deleted;
            property.timestamp = timestamp;
            merge_data[std::move(key)] = std::make_pair(property, std::move(value));
        }
        ASSERT_EQ(merge_data.size(), 2);
        ASSERT_FALSE(merge_data["key1"].first.deleted);
        ASSERT_FALSE(merge_data["key3"].first.deleted);
        ASSERT_EQ(merge_data["key3"].second, "value32");
        ASSERT_EQ(merge_data["key1"].second, "value1");
    }

    {
        // do not remove tombstone
        std::string object_key = "test_key";
        std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
            object_key.size(), model::ModelManager::GetModelId<HashModel>())]);
        std::unique_ptr<DummyOplogger> op_logger(new DummyOplogger());

        partition::Object obj(0, buf.get());
        obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<HashModel>(),
                                object_key);
        model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator, {});

        partition::CmdContext ctx;
        ctx.object = obj;
        ctx.op_logger = op_logger.get();

        auto model = ctx.object.Model<HashModel>();
        auto new_pages = model->CompactPages(pages, false);
        ASSERT_EQ(new_pages.size(), 1);  // we merge all pages to one now
        auto new_page = new_pages[0];
        ASSERT_EQ(new_page.header.slot_id(), 100);
        ASSERT_EQ(new_page.header.object_id(), 10);
        ASSERT_EQ(new_page.header.page_id(), 10);
        ASSERT_EQ(new_page.header.key(), "key");
        ASSERT_EQ(new_page.header.model_id(), 20);
        ASSERT_EQ(new_page.header.oplog_sequence(), 13);
        ASSERT_EQ(new_page.header.version(), 2);

        BtreeMap<std::string, std::pair<Property, std::string>,
                 std::allocator<std::pair<const std::string, std::string>>>
            merge_data;
        google::protobuf::io::ArrayInputStream input(new_page.data.data(), new_page.data.size());
        google::protobuf::io::CodedInputStream stream(&input);
        while (stream.CurrentPosition() < static_cast<int>(new_page.data.size())) {
            std::string key;
            std::string value;
            std::string key_data;
            std::string value_data;
            uint8_t cluster_id = 0;
            bool deleted = 0;
            uint64_t timestamp = 0;
            ASSERT_TRUE(ReadKvItemFromStream(&stream, &key_data, &value_data, &cluster_id, &deleted,
                                             &timestamp));
            ASSERT_TRUE(ParseFromString(key_data, &key));
            ASSERT_TRUE(ParseFromString(value_data, &value));

            Property property;
            property.page_id = new_page.header.page_id();
            property.cluster_id = cluster_id;
            property.deleted = deleted;
            property.timestamp = timestamp;
            merge_data[std::move(key)] = std::make_pair(property, std::move(value));
        }
        ASSERT_EQ(merge_data.size(), 3);
        ASSERT_FALSE(merge_data["key1"].first.deleted);
        ASSERT_TRUE(merge_data["key2"].first.deleted);
        ASSERT_FALSE(merge_data["key3"].first.deleted);
        ASSERT_EQ(merge_data["key3"].second, "value32");
        ASSERT_EQ(merge_data["key1"].second, "value1");
    }
}

TEST(HashModelTest, CompactionAIO) {
    auto model_max_page_id = FLAGS_model_max_page_id;
    FLAGS_model_max_page_id = 64;
    BYTE_DEFER(FLAGS_model_max_page_id = model_max_page_id);

    std::string object_key = "test_key";
    std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
        object_key.size(), model::ModelManager::GetModelId<HashModel>())]);
    std::unique_ptr<DummyOplogger> op_logger(new DummyOplogger());

    partition::Object obj(0, buf.get());
    obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<HashModel>(), object_key);
    model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator, {});

    partition::CmdContext ctx;
    Controller ctrl;
    ctx.ctrl = &ctrl;
    ctx.object = obj;
    ctx.op_logger = op_logger.get();

    std::string base_key = "base_key";
    std::string base_value = std::string(10240, 'v');

    auto model = ctx.object.Model<HashModel>();
    auto hash_orset = model->OrSet();
    hash_orset.Set(&ctx, base_key, base_value);
    std::vector<partition::LogItem> logs;
    std::unordered_map<uint16_t, partition::PageInfo> pages_info;
    std::unordered_map<uint16_t, partition::PageIndex> pages_index;

    std::map<std::string, std::string> base;
    base[base_key] = base_value;

    partition::LogItem item;
    item.log.set_key(SerializeToString(base_key));
    item.log.set_value(SerializeToString(base_value));
    item.log.set_deleted(false);
    logs.push_back(item);

    // key range: [key0, key100]
    for (int i = 0; i < 10000; ++i) {
        // 80% Set, 20% Del
        if (butil::fast_rand_in(0, 9) > 2) {
            std::string key = "key" + std::to_string(butil::fast_rand_in(0, 100));
            std::string value = std::to_string(butil::fast_rand());
            hash_orset.Set(&ctx, key, value);
            base[key] = value;
            partition::LogItem item;
            item.log.set_key(SerializeToString(key));
            item.log.set_value(SerializeToString(value));
            item.log.set_deleted(false);
            logs.push_back(item);
        } else {
            std::string key = "key" + std::to_string(butil::fast_rand_in(0, 100));
            hash_orset.Del(&ctx, key);
            base.erase(key);
            partition::LogItem item;
            item.log.set_key(SerializeToString(key));
            item.log.set_value(SerializeToString(std::string("")));
            item.log.set_deleted(true);
            logs.push_back(item);
        }

        LOG_DEBUG("Show Log").put("Log", logs.back().log.ShortDebugString()).put("Idx", i);

        // 10% dump and clear logs
        if (butil::fast_rand_in(0, 9) == 0) {
            LOG_DEBUG("Show Dump");
            std::vector<partition::PageIndex> tmp;
            for (auto& item : pages_index) {
                tmp.emplace_back(item.second);
            }
            auto new_pages =
                model::ModelManager::DumpNewPages(obj.ModelId(), obj.RawModelBuf(), tmp, logs);
            for (auto& new_page : new_pages) {
                if (new_page.second.empty()) {
                    // delete page
                    pages_info.erase(new_page.first);
                    pages_index.erase(new_page.first);
                } else {
                    // upsert page
                    pages_info[new_page.first].data = new_page.second;
                    pages_info[new_page.first].size = new_page.second.size();
                    pages_info[new_page.first].header.set_page_id(new_page.first);
                    pages_info[new_page.first].header.set_version(GetCurrentTimeInNs());
                    pages_index[new_page.first].page_id = new_page.first;
                    pages_index[new_page.first].page_size = new_page.second.size();
                }
            }
            logs.clear();
        }

        // 10% compaction
        if (butil::fast_rand_in(0, 9) == 0 && !pages_index.empty()) {
            LOG_DEBUG("Show Compaction");
            std::vector<partition::PageIndex> tmp;
            for (auto& item : pages_index) {
                tmp.emplace_back(item.second);
            }

            // get compaction hint
            auto res = model::ModelManager::CompactPagesHint(obj.ModelId(), tmp);
            if (!res.first.empty()) {
                // compaction pages
                auto page_indexes = res.first;
                std::vector<partition::PageInfo> pages;
                for (auto& page_index : page_indexes) {
                    pages.emplace_back(pages_info[page_index.page_id]);
                }
                std::sort(pages.begin(), pages.end(),
                          [](const partition::PageInfo& lhs, const partition::PageInfo& rhs) {
                              return lhs.header.version() < rhs.header.version();
                          });
                auto remove_tombstone = res.second;
                auto new_pages =
                    model::ModelManager::CompactPages(obj.ModelId(), pages, remove_tombstone);

                // delete old page
                for (auto& page : pages) {
                    pages_info.erase(page.header.page_id());
                    pages_index.erase(page.header.page_id());
                }

                // insert new page
                for (auto& new_page : new_pages) {
                    pages_info[new_page.header.page_id()] = new_page;
                    pages_index[new_page.header.page_id()].page_id = new_page.header.page_id();
                    pages_index[new_page.header.page_id()].page_size = new_page.data.size();
                }
            }
        }

        if (i % 100 == 0) {
            ASSERT_EQ(pages_info.size(), pages_index.size());
            std::cout << "PageNum: " << pages_index.size() << ", LogNum: " << logs.size()
                      << ", Process: " << i << "/10000\n";
        }

        // check value
        std::string object_key = "test_key";
        std::unique_ptr<uint8_t[]> buf2(new uint8_t[partition::Object::ComputeRawObjectSize(
            object_key.size(), model::ModelManager::GetModelId<HashModel>())]);

        partition::Object obj2(0, buf2.get());
        obj2.ConstructWithValues(buf2.get(), model::ModelManager::GetModelId<HashModel>(),
                                 object_key);

        std::vector<partition::PageInfo> pages;
        for (auto& item : pages_info) {
            pages.emplace_back(item.second);
        }
        std::sort(pages.begin(), pages.end(),
                  [](const partition::PageInfo& lhs, const partition::PageInfo& rhs) {
                      return lhs.header.version() < rhs.header.version();
                  });
        model::ModelManager::Init(obj2.ModelId(), obj2.RawModelBuf(), &allocator, pages);
        auto model2 = obj2.Model<HashModel>();
        for (auto& item : logs) {
            ASSERT_TRUE(model2
                            ->Apply(&allocator, item.log.key(), item.log.value(),
                                    item.log.cluster_id(), item.log.timestamp_ms(),
                                    item.log.deleted())
                            .ok());
        }
        ASSERT_EQ(base.size(), model2->data_.size()) << i;
        auto base_iter = base.begin();
        auto model_iter = model2->data_.begin();
        while (base_iter != base.end()) {
            ASSERT_EQ(base_iter->second, model_iter->second.second);
            ++base_iter;
            ++model_iter;
        }
    }
}

TEST(HashModelTest, TotalItem) {
    std::string object_key = "test_key";
    std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
        object_key.size(), model::ModelManager::GetModelId<HashModel>())]);
    std::unique_ptr<DummyOplogger> op_logger(new DummyOplogger());

    partition::Object obj(0, buf.get());
    obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<HashModel>(), object_key);

    std::vector<partition::PageInfo> pages_for_init;
    std::string page;
    uint32_t cluster_id_start = 1000;
    uint64_t ts_start = 10000;
    std::string cur_key = "";
    std::string cur_val("hash val");

    google::protobuf::io::StringOutputStream output(&page);
    google::protobuf::io::CodedOutputStream stream(&output);

    for (uint64_t i = 0; i < 100; i++) {
        cur_key = std::to_string(i);
        std::string key = SerializeToString<std::string>(cur_key);
        std::string value = SerializeToString<std::string>(cur_val);
        WriteKvItemToStream(&stream, key, value, cluster_id_start + i, 0, ts_start);
    }
    stream.Trim();
    partition::PageInfo page_info;
    page_info.data = page;
    pages_for_init.emplace_back(page_info);
    Status st = model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator,
                                          std::move(pages_for_init));
    ASSERT_TRUE(st.ok());

    auto model = obj.Model<HashModel>();
    ASSERT_EQ(model->total_item_, 100);

    {
        // incr dump
        std::vector<partition::PageIndex> pages;
        partition::PageIndex page_index;
        page_index.page_id = 0;
        page_index.model_id = 3;
        page_index.page_size = FLAGS_model_size_tiered_compaction_min_bucket_size * 2;
        pages.emplace_back(page_index);
        std::vector<partition::LogItem> logs;
        partition::LogItem item;
        item.log.set_key(SerializeToString(std::string("0")));
        item.log.set_value(SerializeToString(std::string("value1111")));
        logs.push_back(item);
        item.log.Clear();
        item.log.set_key(SerializeToString(std::string("0")));
        item.log.set_value(SerializeToString(std::string("value2222")));
        logs.push_back(item);
        item.log.Clear();
        item.log.set_meta_log(true);
        logs.push_back(item);
        model->DumpNewPages(pages, logs);
        ASSERT_EQ(model->total_item_, 102);
    }

    {
        // dump whole object
        std::vector<partition::PageIndex> pages;
        partition::PageIndex page_index;
        page_index.page_id = FLAGS_model_max_page_id;
        page_index.page_size = FLAGS_model_size_tiered_compaction_min_bucket_size * 2;
        pages.emplace_back(page_index);
        std::vector<partition::LogItem> logs;
        partition::LogItem item;
        item.log.set_key(SerializeToString(std::string("0")));
        item.log.set_value(SerializeToString(std::string("value1111")));
        logs.push_back(item);
        item.log.Clear();
        item.log.set_key(SerializeToString(std::string("0")));
        item.log.set_value(SerializeToString(std::string("value2222")));
        logs.push_back(item);
        item.log.Clear();
        item.log.set_meta_log(true);
        logs.push_back(item);
        model->DumpNewPages(pages, logs);
        ASSERT_EQ(model->total_item_, 100);
    }
}

}  // namespace test
}  // namespace model
}  // namespace bcache2
