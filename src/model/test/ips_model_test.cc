// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "model/ips_model.h"

#include <gtest/gtest.h>

#include <iostream>
#include <map>

#include "absl/container/btree_map.h"
#include "common/allocator.h"
#include "partition/cmd_context.h"
#include "partition/storage/object.h"
#include "partition/storage/op_logger.h"

namespace bcache2 {
namespace model {
namespace test {

static MetricsManager metrics_manager{{}, ""};

class DummyOplogger3 : public partition::OpLogger {
 public:
    DummyOplogger3() : OpLogger(nullptr, nullptr, nullptr, &metrics_manager) {}
    void WriteKvLog(partition::CmdContext* ctx, uint64_t slot_id, uint64_t object_id,
                    const absl::string_view& object_key, uint16_t model_id, std::string key,
                    std::string value, model::Property property) override {
        ctx_vec_.emplace_back(std::make_pair(key, value));
    }
    std::vector<std::pair<std::string, std::string>> ctx_vec_;
};

static Allocator allocator;

TEST(IpsModelTest, HighModel) {
    std::string object_key = "test_key";
    std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
        object_key.size(), model::ModelManager::GetModelId<IpsModel>())]);
    std::unique_ptr<DummyOplogger3> op_logger(new DummyOplogger3());

    partition::Object obj(0, buf.get());
    obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<IpsModel>(), object_key);

    partition::CmdContext ctx;
    ctx.object = obj;
    ctx.op_logger = op_logger.get();

    std::vector<partition::PageInfo> pages;
    std::string page;
    uint64_t cnt = 3;
    // uint32_t page_id_start = 1;
    uint32_t cluster_id_start = 1000;
    uint64_t ts_start = 10000;
    google::protobuf::io::StringOutputStream output(&page);
    google::protobuf::io::CodedOutputStream stream(&output);

    for (uint64_t i = 1; i < cnt; i++) {
        auto cur_val = std::to_string(ts_start + i);
        std::string key = SerializeToString<std::string>(cur_val);
        std::string value = SerializeToString<std::string>(cur_val);
        // std::string key = cur_val;
        // std::string value = cur_val;
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
    auto ips_model = obj.Model<IpsModel>();
    for (uint64_t i = 1; i < cnt; i++) {
        // std::string key = SerializeToString<uint64_t>(ts_start + i);
        // std::string expected_value = SerializeToString<std::string>(cur_val);
        std::string key = std::to_string(ts_start + i);
        std::string expected_value = std::to_string(ts_start + i);
        std::string val;
        ips_model->OrSet().Get(key, &val);
        ASSERT_EQ(expected_value, val);
    }
}

TEST(IpsModelTest, IpsOrSet) {
    // Pass WriteKvLog in PersistentMap::Put
    std::string object_key = "test_key";
    std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
        object_key.size(), model::ModelManager::GetModelId<IpsModel>())]);
    std::unique_ptr<DummyOplogger3> op_logger(new DummyOplogger3());
    partition::Object obj(0, buf.get());
    obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<IpsModel>(), object_key);
    partition::CmdContext ctx;
    ctx.object = obj;
    ctx.op_logger = op_logger.get();

    auto btree = BtreeMap<std::string, std::pair<Property, std::string>>(
        std::less<std::string>(), Allocator::StlWrapper<std::pair<const std::string, std::string>>(
                                      Allocator::DefaultAllocator()));
    uint64_t max_timestamp = 0;
    auto map = PersistentMap<std::string, std::string>(&btree, &max_timestamp);
    auto orset = IpsOrSet(&map);

    std::vector<std::string> keys;
    PersistentMap<std::string, std::string>::IterateFunc append_iter =
        [&keys](const std::string& key, const std::string& value) -> bool {
        keys.push_back(key);
        return true;
    };

    // empty case
    {
        // Get, exepct not found
        std::string value;
        Status ret = orset.Get("key", &value);
        ASSERT_TRUE(ret.IsNotFound());

        // GetMaxItem, expect not found
        std::string key;
        ret = orset.GetMaxItem(&key, &value);
        ASSERT_TRUE(ret.IsNotFound());

        // Size, expect size == 0
        uint64_t size = 100;
        orset.Size(&size);
        ASSERT_EQ(size, 0);

        // Scan, expect iter_func not called
        keys.clear();
        ret = orset.Scan("", append_iter);
        ASSERT_TRUE(ret.IsNotFound());
        ASSERT_EQ(keys.size(), 0);

        // ScanBackward, expect iter_func not called
        keys.clear();
        ret = orset.ScanBackward("", append_iter);
        ASSERT_TRUE(ret.IsNotFound());
        ASSERT_EQ(keys.size(), 0);
    }

    // empty value case
    {
        Status ret = orset.Set(&ctx, "key9", "");
        ASSERT_TRUE(ret.IsOK());

        // GetMaxItem, expect not found
        std::string key;
        std::string value;
        ret = orset.GetMaxItem(&key, &value);
        ASSERT_TRUE(ret.IsNotFound());

        // Scan, expect iter_func not called
        keys.clear();
        ret = orset.Scan("", append_iter);
        ASSERT_TRUE(ret.IsNotFound());
        ASSERT_EQ(keys.size(), 0);

        // ScanBackward, expect iter_func not called
        keys.clear();
        ret = orset.ScanBackward("", append_iter);
        ASSERT_TRUE(ret.IsNotFound());
        ASSERT_EQ(keys.size(), 0);
    }

    // regular case
    {
        // Set key1, key2 and key3
        Status ret = orset.Set(&ctx, "key1", "value1");
        ASSERT_TRUE(ret.IsOK());
        ret = orset.Set(&ctx, "key2", "value2");
        ASSERT_TRUE(ret.IsOK());
        ret = orset.Set(&ctx, "key3", "value3");
        ASSERT_TRUE(ret.IsOK());

        // GetMaxItem, expect get key3 and skip key9
        std::string key;
        std::string value;
        ret = orset.GetMaxItem(&key, &value);
        ASSERT_TRUE(ret.IsOK());
        ASSERT_EQ(key.compare("key3"), 0);
        ASSERT_EQ(value.compare("value3"), 0);

        // Size, expect size == 4
        uint64_t size = 100;
        orset.Size(&size);
        ASSERT_EQ(size, 4);

        // Get key4, exepct not found
        std::string value4;
        ret = orset.Get("key4", &value4);
        ASSERT_TRUE(ret.IsNotFound());

        // Get key1, exepct found
        std::string value1;
        ret = orset.Get("key1", &value1);
        ASSERT_TRUE(ret.IsOK());
        ASSERT_EQ(value1.compare("value1"), 0);
    }

    // scan case
    {
        // Scan from key2, expect key2 included
        keys.clear();
        Status ret = orset.Scan("key2", append_iter);
        ASSERT_TRUE(ret.IsOK());
        ASSERT_EQ(keys.size(), 2);
        ASSERT_EQ(keys[0].compare("key2"), 0);
        ASSERT_EQ(keys[1].compare("key3"), 0);

        // Scan from "", expect all keys included
        keys.clear();
        ret = orset.Scan("", append_iter);
        ASSERT_TRUE(ret.IsOK());
        ASSERT_EQ(keys.size(), 3);
        ASSERT_EQ(keys[0].compare("key1"), 0);
        ASSERT_EQ(keys[1].compare("key2"), 0);
        ASSERT_EQ(keys[2].compare("key3"), 0);

        // BackScan from key2, expect key2 included
        keys.clear();
        ret = orset.ScanBackward("key2", append_iter);
        ASSERT_TRUE(ret.IsOK());
        ASSERT_EQ(keys.size(), 2);
        ASSERT_EQ(keys[0].compare("key2"), 0);
        ASSERT_EQ(keys[1].compare("key1"), 0);

        // BackScan from "", expect key2 included
        keys.clear();
        ret = orset.ScanBackward("", append_iter);
        ASSERT_TRUE(ret.IsOK());
        ASSERT_EQ(keys.size(), 3);
        ASSERT_EQ(keys[0].compare("key3"), 0);
        ASSERT_EQ(keys[1].compare("key2"), 0);
        ASSERT_EQ(keys[2].compare("key1"), 0);
    }
}

}  // namespace test
}  // namespace model
}  // namespace bcache2
