
// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "model/feature_model.h"

#include <gtest/gtest.h>

#include <iostream>
#include <map>

#include "absl/container/btree_map.h"
#include "partition/cmd_context.h"

namespace bcache2 {
namespace model {
namespace test {

static MetricsManager metrics_manager{{}, ""};

class DummyOplogger2 : public partition::OpLogger {
 public:
    DummyOplogger2() : OpLogger(nullptr, nullptr, nullptr, &metrics_manager) {}
    void WriteKvLog(partition::CmdContext* ctx, uint64_t slot_id, uint64_t object_id,
                    const absl::string_view& object_key, uint16_t model_id, std::string key,
                    std::string value, model::Property property) override {
        ctx_vec_.emplace_back(std::make_pair(key, value));
    }
    std::vector<std::pair<std::string, std::string>> ctx_vec_;
};

static Allocator allocator;

TEST(FeatureModelTest, LowModel) {
    std::string object_key = "test_key";
    std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
        object_key.size(), model::ModelManager::GetModelId<FeatureModel>())]);
    std::unique_ptr<DummyOplogger2> op_logger(new DummyOplogger2());

    partition::Object obj(0, buf.get());
    obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<FeatureModel>(), object_key);

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
        std::string key = SerializeToString<uint64_t>(ts_start + i);
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
    auto feature_model1 = obj.Model<FeatureModel>();
    std::shared_ptr<feature::QueryResponse> response_ptr(new feature::QueryResponse);
    auto filter_func = [response_ptr](const uint64_t key, const std::string& entry) mutable {
        auto pt = response_ptr->add_point_list();
        pt->set_ts(key);
        pt->set_value(std::move(entry));
    };
    st = feature_model1->OrSet().Query(0, 100000, cnt, filter_func);
    ASSERT_TRUE(st.ok());
    uint64_t i = 1;
    for (auto it = response_ptr->point_list().begin(); it != response_ptr->point_list().end();
         it++) {
        std::cout << "0 ts " << it->ts() << " value " << it->value() << std::endl;
        ASSERT_EQ(ts_start + i, it->ts());
        std::string real_val = std::to_string(ts_start + i);
        ASSERT_EQ(it->value().compare(real_val), 0);
        i++;
    }

    uint64_t apply_key = ts_start + cnt;
    feature::Point feature_point;
    feature_point.set_ts(ts_start + cnt);
    feature_point.set_value(std::to_string(ts_start + cnt));
    Property pt;
    Property new_pt;
    pt.page_id = 0;
    pt.cluster_id = 99;
    pt.deleted = false;
    pt.timestamp = 999;
    std::string newKey = SerializeToString<uint64_t>(feature_point.ts());
    std::string newVal = SerializeToString<std::string>(feature_point.value());
    st = feature_model1->Apply(&allocator, newKey, newVal, pt.cluster_id, pt.timestamp, pt.deleted);
    std::cout << "model1 apply st is " << st.ToString() << std::endl;
    ASSERT_TRUE(st.ok());

    response_ptr.reset(new feature::QueryResponse);
    st = feature_model1->OrSet().Query(0, 100000, cnt, filter_func);
    ASSERT_TRUE(st.ok());
    i = 1;
    for (auto it = response_ptr->point_list().begin(); it != response_ptr->point_list().end();
         it++) {
        std::cout << "1 ts " << it->ts() << " value " << it->value() << std::endl;
        ASSERT_EQ(ts_start + i, it->ts());
        std::string real_val = std::to_string(ts_start + i);
        ASSERT_EQ(it->value().compare(real_val), 0);
        i++;
    }

    st = feature_model1->OrSet().GetProperty(apply_key, &new_pt);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(new_pt.page_id, pt.page_id);
    ASSERT_EQ(new_pt.cluster_id, pt.cluster_id);
    ASSERT_EQ(new_pt.timestamp, pt.timestamp);
    ASSERT_EQ(new_pt.deleted, pt.deleted);

    std::vector<partition::LogItem> logs;
    auto my_pages = feature_model1->DumpNewPages({}, logs);
    ASSERT_EQ(my_pages.size(), 1);
    page_info.data = my_pages[0].second;
    std::unique_ptr<uint8_t[]> buf2(new uint8_t[partition::Object::ComputeRawObjectSize(
        object_key.size(), model::ModelManager::GetModelId<FeatureModel>())]);
    partition::Object obj2(0, buf2.get());
    obj2.ConstructWithValues(buf2.get(), model::ModelManager::GetModelId<FeatureModel>(),
                             object_key);
    st = model::ModelManager::Init(obj2.ModelId(), obj2.RawModelBuf(), &allocator, {page_info});
    std::cout << "model2 init st is " << st.ToString() << std::endl;
    auto feature_model2 = obj2.Model<FeatureModel>();
    ASSERT_TRUE(st.ok());
    response_ptr.reset(new feature::QueryResponse);
    st = feature_model2->OrSet().Query(0, 100000, cnt, filter_func);
    i = 1;
    for (auto it = response_ptr->point_list().begin(); it != response_ptr->point_list().end();
         it++) {
        std::cout << "2 ts " << it->ts() << " value " << it->value() << std::endl;
        ASSERT_EQ(ts_start + i, it->ts());
        std::string real_val = std::to_string(ts_start + i);
        ASSERT_EQ(it->value().compare(real_val), 0);
        i++;
    }
}

}  // namespace test
}  // namespace model
}  // namespace bcache2
