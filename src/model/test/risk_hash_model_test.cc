// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#define __RISK_HASH_FOR_UNIT_TEST__
#include "model/risk_hash_model.h"

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

struct DummyOplog {
    std::string key;
    std::string value;
    bool isDeleted;
    std::string toString(bool withValue = true) {
        return key + " " + (withValue ? value + " " : "") + (isDeleted ? "true" : "false");
    }
};
class DummyOplogger5 : public partition::OpLogger {
 public:
    DummyOplogger5() : OpLogger(nullptr, nullptr, nullptr, &metrics_manager) {}
    void WriteKvLog(partition::CmdContext* ctx, uint64_t slot_id, uint64_t object_id,
                    const absl::string_view& object_key, uint16_t model_id, std::string key,
                    std::string value, model::Property property) override {
        std::string trueKey = "";
        google::protobuf::io::ArrayInputStream input(key.data(), key.size());
        google::protobuf::io::CodedInputStream stream(&input);
        ReadData(&stream, &trueKey);
        std::string trueValue = "";
        google::protobuf::io::ArrayInputStream inputv(value.data(), value.size());
        google::protobuf::io::CodedInputStream streamv(&inputv);
        ctx_vec_.emplace_back(DummyOplog{trueKey, trueValue, property.deleted});
    }
    std::string toString() {
        std::stringstream out;
        PrintAll(out);
        return out.str();
    }
    void PrintAll(std::ostream& os = std::cout, bool withValue = true) {
        os << "===== start print all op log =====" << std::endl;
        for (size_t i = 0; i < ctx_vec_.size(); ++i) {
            os << ctx_vec_[i].toString(withValue) << std::endl;
        }
        os << "=====  end  print all op log =====" << std::endl;
    }
    void Print(std::vector<int> pos) {
        std::cout << "===== start print op log =====" << std::endl;
        for (int i = 0; i < pos.size(); ++i) {
            std::cout << ctx_vec_[pos[i]].toString() << std::endl;
        }
        std::cout << "=====  end  print op log =====" << std::endl;
    }
    std::vector<DummyOplog> ctx_vec_;
};

static Allocator allocator;

TEST(RiskHashModelTest, RiskHashUUIDCheck) {
    // Pass WriteKvLog in PersistentMap::Put
    std::string object_key = "test_key";
    std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
        object_key.size(), model::ModelManager::GetModelId<RiskHashModel>())]);
    std::unique_ptr<DummyOplogger5> op_logger(new DummyOplogger5());
    partition::Object obj(0, buf.get());
    obj.ConstructWithValues(
        buf.get(), model::ModelManager::GetModelId<RiskHashModel>(), object_key);
    partition::CmdContext ctx;
    ctx.object = obj;
    ctx.op_logger = op_logger.get();

    auto btree = BtreeMap<std::string, std::pair<Property, std::string>>(
        std::less<std::string>(), Allocator::StlWrapper<std::pair<const std::string, std::string>>(
                                      Allocator::DefaultAllocator()));
    uint64_t max_timestamp = 0;
    auto map = PersistentMap<std::string, std::string>(&btree, &max_timestamp);
    auto orset = RiskHashOrSet(&map);
    risk::RiskTimerLogger timer("", "", 0, 1000000000000);

    std::string field = "field";
    std::string uuid = "repeat_uuid";
    // 初次写入
    auto st = orset.BatchUpsert(&ctx, &timer, {field}, {1}, 300,
        [](int64_t cur_val, int64_t pre_val) -> int64_t {
            return cur_val;
        }, uuid);
    ASSERT_TRUE(st.ok());
    bool found = false;
    for (auto p : op_logger.get()->ctx_vec_) {
        if (p.key == Uuid_key_prefix + uuid) {
            found = true;
            break;
        }
    }
    ASSERT_TRUE(found);
    absl::flat_hash_map<std::__cxx11::string, std::__cxx11::string> resMap;
    orset.Query({field}, &resMap);
    ASSERT_TRUE(resMap.contains(field));
    ASSERT_EQ("1", resMap[field]);
    // 写入不同 key
    st = orset.BatchUpsert(&ctx, &timer, {field}, {2}, 300,
        [](int64_t cur_val, int64_t pre_val) -> int64_t {
            return cur_val;
        }, "diff_uuid");
    ASSERT_TRUE(st.ok());
    resMap.clear();
    orset.Query({field}, &resMap);
    ASSERT_TRUE(resMap.contains(field));
    ASSERT_EQ("2", resMap[field]);
    // 写入相同的 key
    st = orset.BatchUpsert(&ctx, &timer, {field}, {3}, 300,
        [](int64_t cur_val, int64_t pre_val) -> int64_t {
            return cur_val;
        }, uuid);

    ASSERT_TRUE(st.IsRiskAlreadyHandled());
    resMap.clear();
    orset.Query({field}, &resMap);
    ASSERT_TRUE(resMap.contains(field));
    ASSERT_EQ("2", resMap[field]);

    // 等待过期
    sleep(kUUIDExpiredTime);
    op_logger.get()->ctx_vec_.clear();
    // 写入相同的 key, 预期覆盖写入当前 key
    st = orset.BatchUpsert(&ctx, &timer, {field}, {3}, 300,
        [](int64_t cur_val, int64_t pre_val) -> int64_t {
            return cur_val;
        }, uuid);

    ASSERT_TRUE(st.ok());
    resMap.clear();
    orset.Query({field}, &resMap);
    ASSERT_TRUE(resMap.contains(field));
    ASSERT_EQ("3", resMap[field]);
    found = false;
    for (auto p : op_logger.get()->ctx_vec_) {
        if (p.key == Uuid_key_prefix + uuid) {
            found = true;
            break;
        }
    }
    ASSERT_TRUE(found);
    // 模拟 300s 之后触发 gc 淘汰其余的过期 uuid
    op_logger.get()->ctx_vec_.clear();
    orset.DoMinGC(&ctx, {field}, std::time(0) + 300);
    found = false;
    for (auto p : op_logger.get()->ctx_vec_) {
        if (p.key == Uuid_key_prefix + std::string("diff_uuid")) {
            ASSERT_TRUE(p.isDeleted);
            found = true;
            break;
        }
    }
    ASSERT_TRUE(found);
    // gc 后可以写入之前过期的 key
    st = orset.BatchUpsert(&ctx, &timer, {field}, {4}, 300,
        [](int64_t cur_val, int64_t pre_val) -> int64_t {
            return cur_val;
        }, "diff_uuid");
    ASSERT_TRUE(st.ok());
    resMap.clear();
    orset.Query({field}, &resMap);
    ASSERT_TRUE(resMap.contains(field));
    ASSERT_EQ("4", resMap[field]);
}

}  // namespace test
}  // namespace model
}  // namespace bcache2
