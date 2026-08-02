// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include <gtest/gtest.h>
#include <gtest/gtest_prod.h>

#include <iostream>
#include <map>

#define __CPC_FOR_UNIT_TEST__

#include "absl/container/btree_map.h"
#include "model/risk_cpc_model.h"
#include "model/risk_hash_model.h"
#include "partition/cmd_context.h"
#include "partition/storage/object.h"
#include "partition/storage/op_logger.h"

namespace bcache2 {
namespace model {

static MetricsManager metrics_manager{{}, ""};
static Controller ctrl(0);
// 错误率理论上应该在 0.78 / sqrt(2^12) ≈ 0.012 , 但是数据量小的时候会超过这个值
// HIP 估计的均方差在 0.5887 / sqrt(2 ^ k) (sqrt(ln2/2) / sqrt(2^k))
// merge 之后使用的是 ICON 估计, 均方差系数等于 ln2 = 0.6931
static const double kRelativeErrorForLgK12 = 0.015;

class SubTestLog {
 public:
    explicit SubTestLog(std::string name) : name_(name) {
        std::cout << "===== enter " << name_ << " =====" << std::endl;
    }
    ~SubTestLog() { std::cout << "===== finish " << name_ << " =====" << std::endl; }
    std::string name_;
};

struct DummyOplog {
    std::string key;
    std::string value;
    bool isDeleted;
    std::string toString(bool withValue = true) {
        return key + " " + (withValue ? value + " " : "") + (isDeleted ? "true" : "false");
    }
};
class DummyOplogger4 : public partition::OpLogger {
 public:
    DummyOplogger4() : OpLogger(nullptr, nullptr, nullptr, &metrics_manager) {}
    void WriteKvLog(partition::CmdContext* ctx, uint64_t slot_id, uint64_t object_id,
                    const absl::string_view& object_key, uint16_t model_id, std::string key,
                    std::string value, model::Property property) override {
        ctx_vec_.emplace_back(DummyOplog{key, value, property.deleted});
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

class DummyOploggerNoOp : public partition::OpLogger {
 public:
    DummyOploggerNoOp() : OpLogger(nullptr, nullptr, nullptr, &metrics_manager) {}
    void WriteKvLog(partition::CmdContext* ctx, uint64_t slot_id, uint64_t object_id,
                    const absl::string_view& object_key, uint16_t model_id, std::string key,
                    std::string value, model::Property property) override {}
};

datasketches::cpc_sketch deserializeCPC(const std::string& value_data) {
    datasketches::cpc_sketch deserializer;
    google::protobuf::io::ArrayInputStream input(value_data.data(), value_data.size());
    google::protobuf::io::CodedInputStream stream(&input);
    return deserializer.deserialize(stream);
}

static Allocator allocator;

// 验证底层数据正确, 可以通过友元访问私有方法和成员
TEST(RISKCPCModelTest, LowModel) {
    // === init ===
    risk::RiskTimerLogger timer("cpc_unit_test", "test", 0, 10 * 1000 * 1000 * 1000);
    std::string object_key = "test_key";
    std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
        object_key.size(), model::ModelManager::GetModelId<CPCModel>())]);
    std::unique_ptr<DummyOplogger4> op_logger(new DummyOplogger4());

    partition::Object obj(0, buf.get());
    obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<CPCModel>(), object_key);
    CPCModel gModel;
    partition::CmdContext ctx;
    ctx.object = obj;
    ctx.op_logger = op_logger.get();
    ctx.ctrl = &ctrl;
    // === 反序列化 ===
    {
        SubTestLog("deserialize");
        std::vector<bcache2::partition::PageInfo> pages;
        std::string page;
        uint64_t cnt = 3;
        uint32_t cluster_id_start = 1000;
        uint64_t ts_start = 10000;
        // 反序列化正常
        {
            SubTestLog("deserialize true");
            google::protobuf::io::StringOutputStream output(&page);
            google::protobuf::io::CodedOutputStream stream(&output);
            WriteKvItemToStream(&stream, kCPCModelTtlKey, "123456", cluster_id_start, 0, ts_start);
            WriteKvItemToStream(&stream, kCPCModelTypeKey, std::to_string(kCPCModelTypeLDC),
                                cluster_id_start, 0, ts_start);
            WriteKvItemToStream(&stream, kCPCModelDontUpgradeCPC, "0", cluster_id_start, 0,
                                ts_start);
            std::map<std::string, double> estimateMap;
            for (uint64_t i = 1; i <= cnt; ++i) {
                auto cur_val = std::to_string(ts_start + i);
                std::string key = cur_val;
                datasketches::cpc_sketch cccc(kCPCModelLgK);
                cccc.update(cur_val);
                for (int j = 0; j < 100; ++j) {
                    cccc.update(j);
                }
                estimateMap[key] = cccc.get_estimate();
                std::string value;
                google::protobuf::io::StringOutputStream item_output(&value);
                google::protobuf::io::CodedOutputStream item_stream(&item_output);
                cccc.serialize(item_stream);
                WriteKvItemToStream(&stream, key, value, cluster_id_start + i, 0, ts_start);
            }
            WriteKvItemToStream(&stream, kCPCModelDumpPartEnd, "", cluster_id_start, 0, ts_start);
            std::string ts1 = "101234567890", ts2 = "101234567891";
            absl::flat_hash_set<std::string> dump_set;
            for (uint64_t i = 1; i <= cnt; ++i) {
                dump_set.emplace(std::to_string(i));
            }
            WriteKvItemToStream(&stream, ts1, gModel.serializeHashSet(dump_set), 0, false,
                                ts_start);
            WriteKvItemToStream(&stream, ts2, gModel.serializeHashSet(dump_set), 0, false,
                                ts_start);
            WriteKvItemToStream(&stream, kCPCModelDumpPartEnd, "", cluster_id_start, 0, ts_start);
            for (uint64_t i = 1; i <= cnt; ++i) {
                WriteKvItemToStream(&stream, std::string("uuid") + std::to_string(i),
                                    std::to_string(ts_start + i), 0, false, ts_start + i);
            }
            stream.Trim();
            bcache2::partition::PageInfo page_info;
            page_info.header.set_version(0);
            page_info.data = page;
            pages.emplace_back(page_info);
            Status st = model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator,
                                                  std::move(pages));
            auto cpc_model = obj.Model<CPCModel>();
            ASSERT_TRUE(st.ok());
            ASSERT_EQ(cpc_model->ttl_, 123456);
            ASSERT_EQ(cpc_model->max_timestamp_, ts_start);
            ASSERT_EQ(cpc_model->data_.size(), cnt + 2);
            ASSERT_TRUE(cpc_model->data_.contains(ts1));
            ASSERT_TRUE(cpc_model->data_.contains(ts2));
            ASSERT_EQ(cpc_model->dc_type_, kCPCModelTypeLDC);
            ASSERT_EQ(cpc_model->dont_upgrade_cpc_, false);
            for (uint64_t i = 1; i <= cnt; ++i) {
                auto key = std::to_string(ts_start + i);
                ASSERT_NE(cpc_model->data_.find(key), cpc_model->data_.end());
                ASSERT_NEAR(cpc_model->data_.find(key)->second.second.get_estimate(), 100,
                            100 * kRelativeErrorForLgK12);
            }
            ASSERT_EQ(cpc_model->dc_data_.size(), 2);
            ASSERT_TRUE(cpc_model->dc_data_.contains(ts1));
            ASSERT_EQ(cpc_model->dc_list_total_count_, cnt * 2);
            ASSERT_EQ(cpc_model->dc_data_[ts1].second.size(), cnt);
            for (uint64_t i = 1; i <= cnt; ++i) {
                ASSERT_TRUE(cpc_model->dc_data_[ts1].second.contains(std::to_string(i)));
            }
            ASSERT_NEAR(cpc_model->data_.find(ts1)->second.second.get_estimate(), cnt,
                        cnt * kRelativeErrorForLgK12);
            ASSERT_TRUE(cpc_model->dc_data_.contains(ts2));
            ASSERT_EQ(cpc_model->dc_data_[ts2].second.size(), cnt);
            for (uint64_t i = 1; i <= cnt; ++i) {
                ASSERT_TRUE(cpc_model->dc_data_[ts2].second.contains(std::to_string(i)));
            }
            ASSERT_NEAR(cpc_model->data_.find(ts2)->second.second.get_estimate(), cnt,
                        cnt * kRelativeErrorForLgK12);
            for (uint64_t i = 1; i <= cnt; ++i) {
                ASSERT_TRUE(cpc_model->data_uuid_.contains("uuid" + std::to_string(i)));
                ASSERT_EQ(cpc_model->data_uuid_["uuid" + std::to_string(i)].second, ts_start + i);
            }
        }
        // 反序列化 dc type
        {
            SubTestLog("deserialize dc type");
            page = "";
            google::protobuf::io::StringOutputStream output(&page);
            google::protobuf::io::CodedOutputStream stream(&output);
            WriteKvItemToStream(&stream, kCPCModelTtlKey, "0", 0, 0, 0);
            WriteKvItemToStream(&stream, kCPCModelTypeKey, std::to_string(kCPCModelTypeDC), 0, 0,
                                0);
            WriteKvItemToStream(&stream, kCPCModelDontUpgradeCPC, "1", 0, 0, 0);
            WriteKvItemToStream(&stream, kCPCModelDumpPartEnd, "", cluster_id_start, 0, ts_start);
            std::string ts1 = "101234567890", ts2 = "101234567891";
            absl::flat_hash_set<std::string> dump_set;
            for (uint64_t i = 1; i <= cnt; ++i) {
                dump_set.emplace(std::to_string(i));
            }
            WriteKvItemToStream(&stream, ts1, gModel.serializeHashSet(dump_set), 0, false,
                                ts_start);
            WriteKvItemToStream(&stream, ts2, gModel.serializeHashSet(dump_set), 0, false,
                                ts_start);
            stream.Trim();
            bcache2::partition::PageInfo page_info;
            page_info.header.set_version(1);
            page_info.data = page;
            pages.emplace_back(page_info);
            Status st = model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator,
                                                  std::move(pages));
            auto cpc_model = obj.Model<CPCModel>();
            ASSERT_TRUE(st.ok());
            ASSERT_EQ(cpc_model->data_.size(), 0);
            ASSERT_EQ(cpc_model->dc_type_, kCPCModelTypeDC);
            ASSERT_TRUE(cpc_model->dont_upgrade_cpc_);
            ASSERT_EQ(cpc_model->dc_data_.size(), 2);
            ASSERT_TRUE(cpc_model->dc_data_.contains(ts1));
            ASSERT_EQ(cpc_model->dc_data_[ts1].second.size(), cnt);
            ASSERT_EQ(cpc_model->dc_list_total_count_, cnt * 2);
            for (uint64_t i = 1; i <= cnt; ++i) {
                ASSERT_TRUE(cpc_model->dc_data_[ts1].second.contains(std::to_string(i)));
            }
            ASSERT_TRUE(cpc_model->dc_data_.contains(ts2));
            ASSERT_EQ(cpc_model->dc_data_[ts2].second.size(), cnt);
            for (uint64_t i = 1; i <= cnt; ++i) {
                ASSERT_TRUE(cpc_model->dc_data_[ts2].second.contains(std::to_string(i)));
            }
        }
        // 反序列化异常
        {
            SubTestLog("deserialize error");
            page = "";
            google::protobuf::io::StringOutputStream output(&page);
            google::protobuf::io::CodedOutputStream stream(&output);
            // 空数据
            stream.Trim();
            bcache2::partition::PageInfo page_info;
            page_info.header.set_version(2);
            page_info.data = page;
            pages.emplace_back(page_info);
            Status st = model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator,
                                                  std::move(pages));
            ASSERT_FALSE(st.ok());
            // 只有 ttl
            WriteKvItemToStream(&stream, kCPCModelTtlKey, "0", 0, 0, 0);
            stream.Trim();
            page_info.header.set_version(3);
            page_info.data = page;
            pages.emplace_back(page_info);
            st = model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator,
                                           std::move(pages));
            ASSERT_FALSE(st.ok());
            // 只有 ttl + type
            WriteKvItemToStream(&stream, kCPCModelTypeKey, std::to_string(kCPCModelTypeDC), 0, 0,
                                0);
            stream.Trim();
            page_info.header.set_version(4);
            page_info.data = page;
            pages.emplace_back(page_info);
            st = model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator,
                                           std::move(pages));
            ASSERT_FALSE(st.ok());
            // 有 ttl + type + upgrade_flag, 可以正常解析
            WriteKvItemToStream(&stream, kCPCModelDontUpgradeCPC, "1", 0, 0, 0);
            stream.Trim();
            page_info.header.set_version(5);
            page_info.data = page;
            pages.emplace_back(page_info);
            st = model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator,
                                           std::move(pages));
            ASSERT_TRUE(st.ok());
            // 插入中间分隔栏, 正常
            WriteKvItemToStream(&stream, kCPCModelDumpPartEnd, "", cluster_id_start, 0, ts_start);
            stream.Trim();
            page_info.header.set_version(6);
            page_info.data = page;
            pages.emplace_back(page_info);
            st = model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator,
                                           std::move(pages));
            ASSERT_TRUE(st.ok());
            // 乱写数据, 读取失败
            stream.WriteVarint32(123);
            stream.Trim();
            page_info.header.set_version(7);
            page_info.data = page;
            pages.emplace_back(page_info);
            st = model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator,
                                           std::move(pages));
            ASSERT_FALSE(st.ok());
        }
        op_logger.get()->ctx_vec_.clear();
    }
    // === add + cpc mode ===
    {
        SubTestLog("add cpc mode");
        {
            auto cpc_model = obj.Model<CPCModel>();
            cpc_model->dc_list_total_count_ = 0;
            cpc_model->dc_data_.clear();
            cpc_model->data_.clear();
            cpc_model->dc_type_ = kCPCModelTypeLDC;
            cpc_model->dont_upgrade_cpc_ = false;
            std::string key = "test_add_cpc";
            auto lastUpdTime = cpc_model->max_timestamp_;
            auto lastTTL = cpc_model->ttl_;
            int dc_count = 0;
            int logSize = 0;
            std::vector<int> cpcLog, dc_add_log, dc_del_log;
            int testSize = kCPCModelCPCOpLogPerUpdate + kCPCModelDumpMemSize * 2 > 1000
                               ? kCPCModelCPCOpLogPerUpdate + kCPCModelDumpMemSize * 2
                               : 1000;
            for (int v = 0; v < testSize; ++v) {
                if (cpc_model->data_[key].second.mem_size() >= kCPCModelDumpMemSize) {
                    dc_count++;
                    dc_add_log.emplace_back(logSize++);
                } else {
                    cpcLog.emplace_back(logSize++);
                }
                auto st = cpc_model->Update(&ctx, &timer, {key},
                    "value" + std::to_string(v), lastTTL - 1);
                ASSERT_TRUE(st.ok()) << st.ToString();
                if (dc_count >= kCPCModelCPCOpLogPerUpdate) {
                    cpcLog.emplace_back(logSize++);
                    for (int c = 0; c < dc_count; ++c) {
                        dc_del_log.emplace_back(logSize++);
                    }
                    dc_count = 0;
                    if (v == testSize - 1) {
                        testSize++;
                    }
                }
                ASSERT_EQ(cpc_model->dc_list_total_count_, dc_count) << "now=" << v;
            }
            ASSERT_TRUE(cpc_model->data_.contains(key));
            ASSERT_NEAR(cpc_model->data_.find(key)->second.second.get_estimate(), testSize,
                        testSize * kRelativeErrorForLgK12);
            ASSERT_EQ(cpc_model->ttl_, lastTTL);
            ASSERT_EQ(op_logger.get()->ctx_vec_.size(), logSize);
            ASSERT_EQ(op_logger.get()->ctx_vec_[0].key, key);
            ASSERT_NEAR(deserializeCPC(op_logger.get()->ctx_vec_[0].value).get_estimate(), 1,
                        kRelativeErrorForLgK12);
            for (auto ind : cpcLog) {
                ASSERT_EQ(op_logger.get()->ctx_vec_[ind].key, key);
                ASSERT_FALSE(op_logger.get()->ctx_vec_[ind].isDeleted);
            }
            int subSize = kCPCModelFieldDcPrefixSize + key.length();
            for (auto ind : dc_add_log) {
                ASSERT_EQ(op_logger.get()->ctx_vec_[ind].key.substr(0, subSize),
                          kCPCModelDCKeyPrefix + key);
                ASSERT_FALSE(op_logger.get()->ctx_vec_[ind].isDeleted);
            }
            for (auto ind : dc_del_log) {
                ASSERT_EQ(op_logger.get()->ctx_vec_[ind].key.substr(0, subSize),
                          kCPCModelDCKeyPrefix + key);
                ASSERT_TRUE(op_logger.get()->ctx_vec_[ind].isDeleted);
            }
            if (dc_count != 0) {
                ASSERT_TRUE(cpc_model->dc_data_.contains(key));
                ASSERT_EQ(cpc_model->dc_data_[key].second.size(), dc_count);
            }
            op_logger.get()->ctx_vec_.clear();
        }
    }
    // === add + dc mode ===
    {
        SubTestLog("add dc mode");
        auto cpc_model = obj.Model<CPCModel>();
        cpc_model->dc_list_total_count_ = 0;
        cpc_model->dc_data_.clear();
        cpc_model->data_.clear();
        cpc_model->dc_type_ = kCPCModelTypeDC;
        cpc_model->dont_upgrade_cpc_ = false;
        std::string key = "10test_add_dc";
        auto lastUpdTime = cpc_model->max_timestamp_;
        auto lastTTL = cpc_model->ttl_;
        // 禁止升级
        for (int v = 0; v < 200; ++v) {
            auto st = cpc_model->Update(&ctx, &timer, {key}, std::to_string(v), lastTTL - 1, true);
            ASSERT_TRUE(st.ok());
            ASSERT_EQ(cpc_model->dc_list_total_count_, v + 1);
        }
        ASSERT_FALSE(cpc_model->data_.contains(key));
        ASSERT_EQ(cpc_model->ttl_, lastTTL);
        ASSERT_LT(lastUpdTime + 200, cpc_model->max_timestamp_);
        ASSERT_EQ(op_logger.get()->ctx_vec_.size(), 201);
        ASSERT_EQ(op_logger.get()->ctx_vec_[0].key, kCPCModelDontUpgradeCPC);
        ASSERT_EQ(op_logger.get()->ctx_vec_[0].value, "1");
        ASSERT_FALSE(op_logger.get()->ctx_vec_[0].isDeleted);
        for (int v = 0; v < 200; ++v) {
            ASSERT_EQ(op_logger.get()->ctx_vec_[v + 1].key,
                      kCPCModelDCKeyPrefix + key + std::to_string(v));
            ASSERT_FALSE(op_logger.get()->ctx_vec_[v].isDeleted);
        }
        ASSERT_TRUE(cpc_model->dc_data_.contains(key));
        ASSERT_EQ(cpc_model->dc_data_[key].second.size(), 200);
        for (int v = 0; v < 200; ++v) {
            ASSERT_TRUE(cpc_model->dc_data_[key].second.contains(std::to_string(v)));
        }
        for (int v = 200; v < 300; ++v) {
            auto st = cpc_model->Update(&ctx, &timer, {key}, std::to_string(v), lastTTL - 1, true);
            ASSERT_TRUE(st.ok());
        }
        // 允许升级
        op_logger.get()->ctx_vec_.clear();
        cpc_model->dc_data_["useless"] =
            std::make_pair(Property{}, absl::flat_hash_set<std::string>());
        for (int i = 0; i < kCPCModelTransforLDCSize; ++i) {
            cpc_model->dc_data_["useless"].second.emplace("useless" + std::to_string(i));
            cpc_model->dc_list_total_count_++;
        }
        cpc_model->cpc_transfor_check_cnt_ = kCPCModelTransforCheckPreWrite;
        auto st = cpc_model->Update(&ctx, &timer, {key}, "0", lastTTL - 1);
        ASSERT_TRUE(st.ok());
        ASSERT_FALSE(cpc_model->dont_upgrade_cpc_);
        ASSERT_EQ(cpc_model->dc_type_, kCPCModelTypeLDC);
        ASSERT_EQ(cpc_model->dc_data_.size(), 0);
        ASSERT_EQ(cpc_model->data_.size(), 2);
        ASSERT_TRUE(cpc_model->data_.contains(key));
        ASSERT_TRUE(cpc_model->data_.contains("useless"));
        ASSERT_NEAR(cpc_model->data_.find(key)->second.second.get_estimate(), 300,
                    300 * kRelativeErrorForLgK12);
        ASSERT_NEAR(cpc_model->data_.find("useless")->second.second.get_estimate(),
                    kCPCModelTransforLDCSize, kCPCModelTransforLDCSize * kRelativeErrorForLgK12);
        ASSERT_EQ(op_logger.get()->ctx_vec_[0].key, kCPCModelDontUpgradeCPC);
        ASSERT_EQ(op_logger.get()->ctx_vec_[0].value, "0");
        ASSERT_FALSE(op_logger.get()->ctx_vec_[0].isDeleted);
        ASSERT_EQ(op_logger.get()->ctx_vec_[1].key, kCPCModelDCKeyPrefix + key + "0");
        ASSERT_FALSE(op_logger.get()->ctx_vec_[1].isDeleted);
        ASSERT_EQ(op_logger.get()->ctx_vec_[2].key, kCPCModelTypeKey);
        ASSERT_EQ(op_logger.get()->ctx_vec_[2].value, std::to_string(kCPCModelTypeLDC));
        ASSERT_FALSE(op_logger.get()->ctx_vec_[2].isDeleted);
        op_logger.get()->ctx_vec_.clear();
    }
    // === add dc mode dont upgrade
    {
        SubTestLog("add dc mode dont upgrade");
        auto cpc_model = obj.Model<CPCModel>();
        cpc_model->dc_list_total_count_ = 0;
        cpc_model->dc_data_.clear();
        cpc_model->data_.clear();
        cpc_model->dc_type_ = kCPCModelTypeDC;
        cpc_model->dont_upgrade_cpc_ = false;
        std::string key = "10test_add_dc_over_limit";
        auto lastUpdTime = cpc_model->max_timestamp_;
        auto lastTTL = cpc_model->ttl_;
        // 写入足够 limit 的数量 (多个key)
        for (int i = 0; i < kCPCModelListSizeLimit; ++i) {
            cpc_model->Update(&ctx, &timer, {key + std::to_string(i)}, "1231", 0, true);
        }
        ASSERT_EQ(cpc_model->dc_list_total_count_, kCPCModelListSizeLimit);
        ASSERT_EQ(cpc_model->dc_data_.size(), kCPCModelListSizeLimit);
        cpc_model->Update(&ctx, &timer, {key}, "1231", 0, true);
        ASSERT_EQ(cpc_model->dc_list_total_count_, kCPCModelListSizeLimit);
        ASSERT_EQ(cpc_model->dc_data_.size(), kCPCModelListSizeLimit);
        cpc_model->dc_data_.clear();
        cpc_model->data_.clear();
        cpc_model->dc_list_total_count_ = 0;
        // 写入足够 limit 的数量 (单个key)
        for (int i = 0; i < kCPCModelListSizeLimit; ++i) {
            cpc_model->Update(&ctx, &timer, {key}, std::to_string(i), 0, true);
        }
        ASSERT_EQ(cpc_model->dc_list_total_count_, kCPCModelListSizeLimit);
        ASSERT_EQ(cpc_model->dc_data_.size(), 1);
        cpc_model->Update(&ctx, &timer, {key}, "fdsf", 0, true);
        ASSERT_EQ(cpc_model->dc_list_total_count_, kCPCModelListSizeLimit);
        ASSERT_EQ(cpc_model->dc_data_.size(), 1);
        cpc_model->dc_data_.clear();
        cpc_model->data_.clear();
        op_logger.get()->ctx_vec_.clear();
    }
    // === del ===
    {
        SubTestLog("del");
        auto cpc_model = obj.Model<CPCModel>();
        cpc_model->dc_list_total_count_ = 0;
        cpc_model->dont_upgrade_cpc_ = false;
        cpc_model->dc_type_ = kCPCModelTypeLDC;
        std::string key = "test_del";
        auto st = cpc_model->Update(&ctx, &timer, {key}, std::to_string(123), 0);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(op_logger.get()->ctx_vec_.size(), 1) << op_logger.get()->toString();
        ASSERT_EQ(op_logger.get()->ctx_vec_[0].key, key);
        ASSERT_FALSE(op_logger.get()->ctx_vec_[0].isDeleted);
        op_logger.get()->ctx_vec_.clear();
        st = cpc_model->BatchDel(&ctx, {key});
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(cpc_model->data_.find(key), cpc_model->data_.end());
        ASSERT_EQ(op_logger.get()->ctx_vec_.size(), 1) << op_logger.get()->toString();
        ASSERT_EQ(op_logger.get()->ctx_vec_[0].key, key);
        ASSERT_TRUE(op_logger.get()->ctx_vec_[0].isDeleted);
        ASSERT_EQ(cpc_model->dc_list_total_count_, 0);
        int value = 0;
        for (int i = 0; i < kCPCModelDumpMemSize; ++i) {
            ASSERT_TRUE(cpc_model->Update(&ctx, &timer, {key}, std::to_string(i), 0).ok());
            if (cpc_model->dc_list_total_count_ > 0) {
                value = i;
                break;
            }
        }
        op_logger.get()->ctx_vec_.clear();
        st = cpc_model->BatchDel(&ctx, {key});
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(cpc_model->data_.find(key), cpc_model->data_.end());
        ASSERT_EQ(op_logger.get()->ctx_vec_.size(), 2) << op_logger.get()->toString();
        ASSERT_EQ(op_logger.get()->ctx_vec_[0].key, key);
        ASSERT_TRUE(op_logger.get()->ctx_vec_[0].isDeleted);
        ASSERT_EQ(op_logger.get()->ctx_vec_[1].key,
                  kCPCModelDCKeyPrefix + key + std::to_string(value));
        ASSERT_TRUE(op_logger.get()->ctx_vec_[1].isDeleted);
        ASSERT_EQ(cpc_model->dc_list_total_count_, 0);
        op_logger.get()->ctx_vec_.clear();
    }
    // === del dc set ===
    {
        SubTestLog("del dc set");
        auto cpc_model = obj.Model<CPCModel>();
        cpc_model->dc_list_total_count_ = 0;
        cpc_model->dont_upgrade_cpc_ = false;
        cpc_model->dc_type_ = kCPCModelTypeDC;
        std::string key = "test_del_dc_set";
        ASSERT_TRUE(cpc_model->Update(&ctx, &timer, {key}, std::to_string(0), 0).ok());
        for (int i = 0; i < 50; ++i) {
            auto st = cpc_model->Update(&ctx, &timer, {key}, std::to_string(i), 0);
            ASSERT_TRUE(st.ok());
            ASSERT_TRUE(cpc_model->dc_data_[key].second.contains(std::to_string(i))) << i;
        }
        ASSERT_EQ(cpc_model->dc_data_[key].second.size(), 50);
        ASSERT_EQ(cpc_model->dc_list_total_count_, 50);
        op_logger.get()->ctx_vec_.clear();
        cpc_model->delDcSet(&ctx, key, {"0", "1", "2", "3", "4", "abc"});
        ASSERT_EQ(cpc_model->dc_list_total_count_, 45);
        ASSERT_EQ(op_logger.get()->ctx_vec_.size(), 6) << op_logger.get()->toString();
        for (int i = 0; i < 5; ++i) {
            ASSERT_EQ(op_logger.get()->ctx_vec_[i].key,
                      kCPCModelDCKeyPrefix + key + std::to_string(i));
            ASSERT_TRUE(op_logger.get()->ctx_vec_[i].isDeleted);
        }
        ASSERT_EQ(op_logger.get()->ctx_vec_[5].key, kCPCModelDCKeyPrefix + key + "abc");
        ASSERT_TRUE(op_logger.get()->ctx_vec_[5].isDeleted);
        ASSERT_EQ(cpc_model->dc_data_[key].second.size(), 45);
        for (int i = 5; i < 50; ++i) {
            ASSERT_TRUE(cpc_model->dc_data_[key].second.contains(std::to_string(i))) << i;
        }
    }
    // === del full dc set ===
    {
        SubTestLog("del full dc set");
        auto cpc_model = obj.Model<CPCModel>();
        cpc_model->dont_upgrade_cpc_ = false;
        cpc_model->dc_list_total_count_ = 0;
        cpc_model->dc_type_ = kCPCModelTypeDC;
        std::string key = "test_del_full_dc_set";
        ASSERT_TRUE(cpc_model->Update(&ctx, &timer, {key}, std::to_string(0), 0).ok());
        for (int i = 0; i < 50; ++i) {
            auto st = cpc_model->Update(&ctx, &timer, {key}, std::to_string(i), 0);
            ASSERT_TRUE(st.ok());
            ASSERT_TRUE(cpc_model->dc_data_[key].second.contains(std::to_string(i))) << i;
        }
        ASSERT_EQ(cpc_model->dc_list_total_count_, 50);
        ASSERT_EQ(cpc_model->dc_data_[key].second.size(), 50);
        op_logger.get()->ctx_vec_.clear();
        cpc_model->delFullDcSet(&ctx, key);
        ASSERT_EQ(op_logger.get()->ctx_vec_.size(), 50) << op_logger.get()->toString();
        std::sort(
            op_logger.get()->ctx_vec_.begin(), op_logger.get()->ctx_vec_.end(),
            [&key](const DummyOplog& lhs, const DummyOplog& rhs) -> bool {
                return std::strtol(lhs.key.substr(kCPCModelFieldDcPrefixSize + key.size()).c_str(),
                                   nullptr, 10) <
                       std::strtol(rhs.key.substr(kCPCModelFieldDcPrefixSize + key.size()).c_str(),
                                   nullptr, 10);
            });
        for (int i = 0; i < 50; ++i) {
            ASSERT_EQ(op_logger.get()->ctx_vec_[i].key,
                      kCPCModelDCKeyPrefix + key + std::to_string(i));
            ASSERT_TRUE(op_logger.get()->ctx_vec_[i].isDeleted);
        }
        ASSERT_EQ(cpc_model->dc_list_total_count_, 0);
        ASSERT_FALSE(cpc_model->dc_data_.contains(key));
        op_logger.get()->ctx_vec_.clear();
    }
    // === apply ===
    {
        SubTestLog("apply");
        auto cpc_model = obj.Model<CPCModel>();
        std::string key = "101234567890";
        auto lastUpdTime = cpc_model->max_timestamp_;
        cpc_model->cpc_transfor_check_cnt_ = 0;
        cpc_model->dc_type_ = kCPCModelTypeDC;
        cpc_model->data_.clear();
        cpc_model->dc_data_.clear();
        cpc_model->dont_upgrade_cpc_ = false;
        // add cpc
        datasketches::cpc_sketch cpc_data(kCPCModelLgK);
        cpc_data.update(99999999999);
        std::string item_data = "";
        google::protobuf::io::StringOutputStream item_output(&item_data);
        google::protobuf::io::CodedOutputStream item_stream(&item_output);
        cpc_data.serialize(item_stream);
        auto st = cpc_model->Apply(nullptr, key, item_data, 0, 0, false);
        ASSERT_TRUE(st.ok());
        ASSERT_NE(cpc_model->data_.find(key), cpc_model->data_.end());
        ASSERT_NEAR(cpc_model->data_.find(key)->second.second.get_estimate(), 1,
                    kRelativeErrorForLgK12);
        // add dc key error
        st = cpc_model->Apply(nullptr, kCPCModelDCKeyPrefix + std::string("0"), "", 0, 0, false);
        ASSERT_FALSE(st.ok());
        // add dc
        for (int i = 0; i < 200; ++i) {
            st = cpc_model->Apply(nullptr, kCPCModelDCKeyPrefix + key + std::to_string(i), "", 0, 0,
                                  false);
            ASSERT_TRUE(st.ok());
            ASSERT_TRUE(cpc_model->dc_data_.contains(key));
            ASSERT_EQ(cpc_model->dc_data_[key].second.size(), i + 1);
            ASSERT_TRUE(cpc_model->dc_data_[key].second.contains(std::to_string(i)));
        }
        ASSERT_EQ(cpc_model->dc_list_total_count_, 200);
        // ttl
        auto lastTTL = cpc_model->ttl_;
        st = cpc_model->Apply(nullptr, kCPCModelTtlKey, std::to_string(lastTTL + 1), 0, 0, false);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(lastTTL + 1, cpc_model->ttl_);
        op_logger.get()->ctx_vec_.clear();
        // need_upgrade
        st = cpc_model->Apply(nullptr, kCPCModelDontUpgradeCPC, "1", 0, 0, false);
        ASSERT_TRUE(st.ok());
        ASSERT_TRUE(cpc_model->dont_upgrade_cpc_);

        ASSERT_EQ(op_logger.get()->ctx_vec_.size(), 0);
        // dc_type
        st = cpc_model->Apply(nullptr, kCPCModelTypeKey, std::to_string(kCPCModelTypeLDC), 0, 0,
                              false);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(cpc_model->dc_type_, kCPCModelTypeLDC);
        // dc_type error
        st = cpc_model->Apply(nullptr, kCPCModelTypeKey, std::to_string(kCPCModelTypeDC), 0, 0,
                              false);
        ASSERT_FALSE(st.ok());
        ASSERT_EQ(cpc_model->dc_type_, kCPCModelTypeLDC);
        // need_upgrade error (cpc 模式无法修改成禁止升级模式)
        st = cpc_model->Apply(nullptr, kCPCModelDontUpgradeCPC, "1", 0, 0, false);
        ASSERT_FALSE(st.ok());
        // add dc with cpc type
        for (int i = 0; i < 200; ++i) {
            st = cpc_model->Apply(nullptr, kCPCModelDCKeyPrefix + key + std::to_string(i), "", 0, 0,
                                  false);
            ASSERT_TRUE(st.ok());
            ASSERT_NE(cpc_model->data_.find(key), cpc_model->data_.end());
            ASSERT_NEAR(cpc_model->data_.find(key)->second.second.get_estimate(), i + 2,
                        (i + 2) * kRelativeErrorForLgK12)
                << "now=" << i;
        }
        ASSERT_NEAR(cpc_model->data_.find(key)->second.second.get_estimate(), 201,
                    201 * kRelativeErrorForLgK12);
        // delete dc
        st = cpc_model->Apply(nullptr, kCPCModelDCKeyPrefix + key + "0", "", 0, 0, true);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(cpc_model->dc_data_[key].second.size(), 199);
        ASSERT_EQ(cpc_model->dc_list_total_count_, 199);
        ASSERT_FALSE(cpc_model->dc_data_[key].second.contains("0"));
        ASSERT_NEAR(cpc_model->data_.find(key)->second.second.get_estimate(), 200,
                    200 * kRelativeErrorForLgK12);
        // delete cpc
        st = cpc_model->Apply(nullptr, key, "", 0, 0, true);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(cpc_model->data_.find(key), cpc_model->data_.end());
        ASSERT_EQ(op_logger.get()->ctx_vec_.size(), 0);
        // add uuid
        std::string uuid = "uuid1";
        st = cpc_model->Apply(nullptr, kCPCModelUUIDKeyPrefix + uuid, "12345", 0, 0, false);
        ASSERT_TRUE(st.ok());
        ASSERT_TRUE(cpc_model->data_uuid_.contains(uuid));
        ASSERT_EQ(cpc_model->data_uuid_[uuid].second, 12345);
        ASSERT_EQ(op_logger.get()->ctx_vec_.size(), 0);
    }
    // === change to cpc ===
    {
        SubTestLog("change to cpc");
        auto cpc_model = obj.Model<CPCModel>();
        std::string key = "change_to_cpc";
        // 初始化
        cpc_model->dc_data_.clear();
        cpc_model->data_.clear();
        cpc_model->cpc_transfor_check_cnt_ = 0;
        cpc_model->dont_upgrade_cpc_ = false;
        cpc_model->dc_list_total_count_ = 0;
        cpc_model->dc_list_total_mem_size_ = 0;
        cpc_model->dc_type_ = kCPCModelTypeDC;
        // 不断写入, 直到写入到阈值之前, 每次写入都触发 check
        for (int i = 1; i < kCPCModelTransforLDCSize; ++i) {
            cpc_model->cpc_transfor_check_cnt_ = kCPCModelTransforCheckPreWrite + 1;
            ASSERT_TRUE(cpc_model->Update(&ctx, &timer, {key}, std::to_string(i), 0).ok());
            ASSERT_EQ(cpc_model->cpc_transfor_check_cnt_, 0);
            ASSERT_EQ(cpc_model->data_.size(), 0);
            ASSERT_EQ(cpc_model->dc_data_.size(), 1);
            ASSERT_EQ(cpc_model->dc_type_, kCPCModelTypeDC);
            ASSERT_EQ(cpc_model->dc_list_total_count_, i);
        }
        // 写入超过阈值
        cpc_model->cpc_transfor_check_cnt_ = kCPCModelTransforCheckPreWrite + 1;
        ASSERT_TRUE(cpc_model->Update(&ctx, &timer, {key}, "0", 0).ok());
        ASSERT_EQ(cpc_model->cpc_transfor_check_cnt_, 0);
        ASSERT_EQ(cpc_model->data_.size(), 1);
        ASSERT_TRUE(cpc_model->data_.contains(key));
        ASSERT_EQ(cpc_model->dc_data_.size(), 0);
        ASSERT_EQ(cpc_model->dc_type_, kCPCModelTypeLDC);
        ASSERT_EQ(cpc_model->dc_list_total_count_, 0);
        ASSERT_NEAR(cpc_model->data_[key].second.get_estimate(), kCPCModelTransforLDCSize,
                    kCPCModelTransforLDCSize * kRelativeErrorForLgK12);
        op_logger.get()->ctx_vec_.clear();
    }
    // === gc ===
    {
        SubTestLog("gc");
        // 测试 doExpire
        {
            SubTestLog("doExpire");
            // 清除部分
            auto cpc_model = obj.Model<CPCModel>();
            cpc_model->dc_list_total_count_ = 0;
            std::string key = "test_expire";
            cpc_model->data_.clear();
            cpc_model->dc_data_.clear();
            cpc_model->dont_upgrade_cpc_ = false;
            cpc_model->dc_type_ = kCPCModelTypeLDC;
            std::vector<std::string> keys;
            for (int i = 1000; i < 2000; ++i) {
                auto trueKey = key + std::to_string(i);
                keys.emplace_back(trueKey);
            }
            auto st = cpc_model->Update(&ctx, &timer, keys, "123", 0);
            ASSERT_TRUE(st.ok());
            ASSERT_EQ(cpc_model->dc_list_total_count_, 0);
            cpc_model->dc_data_.clear();
            op_logger.get()->ctx_vec_.clear();
            absl::flat_hash_set<std::string> set;
            cpc_model->dc_data_[key] = std::make_pair(Property{}, std::move(set));
            for (int i = 1000; i < 2000; ++i) {
                cpc_model->dc_data_[key].second.emplace(std::to_string(i));
            }
            cpc_model->dc_list_total_count_ = 1000;
            st = cpc_model->doExpire(&ctx, "test_expire", "test_expire99999999", 10);
            ASSERT_TRUE(st.ok());
            ASSERT_EQ(op_logger.get()->ctx_vec_.size(), 20);
            ASSERT_EQ(cpc_model->data_.size(), 990);
            ASSERT_EQ(cpc_model->dc_list_total_count_, 990);
            for (int i = 0; i < 10; ++i) {
                ASSERT_EQ(op_logger.get()->ctx_vec_[i].key, key + std::to_string(i + 1000));
                ASSERT_EQ(op_logger.get()->ctx_vec_[i].value, "");
                ASSERT_TRUE(op_logger.get()->ctx_vec_[i].isDeleted);
            }
            ASSERT_EQ(cpc_model->dc_data_[key].second.size(), 990);
            for (int i = 10; i < 20; ++i) {
                ASSERT_EQ(
                    op_logger.get()->ctx_vec_[i].key.substr(0, (kCPCModelDCKeyPrefix + key).size()),
                    kCPCModelDCKeyPrefix + key);
                auto value =
                    op_logger.get()->ctx_vec_[i].key.substr((kCPCModelDCKeyPrefix + key).size());
                ASSERT_FALSE(cpc_model->dc_data_[key].second.contains(value));
                ASSERT_TRUE(op_logger.get()->ctx_vec_[i].isDeleted);
            }

            // 清除全部
            st = cpc_model->doExpire(&ctx, "test_expire", "test_expire99999999", 0);
            ASSERT_TRUE(st.ok());
            ASSERT_EQ(op_logger.get()->ctx_vec_.size(), 2000);
            ASSERT_EQ(cpc_model->data_.size(), 0);
            for (int i = 20; i < 1010; ++i) {
                ASSERT_EQ(op_logger.get()->ctx_vec_[i].key, key + std::to_string(i + 990));
                ASSERT_EQ(op_logger.get()->ctx_vec_[i].value, "");
                ASSERT_TRUE(op_logger.get()->ctx_vec_[i].isDeleted);
            }
            ASSERT_EQ(cpc_model->dc_data_.size(), 0);
            ASSERT_EQ(cpc_model->dc_list_total_count_, 0);
            for (int i = 1010; i < 2000; ++i) {
                ASSERT_EQ(
                    op_logger.get()->ctx_vec_[i].key.substr(0, (kCPCModelDCKeyPrefix + key).size()),
                    kCPCModelDCKeyPrefix + key);
                ASSERT_TRUE(op_logger.get()->ctx_vec_[i].isDeleted);
            }
            op_logger.get()->ctx_vec_.clear();
        }
        // 测试 uuid 的 gc
        {
            SubTestLog("uuidGC");
            auto cpc_model = obj.Model<CPCModel>();
            std::string key = "test_uuid_gc";
            cpc_model->data_.clear();
            cpc_model->dc_data_.clear();
            cpc_model->dont_upgrade_cpc_ = true;
            cpc_model->dc_type_ = kCPCModelTypeDC;
            // 1000 个要过期的 uuid
            for (int i = 1000; i < 2000; ++i) {
                auto st = cpc_model->Update(&ctx, &timer, {key}, "123",
                    0, true, "uuid" + std::to_string(i));
                ASSERT_TRUE(st.ok());
                ASSERT_EQ(cpc_model->data_uuid_.size(), i - 1000 + 1);
                ASSERT_EQ(op_logger.get()->ctx_vec_.size(), (i - 1000 + 1) * 2);
                ASSERT_EQ(op_logger.get()->ctx_vec_[(i - 1000) * 2].key,
                    kCPCModelUUIDKeyPrefix + std::string("uuid") + std::to_string(i));
                ASSERT_EQ(op_logger.get()->ctx_vec_[(i - 1000) * 2 + 1].key,
                    kCPCModelDCKeyPrefix + key + "123");
            }
            // 插入一个不过期的 uuid
            cpc_model->data_uuid_["not_expired_uuid"] =
                std::make_pair(Property{}, time(0) + kCPCModelUUIDExpiredTime + 100);
            op_logger.get()->ctx_vec_.clear();
            cpc_model->uuidGc(&ctx, time(0) + kCPCModelUUIDExpiredTime + 1);
            ASSERT_EQ(op_logger.get()->ctx_vec_.size(), 1000);
            ASSERT_EQ(cpc_model->data_uuid_.size(), 1);
            for (int i = 0; i < 1000; ++i) {
                ASSERT_EQ(op_logger.get()->ctx_vec_[i].key,
                    kCPCModelUUIDKeyPrefix + std::string("uuid") + std::to_string(i + 1000));
                ASSERT_TRUE(op_logger.get()->ctx_vec_[i].isDeleted);
            }
            op_logger.get()->ctx_vec_.clear();
        }
        // 写入正式 key, 测试 doGC
        {
            SubTestLog("doGC");
            auto cpc_model = obj.Model<CPCModel>();
            cpc_model->ttl_ = 0;
            cpc_model->dc_type_ = kCPCModelTypeLDC;
            cpc_model->dont_upgrade_cpc_ = false;
            cpc_model->dc_list_total_count_ = 0;
            time_t expired = 1669776460;
            time_t now = time(nullptr);
            cpc_model->data_.clear();
            cpc_model->dc_data_.clear();
            auto getKeys = [](int64_t timestamp) -> std::vector<std::string> {
                return {
                    std::to_string(risk::OneSecond) + std::to_string(timestamp),
                    std::to_string(risk::OneMinute) + std::to_string(timestamp),
                };
            };
            // 写入部分不过期的 key
            for (int i = 0; i < 100; ++i) {
                cpc_model->trigger_gc_count_ = 0;
                cpc_model->last_gc_timestamp_ = UINT32_MAX - kCPCModelGCPerSecond;
                cpc_model->Update(&ctx, &timer, getKeys(now + 1000 + i), "124", 0);
                cpc_model->trigger_gc_count_ = 0;
                cpc_model->last_gc_timestamp_ = UINT32_MAX - kCPCModelGCPerSecond;
                cpc_model->Update(&ctx, &timer, getKeys(now + 1000 + i), "125", 0);
                absl::flat_hash_set<std::string> st, st2;
                st.emplace("124");
                st.emplace("125");
                st2.emplace("124");
                st2.emplace("125");
                cpc_model->dc_data_[getKeys(now + 1000 + i)[0]] =
                    std::make_pair(Property{}, std::move(st));
                cpc_model->dc_data_[getKeys(now + 1000 + i)[1]] =
                    std::make_pair(Property{}, std::move(st2));
                cpc_model->dc_list_total_count_ += 4;
            }
            ASSERT_EQ(cpc_model->data_.size(), 200);
            for (int i = 0; i < 10000; ++i) {
                cpc_model->trigger_gc_count_ = 0;
                cpc_model->last_gc_timestamp_ = UINT32_MAX - kCPCModelGCPerSecond;
                cpc_model->Update(&ctx, &timer, getKeys(expired - i), "1234", 0);
                cpc_model->trigger_gc_count_ = 0;
                cpc_model->last_gc_timestamp_ = UINT32_MAX - kCPCModelGCPerSecond;
                cpc_model->Update(&ctx, &timer, getKeys(expired - i), "12345", 0);
                absl::flat_hash_set<std::string> st, st2;
                st.emplace("1234");
                st.emplace("12345");
                st2.emplace("1234");
                st2.emplace("12345");
                cpc_model->dc_data_[getKeys(expired - i)[0]] =
                    std::make_pair(Property{}, std::move(st));
                cpc_model->dc_data_[getKeys(expired - i)[1]] =
                    std::make_pair(Property{}, std::move(st2));
                cpc_model->dc_list_total_count_ += 4;
            }
            ASSERT_EQ(cpc_model->data_.size(), 20200);
            ASSERT_EQ(cpc_model->dc_data_.size(), 20200);
            ASSERT_EQ(cpc_model->dc_list_total_count_, 40400);
            for (auto sdata : cpc_model->dc_data_) {
                ASSERT_EQ(sdata.second.second.size(), 2);
            }
            cpc_model->last_gc_timestamp_ = 0;
            cpc_model->Update(&ctx, &timer, getKeys(expired), "123", 0);
            // -4 是因为写入触发了 dump cpc, 清除了 dc 的 key, 两个 key 各两个 value
            ASSERT_EQ(cpc_model->dc_list_total_count_, 40400 - kCPCModelMinGCLimit * 2 - 4);
            ASSERT_EQ(cpc_model->dc_data_.size(), 20200 - kCPCModelMinGCLimit - 2);
            ASSERT_EQ(cpc_model->data_.size(), 20200 - kCPCModelMinGCLimit * 2);
            cpc_model->last_gc_timestamp_ = UINT32_MAX - kCPCModelGCPerSecond;
            cpc_model->trigger_gc_count_ = 10000;
            cpc_model->Update(&ctx, &timer, getKeys(expired), "123", 0);
            // -4 是因为写入触发了 dump cpc, 清除了 dc 的 key, 两个 key 各两个 value
            ASSERT_EQ(cpc_model->dc_list_total_count_, 40400 - kCPCModelMinGCLimit * 4 - 4);
            ASSERT_EQ(cpc_model->dc_data_.size(), 20200 - kCPCModelMinGCLimit * 2 - 2);
            ASSERT_EQ(cpc_model->data_.size(), 20200 - kCPCModelMinGCLimit * 4);
            cpc_model->DoGC(&ctx, getKeys(expired), true, std::time(0));
            ASSERT_EQ(cpc_model->data_.size(), 200);
            ASSERT_EQ(cpc_model->dc_data_.size(), 200);
            ASSERT_EQ(cpc_model->dc_list_total_count_, 400);
            op_logger.get()->ctx_vec_.clear();
            for (auto sdata : cpc_model->dc_data_) {
                ASSERT_EQ(sdata.second.second.size(), 2);
            }
        }
    }
    // === 序列化 ===
    {
        SubTestLog("serialize");
        auto cpc_model = obj.Model<CPCModel>();
        auto key = "test_dump";
        auto st = cpc_model->Update(&ctx, &timer, {key}, "1234", 1234, false, "uuid1");
        ASSERT_TRUE(st.ok());
        st = cpc_model->Update(&ctx, &timer, {key}, "12345", 1234, false, "uuid2");
        ASSERT_TRUE(st.ok());
        std::vector<partition::LogItem> logs;
        auto pages = cpc_model->DumpNewPages({}, logs);
        ASSERT_EQ(pages.size(), 1);
        partition::PageInfo page_info;
        page_info.data = pages[0].second;
        std::string new_key = "test_dump_key";
        std::unique_ptr<uint8_t[]> new_buf(new uint8_t[partition::Object::ComputeRawObjectSize(
            new_key.size(), model::ModelManager::GetModelId<CPCModel>())]);
        partition::Object new_obj(0, new_buf.get());
        new_obj.ConstructWithValues(new_buf.get(), model::ModelManager::GetModelId<CPCModel>(),
                                    new_key);
        st = model::ModelManager::Init(new_obj.ModelId(), new_obj.RawModelBuf(), &allocator,
                                       {page_info});
        auto new_cpc_model = new_obj.Model<CPCModel>();
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(cpc_model->ttl_, new_cpc_model->ttl_);
        ASSERT_EQ(cpc_model->max_timestamp_, new_cpc_model->max_timestamp_);
        ASSERT_EQ(cpc_model->data_.size(), new_cpc_model->data_.size());
        auto it1 = cpc_model->data_.begin();
        auto it2 = new_cpc_model->data_.begin();
        for (; it1 != cpc_model->data_.end() || it2 != new_cpc_model->data_.end(); ++it1, ++it2) {
            ASSERT_EQ(it1->first, it2->first);
            ASSERT_EQ(it1->second.first.cluster_id, it2->second.first.cluster_id);
            ASSERT_EQ(it1->second.first.deleted, it2->second.first.deleted);
            ASSERT_EQ(it1->second.first.page_id, it2->second.first.page_id);
            ASSERT_EQ(it1->second.first.timestamp, it2->second.first.timestamp);
            ASSERT_EQ(it1->second.second.to_string(), it1->second.second.to_string());
            ASSERT_EQ(it1->second.second.get_estimate(), it1->second.second.get_estimate());
            ASSERT_EQ(it1->second.second.get_num_coupons(), it1->second.second.get_num_coupons());
            ASSERT_EQ(it1->second.second.get_lower_bound(3), it1->second.second.get_lower_bound(3));
            ASSERT_EQ(it1->second.second.get_upper_bound(3), it1->second.second.get_upper_bound(3));
        }
        ASSERT_EQ(cpc_model->dc_data_.size(), new_cpc_model->dc_data_.size());
        for (auto it1 = new_cpc_model->dc_data_.begin(); it1 != new_cpc_model->dc_data_.end();
             ++it1) {
            auto it2 = cpc_model->dc_data_.find(it1->first);
            ASSERT_NE(it2, cpc_model->dc_data_.end());
            ASSERT_EQ(it1->second.first.cluster_id, it2->second.first.cluster_id);
            ASSERT_EQ(it1->second.first.deleted, it2->second.first.deleted);
            ASSERT_EQ(it1->second.first.page_id, it2->second.first.page_id);
            ASSERT_EQ(it1->second.first.timestamp, it2->second.first.timestamp);
            ASSERT_EQ(it1->second.second.size(), it2->second.second.size());
            for (auto v : it1->second.second) {
                ASSERT_TRUE(it2->second.second.contains(v));
            }
        }
        ASSERT_EQ(cpc_model->data_uuid_.size(), new_cpc_model->data_uuid_.size());
        for (auto it1 : new_cpc_model->data_uuid_) {
            auto it2 = cpc_model->data_uuid_.find(it1.first);
            ASSERT_NE(it2, cpc_model->data_uuid_.end());
            ASSERT_EQ(it1.second.first.cluster_id, it2->second.first.cluster_id);
            ASSERT_EQ(it1.second.first.deleted, it2->second.first.deleted);
            ASSERT_EQ(it1.second.first.page_id, it2->second.first.page_id);
            ASSERT_EQ(it1.second.first.timestamp, it2->second.first.timestamp);
            ASSERT_EQ(it1.second.second, it2->second.second);
        }
        op_logger.get()->ctx_vec_.clear();
    }
    // === absl flat_hash_set 序列化 & 反序列化 ===
    {
        SubTestLog("flat_hash_set serialize");
        absl::flat_hash_set<std::string> st;
        CPCModel cpc_model;
        st.insert("1");
        st.insert("2");
        st.insert("3");
        std::string data = cpc_model.serializeHashSet(st);
        auto new_st = cpc_model.deserializeHashSet(data);
        ASSERT_EQ(st.size(), new_st.size());
        for (auto v : st) {
            ASSERT_TRUE(new_st.contains(v));
        }
    }
    // === 请求去重 ===
    {
        SubTestLog("req repeat check");
        auto cpc_model = obj.Model<CPCModel>();
        auto key = "test_repeat";
        cpc_model->data_.clear();
        cpc_model->dc_data_.clear();
        cpc_model->dc_type_ = kCPCModelTypeDC;
        cpc_model->trigger_gc_count_ = 0;
        cpc_model->last_gc_timestamp_ = UINT32_MAX - kCPCModelGCPerSecond;
        // uuid 为空不去重
        ASSERT_TRUE(cpc_model->Update(&ctx, &timer, {key}, "1234", 1234, true, "").ok());
        ASSERT_EQ(1, cpc_model->dc_data_.size());
        ASSERT_NE(cpc_model->dc_data_.find(key), cpc_model->dc_data_.end());
        ASSERT_EQ(1, cpc_model->dc_data_[key].second.size());
        ASSERT_TRUE(cpc_model->Update(&ctx, &timer, {key}, "12345", 1234, true, "").ok());
        ASSERT_EQ(2, cpc_model->dc_data_[key].second.size());
        // uuid 第一次写入
        ASSERT_TRUE(cpc_model->Update(&ctx, &timer, {key}, "123456", 1234, true, "1").ok());
        ASSERT_EQ(3, cpc_model->dc_data_[key].second.size());
        // 写入同样的uuid
        ASSERT_FALSE(cpc_model->Update(&ctx, &timer, {key}, "1234567", 1234, true, "1").ok());
        ASSERT_EQ(3, cpc_model->dc_data_[key].second.size());
        // 写入不同的uuid
        ASSERT_TRUE(cpc_model->Update(&ctx, &timer, {key}, "1234567", 1234, true, "2").ok());
        ASSERT_EQ(4, cpc_model->dc_data_[key].second.size());
    }
}

// 验证 Scan 方法
TEST(RISKCPCModelScanTest, HighModel) {
    // === init ===
    risk::RiskTimerLogger timer("cpc_unit_test", "test", 0, 10 * 1000 * 1000 * 1000);
    std::string object_key = "test_key";
    std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
        object_key.size(), model::ModelManager::GetModelId<CPCModel>())]);
    std::unique_ptr<DummyOplogger4> op_logger(new DummyOplogger4());

    partition::Object obj(0, buf.get());
    obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<CPCModel>(), object_key);

    partition::CmdContext ctx;
    ctx.object = obj;
    ctx.op_logger = op_logger.get();
    ctx.ctrl = &ctrl;
    auto st = model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator, {});
    ASSERT_TRUE(st.ok());
    auto cpc_model = obj.Model<CPCModel>();
    time_t now = time(nullptr);
    for (int64_t i = 0; i < 100; ++i) {
        std::string field = std::to_string(now + i);
        for (int j = 0; j < 100; ++j) {
            st = cpc_model->Update(&ctx, &timer, {field}, std::to_string(j + i), 999999);
            ASSERT_TRUE(st.ok());
        }
    }
    double result = 0.0;
    ASSERT_TRUE(cpc_model->Scan({{std::to_string(now), std::to_string(now + 100)}}, &result).ok());
    ASSERT_NEAR(result, 199, 199 * kRelativeErrorForLgK12);
    ASSERT_TRUE(cpc_model
                    ->Scan({{std::to_string(now), std::to_string(now + 50)},
                            {std::to_string(now + 50), std::to_string(now + 100)}},
                           &result)
                    .ok());
    ASSERT_NEAR(result, 199, 199 * kRelativeErrorForLgK12);
}

// 验证 ScanForList 方法
TEST(RISKCPCModelScanForListTest, HighModel) {
    // === init ===
    risk::RiskTimerLogger timer("cpc_unit_test", "test", 0, 10 * 1000 * 1000 * 1000);
    std::string object_key = "test_key";
    std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
        object_key.size(), model::ModelManager::GetModelId<CPCModel>())]);
    std::unique_ptr<DummyOplogger4> op_logger(new DummyOplogger4());

    partition::Object obj(0, buf.get());
    obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<CPCModel>(), object_key);

    partition::CmdContext ctx;
    ctx.object = obj;
    ctx.op_logger = op_logger.get();
    ctx.ctrl = &ctrl;
    auto st = model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator, {});
    ASSERT_TRUE(st.ok());
    auto cpc_model = obj.Model<CPCModel>();
    // dc type
    time_t now = time(nullptr);
    for (int64_t i = 0; i < 10; ++i) {
        std::string field = std::to_string(now + i);
        for (int j = 0; j < 100; ++j) {
            st = cpc_model->Update(&ctx, &timer, {field}, std::to_string(j + i), 999999, true);
            ASSERT_TRUE(st.ok());
        }
    }
    absl::flat_hash_set<std::string> result;
    ASSERT_TRUE(
        cpc_model->ScanForList({{std::to_string(now), std::to_string(now + 100)}}, &result).ok());
    ASSERT_EQ(result.size(), 109);
    for (int i = 0; i < 109; ++i) {
        ASSERT_TRUE(result.contains(std::to_string(i)));
    }
    ASSERT_TRUE(cpc_model
                    ->ScanForList({{std::to_string(now), std::to_string(now + 5)},
                                   {std::to_string(now + 5), std::to_string(now + 10)}},
                                  &result)
                    .ok());
    ASSERT_EQ(result.size(), 109);
    for (int i = 0; i < 109; ++i) {
        ASSERT_TRUE(result.contains(std::to_string(i)));
    }
    // cpc type error
    st = cpc_model->Update(&ctx, &timer, {"123"}, "1", 999999, false);
    ASSERT_TRUE(st.ok());
    ASSERT_FALSE(
        cpc_model->ScanForList({{std::to_string(now), std::to_string(now + 100)}}, &result).ok());
}

// 验证 dc 写入正常
TEST(RISKCPCModelDCTest, HighModel) {
    // === init ===
    risk::RiskTimerLogger timer("cpc_unit_test", "test", 0, 10 * 1000 * 1000 * 1000);
    std::string object_key = "test_key";
    std::unique_ptr<uint8_t[]> buf(new uint8_t[partition::Object::ComputeRawObjectSize(
        object_key.size(), model::ModelManager::GetModelId<CPCModel>())]);
    std::unique_ptr<DummyOploggerNoOp> op_logger(new DummyOploggerNoOp());

    partition::Object obj(0, buf.get());
    obj.ConstructWithValues(buf.get(), model::ModelManager::GetModelId<CPCModel>(), object_key);

    partition::CmdContext ctx;
    ctx.object = obj;
    ctx.op_logger = op_logger.get();
    ctx.ctrl = &ctrl;
    auto st = model::ModelManager::Init(obj.ModelId(), obj.RawModelBuf(), &allocator, {});
    ASSERT_TRUE(st.ok());
    auto cpc_model = obj.Model<CPCModel>();
}

TEST(CPC_Precision_Test, LowModel) {
    return;
    // init
    srand(time(nullptr));
    char s[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    int len = 26 + 26 + 10;
    auto randomStr = [&s, len]() -> std::string {
        std::string res = "";
        for (int i = 0; i < 30; ++i) {
            res += s[rand() % len];
        }
        return res;
    };
    std::vector<int64_t> checkInit;
    checkInit.emplace_back(100);
    for (int i = 1; i < 10; ++i) {
        checkInit.emplace_back(checkInit[checkInit.size() - 1] * 10);
    }
    int64_t checkNeedLastMax = 100, checkNeedLastMin = 100, checkNeedLastGap = 1;
    auto checkNeedRecord = [&](int64_t now) -> bool {
        if (now < 100) {
            return true;
        }
        if (now < checkNeedLastMax) {
            return (now - checkNeedLastMin) % checkNeedLastGap == 0;
        }
        for (int i = 1; i < 10; ++i) {
            if (checkInit[i] > now) {
                checkNeedLastMin = checkInit[i - 1];
                checkNeedLastMax = checkInit[i];
                checkNeedLastGap = (checkNeedLastMax - checkNeedLastMin) / 100;
                return true;
            }
        }
        checkNeedLastMin = checkInit[checkInit.size() - 1];
        checkNeedLastMax = INT64_MAX;
        checkNeedLastGap *= 10;
        return true;
    };

    // test
    std::string res = "@@@query_res|=[", res2 = "@@@query_res|=[";
    std::vector<datasketches::cpc_sketch> cpcModel, cpcModel2;
    for (int i = 0; i < 10; ++i) {
        cpcModel.emplace_back(datasketches::cpc_sketch(12));
        cpcModel2.emplace_back(datasketches::cpc_sketch(14));
    }
    // {"want":1,"diff":0,"read_cost":344117,"write_cost":498371,"got":0},
    for (int64_t i = 0; i < 100000; ++i) {
        int64_t diff = 0, diff2 = 0;
        for (int j = 0; j < 10; ++j) {
            cpcModel[j].update(randomStr());
            cpcModel2[j].update(randomStr());
            diff += abs((int64_t)(cpcModel[j].get_estimate() + 0.5) - i - 1);
            diff2 += abs((int64_t)(cpcModel2[j].get_estimate() + 0.5) - i - 1);
        }
        if (checkNeedRecord(i)) {
            res += "{\"want\":" + std::to_string(i + 1) + ",\"diff\":";
            res2 += "{\"want\":" + std::to_string(i + 1) + ",\"diff\":";
            res += std::to_string(diff / 10) + ",\"read_cost\":0,\"write_cost\":0,\"got\":0},";
            res2 += std::to_string(diff2 / 10) + ",\"read_cost\":0,\"write_cost\":0,\"got\":0},";
        }
    }
    res[res.length() - 1] = ']';
    res2[res2.length() - 1] = ']';
    std::cout << res << std::endl;
    std::cout << res2 << std::endl;
    sleep(10);
    std::cout << std::endl;
}

}  // namespace model
}  // namespace bcache2
