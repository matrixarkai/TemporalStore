// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once
#include <absl/container/flat_hash_map.h>

#include <string>
#include <utility>
#include <vector>

#include "model/orset_model.h"
#include "model/persistent_map.h"
#include "extension/risk/metrics_log.h"

// n 次之后触发 gc
const int kGCPerWrite = 100;
// n 秒之后触发 gc
const int kGCPerSecond = 60;
// 一次gc的元素个数
const int kDefaultMinGCCount = 1000;
// 一个PersistentMap的最大size
const int kMaxMapSize = 2000000;
// 针对dc场景，最小精度下fileds的size大小
const int kMaxFiledSize = 200;


#ifdef __RISK_HASH_FOR_UNIT_TEST__
const int32_t kUUIDExpiredTime = 3;
#else
const int32_t kUUIDExpiredTime = 300;  // uuid 过期时间 300 秒
#endif

const char Ttl_field_key[] = "6f956302-d4fc-4df8-8559-adbdb3dd4954";
const char Change_field_key[] = "04405c4a-27ea-4559-9b26-8cffdd105997";
const char Uuid_key_prefix[] = "st";

// field 中 精度占用的起始值和位数
const int kFieldPrecisionStart = 0;
const int kFieldPrecisionSize = 2;
const int kFieldTimestampStart = kFieldPrecisionStart + kFieldPrecisionSize;
const int kFieldTimestampSize = 10;
const int kFieldValueStart = kFieldTimestampStart + kFieldTimestampSize;

namespace bcache2 {
namespace model {

using RiskComputeFunc = std::function<void(const std::string& filed, const std::string& value)>;
using RiskUpsertFunc = std::function<int64_t(int64_t cur_val, int64_t pre_val)>;
/*
结构为：
key：              12346_255&#@shopOrder_dc（对应这BCache2中的Object_id）
fieldAndValues:    {$precison}{$timestamp}{$filed}  {$value}
说明：$precison 占用2位(使用枚举值)，$timestamp占用10位, 对于普通的计数$filed为null,
$value为int64_t整形，对于dc $value = $filed，字符串类型
ttl:(过期时间)，存在一个特殊的field中，ttl_field_8864中
*/
class RiskHashOrSet {
 public:
    // RiskHashModel(){}
    explicit RiskHashOrSet(model::PersistentMap<std::string, std::string>* data) : data_(data) {}
    RiskHashOrSet(RiskHashOrSet&& rmodel)
        : gc_write_cnt_(rmodel.gc_write_cnt_),
          ttl_(rmodel.ttl_),
          last_gc_timestamp_(rmodel.last_gc_timestamp_),
          data_(rmodel.data_),
          change_value_(rmodel.change_value_) {}
    RiskHashOrSet& operator=(RiskHashOrSet&& rmodel) {
        if (this == &rmodel) {
            return *this;
        }
        gc_write_cnt_ = rmodel.gc_write_cnt_;
        ttl_ = rmodel.ttl_;
        last_gc_timestamp_ = rmodel.last_gc_timestamp_;
        data_ = rmodel.data_;
        change_value_ = rmodel.change_value_;
        return *this;
    }
    RiskHashOrSet(const RiskHashOrSet&) = delete;
    RiskHashOrSet& operator=(const RiskHashOrSet&) = delete;
    // RiskHashOrSet& operator=(RiskHashOrSet&&) = delete;

    Status OnLoaded() { return Status::OK(); }

    Status CompareAndIncrBy(partition::CmdContext* ctx, risk::RiskTimerLogger *timer,
                            const std::string change_value, std::string key, uint64_t ttl,
                            const std::string& uuid = "") {
        // 前置判断请求是否重复
        std::time_t now = std::time(0);
        if (!InsertUUID(ctx, uuid, now)) {
            return Status::RiskAlreadyHandled("");
        }
        SetTTL(ctx, ttl);
        if (change_value_ == "") {
            // 从map中遍历获取
            auto it = data_->Find(Change_field_key);
            if (it == data_->End()) {
                // 确实没有，执行+1操作，并且设定新值
                return InnerIncrByAndSetValue(ctx, timer, change_value, key, now);
            } else {
                change_value_ = it.Second();
            }
        }
        if (change_value_ != change_value) {  // 如果之前的值和当前值不一致，不进行+1操作
            return InnerIncrByAndSetValue(ctx, timer, change_value, key, now);
        }
        return Status::OK();
    }

    Status BatchUpsert(partition::CmdContext* ctx, risk::RiskTimerLogger *timer,
                       const std::vector<std::string>& keys, const std::vector<int64_t>& vals,
                       uint64_t ttl, const RiskUpsertFunc& riskUpsertFunc,
                       const std::string& uuid) {
        // 前置判断请求是否重复
        std::time_t now = std::time(0);
        if (!InsertUUID(ctx, uuid, now)) {
            return Status::RiskAlreadyHandled("");
        }
        SetTTL(ctx, ttl);
        std::vector<std::string> cur_vals;
        size_t index = 0;
        for (const auto& key : keys) {
            auto it = data_->Find(key);
            int64_t cur_val = vals[index];
            if (it != data_->End()) {
                cur_val = riskUpsertFunc(cur_val, std::strtoll(it.Second().c_str(), nullptr, 10));
            }

            cur_vals.push_back(std::to_string(cur_val));
            index++;
        }
        timer->AddCheckPoint("set_pre");
        return InnerBatchSet(ctx, timer, keys, cur_vals, now);
    }

    Status BatchOverWrite(partition::CmdContext* ctx, risk::RiskTimerLogger *timer,
                          const std::vector<std::string>& keys,
                          const std::vector<std::string>& vals, uint64_t ttl) {
        SetTTL(ctx, ttl);
        return InnerBatchSet(ctx, timer, keys, vals, std::time(0));
    }

    //  查询范围为[startPrefix, endPrefix)
    Status Scan(partition::CmdContext* ctx, const std::string& startPrefix,
                const std::string& endPrefix, const RiskComputeFunc& riskComputeFunc) {
        // 增加过期机制
        // 定位开始点和结束点, 获取最小的大于等于这个时间戳的key, 事实上由于key有后缀,
        // 所有key都应当大于prefix
        auto startIter = data_->LowerBound(startPrefix);
        auto endIter = data_->LowerBound(endPrefix);
        for (; startIter != endIter; ++startIter) {
            riskComputeFunc(startIter.First(), startIter.Second());
        }
        return Status::OK();
    }

    void Query(const std::vector<std::string>& keys,
               absl::flat_hash_map<std::string, std::string>* values) {
        if (values == nullptr) {
            return;
        }
        bcache2::model::PersistentMap<std::string, std::string>::Iterator iter = data_->Begin();
        for (auto key : keys) {
            iter = data_->Find(key);
            if (iter != data_->End()) {
                (*values)[key] = iter.Second();
            }
        }
        return;
    }
    std::string FieldList(const std::string& start, const std::string& end) {
        auto startIter = data_->LowerBound(start);
        auto endIter = data_->LowerBound(end);
        std::string res;
        for (; startIter != endIter && startIter != data_->End(); ++startIter) {
            res += startIter.First() + ",";
        }
        return res;
    }
    uint64_t Size() { return data_->Size(); }
    void Del(partition::CmdContext* ctx, const std::string& key);
    void DoMinGC(partition::CmdContext* ctx, const std::vector<std::string>& keys,
                 std::time_t now);
    void DoFullGC(partition::CmdContext* ctx, const std::vector<std::string>& keys,
                  std::time_t now);
    std::string RepeatData() {
        std::string res = "";
        auto startIter = data_->LowerBound(Uuid_key_prefix);
        std::string maxUUID = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
        auto endIter = data_->LowerBound(Uuid_key_prefix + maxUUID);
        for (; startIter != endIter; ++startIter) {
            res += startIter.First() + ":" + startIter.Second() + ",";
        }
        return res;
    }

 private:
    bool InsertUUID(partition::CmdContext* ctx, const std::string& uuid, uint32_t now) {
        if (uuid == "") return true;
        std::string full_uuid = Uuid_key_prefix + uuid;
        // 找到具体的uuid了
        auto iter = data_->Find(full_uuid);
        if (iter != data_->End()) {
            // uuid 超过过期时间认为不存在
            if (iter.Second() > std::to_string(now - kUUIDExpiredTime)) {
                return false;
            }
        }
        // 没找到进行插入
        data_->Put(ctx, full_uuid, std::to_string(now));
        return true;
    }
    void DoGC(partition::CmdContext* ctx, const std::string& startPrefix,
            const std::string& endPrefix, bool isFull);
    void DoUUIDGC(partition::CmdContext* ctx, std::time_t now);
    void SetTTL(partition::CmdContext* ctx, uint64_t ttl);
    uint64_t GetTTL(partition::CmdContext* ctx);
    uint64_t GetEndTimestamp(std::time_t now);
    uint64_t GetOldestTimestamp(const std::string& prefix);
    Status InnerBatchSet(partition::CmdContext* ctx, risk::RiskTimerLogger *timer,
                         const std::vector<std::string>& keys,
                         const std::vector<std::string>& vals,
                         std::time_t now);
    Status CheckMinPrecisonFiledSize(partition::CmdContext* ctx,
                                     const std::vector<std::string>& keys);

    Status InnerIncrByAndSetValue(partition::CmdContext* ctx, risk::RiskTimerLogger *timer,
                                  const std::string change_value, std::string key,
                                  std::time_t now);

 private:
    uint16_t gc_write_cnt_ = 0L;
    uint32_t ttl_ = 0L;
    uint32_t last_gc_timestamp_ = 0L;
    model::PersistentMap<std::string, std::string>* data_ = nullptr;
    // 用于change命令，保持其中的最终值
    std::string change_value_ = "";
};
using RiskHashModel = bcache2::model::OrSetModel<std::string, std::string, RiskHashOrSet>;
}  // namespace model
}  // namespace bcache2
