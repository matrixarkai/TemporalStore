// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "model/risk_hash_model.h"

#include <absl/container/flat_hash_set.h>

#include <iostream>
#include <set>
#include <vector>

#include "model/risk_cpc_model.h"

namespace bcache2 {
namespace model {
void RiskHashOrSet::Del(partition::CmdContext* ctx, const std::string& key) {
    data_->Del(ctx, key);
}

void RiskHashOrSet::DoMinGC(partition::CmdContext* ctx, const std::vector<std::string>& keys,
                            std::time_t now) {
    // 从keys的前缀中获取到当前写入的精度(随机一个)，对该精度下的数据进行过期，每次过期1000个即终止，key的结束为前缀_(当前时间戳-ttl)
    size_t size = keys.size();
    std::string endTimestamp = std::to_string(GetEndTimestamp(now));
    for (auto key : keys) {
        std::string startPrefix = key.substr(kFieldPrecisionStart, kFieldPrecisionSize);
        std::string endPrefix;
        endPrefix += startPrefix;
        endPrefix += endTimestamp;
        DoGC(ctx, startPrefix, endPrefix, false);
    }
    // 清除过期 uuid
    DoUUIDGC(ctx, now);
}

void RiskHashOrSet::DoUUIDGC(partition::CmdContext* ctx, std::time_t now) {
    std::vector<std::string> expire_keys;
    auto startIter = data_->LowerBound(Uuid_key_prefix);
    std::string maxUUID = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
    auto endIter = data_->LowerBound(Uuid_key_prefix + maxUUID);
    std::string expiredUUIDTime = std::to_string(now - kUUIDExpiredTime);
    for (; startIter != endIter; ++startIter) {
        if (startIter.Second() < expiredUUIDTime) {
            expire_keys.emplace_back(startIter.First());
        }
    }
    for (const auto& k : expire_keys) {
        Del(ctx, k);
    }
}

void RiskHashOrSet::DoGC(partition::CmdContext* ctx, const std::string& startPrefix,
                        const std::string& endPrefix, bool isFull) {
    uint64_t count = 0;
    std::vector<std::string> expire_keys;
    auto startIter = data_->LowerBound(startPrefix);
    auto endIter = data_->LowerBound(endPrefix);
    for (; startIter != endIter; ++startIter) {
        expire_keys.emplace_back(startIter.First());
        if (!isFull && ++count >= kDefaultMinGCCount) {
            break;
        }
    }

    for (const auto& k : expire_keys) {
        Del(ctx, k);
    }
}

// 过期的key多保留一会，保留过期时间的1.2倍的时间
uint64_t RiskHashOrSet::GetEndTimestamp(std::time_t now) {
    return now - ttl_;
}

void RiskHashOrSet::DoFullGC(partition::CmdContext* ctx, const std::vector<std::string>& keys,
                             std::time_t now) {
    absl::flat_hash_set<std::string> prefixs;
    for (auto key : keys) {
        prefixs.insert(key.substr(kFieldPrecisionStart, kFieldPrecisionSize));
    }
    uint64_t endTimestamp = GetEndTimestamp(now);

    for (auto p : prefixs) {
        std::string startPrefix = p;
        std::string endPrefix;
        endPrefix += startPrefix;
        endPrefix += std::to_string(endTimestamp);
        DoGC(ctx, startPrefix, endPrefix, true);
    }
    // 清除过期 uuid
    DoUUIDGC(ctx, now);
}

void RiskHashOrSet::SetTTL(partition::CmdContext* ctx, uint64_t ttl) {
    if (ttl_ == 0L) {
        auto it = data_->Find(Ttl_field_key);
        if (it == data_->End()) {
            data_->Put(ctx, Ttl_field_key, std::to_string(ttl));
            ttl_ = ttl;
        } else {
            ttl_ = std::strtoull(it.Second().c_str(), nullptr, 10);
            if (ttl_ < ttl) {
                ttl_ = ttl;
                data_->Put(ctx, Ttl_field_key, std::to_string(ttl));
            }
        }
    } else {
        if (ttl_ < ttl) {
            ttl_ = ttl;
            data_->Put(ctx, Ttl_field_key, std::to_string(ttl));
        }
    }
}

uint64_t RiskHashOrSet::GetTTL(partition::CmdContext* ctx) {
    auto it = data_->Find(Ttl_field_key);
    if (it == data_->End()) {
        return 0L;
    } else {
        return std::strtoull(it.Second().c_str(), nullptr, 10);
    }
}

Status RiskHashOrSet::InnerBatchSet(partition::CmdContext* ctx, risk::RiskTimerLogger *timer,
                                    const std::vector<std::string>& keys,
                                    const std::vector<std::string>& vals,
                                    std::time_t now) {
    Status status;
    uint64_t size = data_->Size();
    if (size >= kMaxMapSize) {
        DoFullGC(ctx, keys, now);
        return Status::OutOfRange("the current size is exceed max");
    }
    if (size >= kMaxMapSize * 8 / 10) {  // 超过最大size的0.8,触发一次full gc
        DoFullGC(ctx, keys, now);
    }
    timer->AddCheckPoint("do_full_gc_check");
    auto key_size = keys.size();
    for (size_t i = 0; i < key_size; i++) {
        data_->Put(ctx, keys[i], vals[i]);
    }
    timer->AddCheckPoint("put");
    // 判断是否需要过期
    ++gc_write_cnt_;
    if (gc_write_cnt_ >= kGCPerWrite || now - last_gc_timestamp_ >= kGCPerSecond) {
        DoMinGC(ctx, keys, now);
        gc_write_cnt_ = 0;
        last_gc_timestamp_ = now;
        timer->AddCheckPoint("do_gc");
    }
    timer->AddCheckPoint("do_gc_check");

    return status;
}

Status RiskHashOrSet::CheckMinPrecisonFiledSize(partition::CmdContext* ctx,
                                                const std::vector<std::string>& keys) {
    if (keys.size() <= 0) {
        return Status::InvalidArgument("key is empty");
    }
    // 精度越细，枚举值中RiskPrecision中的值越小
    std::string min_precison_key = keys[0];
    for (auto key : keys) {
        if (key < min_precison_key) {
            min_precison_key = key;
        }
    }
    // 获取min_precison_key的前缀，用于遍历扫描他的filed大小
    auto prefix = min_precison_key.substr(0, kFieldPrecisionSize + kFieldTimestampSize);
    uint64_t end_start_uint = std::strtoull(prefix.c_str(), nullptr, 10) + 1L;
    auto startIter = data_->LowerBound(prefix);
    auto endIter = data_->LowerBound(std::to_string(end_start_uint));
    absl::flat_hash_set<std::string> fieldSet;
    for (; startIter != endIter; ++startIter) {
        fieldSet.insert(startIter.Second());
    }
    if (fieldSet.size() > kMaxFiledSize) {
        return Status::OutOfRange("the min precison's filed size exceed max");
    } else {
        return Status::OK();
    }
}

Status RiskHashOrSet::InnerIncrByAndSetValue(partition::CmdContext* ctx,
                                             risk::RiskTimerLogger *timer,
                                             const std::string change_value, std::string key,
                                             std::time_t now) {
    int64_t cur_val = 1;
    auto old_count_iter = data_->Find(key);
    if (old_count_iter != data_->End()) {
        cur_val += std::strtoll(old_count_iter.Second().c_str(), nullptr, 10);
    }
    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.push_back(key);
    values.push_back(std::to_string(cur_val));
    keys.push_back(Change_field_key);
    values.push_back(change_value);
    timer->AddCheckPoint("change_pre");
    InnerBatchSet(ctx, timer, keys, values, now);
    change_value_ = change_value;
    return Status::OK();
}
}  // namespace model
}  // namespace bcache2
