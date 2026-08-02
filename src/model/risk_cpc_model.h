// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/container/flat_hash_set.h>
#include <byte/include/byte_log.h>
#if __has_include(<gtest/gtest_prod.h>)
#include <gtest/gtest_prod.h>
#else
#ifndef FRIEND_TEST
#define FRIEND_TEST(test_case_name, test_name)
#endif
#endif

#include <algorithm>
#include <functional>
#include <list>
#include <string>
#include <utility>
#include <vector>

#include "extension/risk/window.h"
#include "extension/risk/metrics_log.h"
#include "model/cpc/cpc_sketch.h"
#include "model/cpc/cpc_union.h"
#include "model/persistent_map.h"
#include "partition/cmd_context.h"

namespace bcache2 {
namespace model {

const char kCPCModelTtlKey[] = "__cpc_ttl_oplog_key__";
const char kCPCModelTypeKey[] = "__cpc_dc_type_oplog_key__";
const char kCPCModelDCKeyPrefix[] = "_dc_";
const char kCPCModelDumpPartEnd[] = "__one_part_end__";
const char kCPCModelDontUpgradeCPC[] = "__cpc_dont_upgrade_oplog_key__";
const char kCPCModelUUIDKeyPrefix[] = "_st_";

const int8_t kCPCModelTypeDC = 0;
const int8_t kCPCModelTypeLDC = 1;

const uint8_t kCPCModelLgK = 12;
// 这个 size 判断的是所有 field 的集合
// 当前写入一次保存两个精度的 field, 对应业务上的 key 数量需要减半
const int32_t kCPCModelTransforLDCSize = 4096;  // 总共的key数量达到 n 就转换成 ldc 进行服务
const int16_t kCPCModelGCPerWrite = 100;  // n 次之后触发 gc
const int16_t kCPCModelGCPerSecond = 60;  // n 秒之后触发 gc
const int32_t kCPCModelMinGCLimit = 1000;         // 每次 min gc 清除的 field 的数量
const int16_t kCPCModelTransforCheckPreWrite = 100;  // 每 n 次写入触发一次是否转换成 cpc 的 check
// cpc 预计的内存大小小于 n 字节的时候会全量 dump
const int32_t kCPCModelDumpMemSize = 512;
const uint32_t kCPCModelCPCOpLogPerUpdate = 20;  // 每 n 次更新写一次cpc oplog

#ifdef __CPC_FOR_UNIT_TEST__
const int32_t kCPCModelListSizeLimit = 2000;
const uint32_t kCPCModelUUIDExpiredTime = 3;
#else
const int32_t kCPCModelListSizeLimit = 2000000;  // 不升级模式, 总共的 key 最多 n 个
const uint32_t kCPCModelUUIDExpiredTime = 300;  // uuid 过期时间 300 秒
#endif

// field 中 各种类型占用的起始值和位数 {precision[2]}{timestamp[10]}
const int kCPCModelFieldPrecisionStart = 0;
const int kCPCModelFieldPrecisionSize = 2;
const int kCPCModelFieldTimestampStart = kCPCModelFieldPrecisionStart + kCPCModelFieldPrecisionSize;
const int kCPCModelFieldTimestampSize = 10;
const int kCPCModelFieldKeySize = kCPCModelFieldPrecisionSize + kCPCModelFieldTimestampSize;
const int kCPCModelFieldDcPrefixSize = std::string(kCPCModelDCKeyPrefix).length();
const int kCPCModelUUIDKeyPrefixSize = std::string(kCPCModelUUIDKeyPrefix).length();
const int kCPCModelOplogDCValueStart = kCPCModelFieldDcPrefixSize + kCPCModelFieldKeySize;
/**
 * cpc key = {precision(2)}{timestamp(10)}
 * dc key = {precision(2)}{timestamp(10)}
 * cpc oplog key = cpc key
 * dc oplog key = {dc prefix(_dc_)}{precision(2)}{timestamp(10)}{value}
 */
class CPCModel {
 public:
    CPCModel() {}
    virtual ~CPCModel() {}

    CPCModel(CPCModel&& rmodel)
        : dont_upgrade_cpc_(rmodel.dont_upgrade_cpc_),
          dc_type_(rmodel.dc_type_),
          trigger_gc_count_(rmodel.trigger_gc_count_),
          cpc_transfor_check_cnt_(rmodel.cpc_transfor_check_cnt_),
          last_gc_timestamp_(rmodel.last_gc_timestamp_),
          ttl_(rmodel.ttl_),
          dc_list_total_count_(rmodel.dc_list_total_count_),
          dc_list_total_mem_size_(rmodel.dc_list_total_mem_size_),
          max_timestamp_(rmodel.max_timestamp_),
          data_(std::move(rmodel.data_)),
          dc_data_(std::move(rmodel.dc_data_)),
          data_uuid_(std::move(rmodel.data_uuid_)) {}
    CPCModel(const CPCModel&) = delete;
    CPCModel& operator=(const CPCModel&) = delete;
    CPCModel& operator=(CPCModel&&) = delete;
    // ================ 业务调用函数 ================
    Status Update(partition::CmdContext* ctx, risk::RiskTimerLogger *timer,
                  const std::vector<std::string>& keys, const std::string& value,
                  uint64_t ttl, bool dont_upgrade_cpc = false, const std::string& uuid = "") {
        // 前置判断请求是否重复
        std::time_t now = std::time(0);
        if (!insertUUID(ctx, uuid, now)) {
            return Status::RiskAlreadyHandled("");
        }
        // 如果禁止升级标识不一致, 则需要修改
        if (UNLIKELY(dont_upgrade_cpc != dont_upgrade_cpc_)) {
            if (dont_upgrade_cpc) {
                if (dc_type_ == kCPCModelTypeLDC) {
                    return Status::Internal("model type is already ldc, can not downgrade");
                }
            }
            Property property;
            property.page_id = 0;
            property.cluster_id = 0;
            property.deleted = false;
            property.timestamp = getAndSaveMaxTimestamp();
            writeOpLog(ctx, kCPCModelDontUpgradeCPC, dont_upgrade_cpc ? "1" : "0", property);
            dont_upgrade_cpc_ = dont_upgrade_cpc;
        }

        bool need_dump_cpc = false;
        Property property;
        property.page_id = 0;
        property.cluster_id = 0;
        property.deleted = 0;

        // 判断是否写入 dc
        bool needWriteDC = true;
        if (UNLIKELY(dont_upgrade_cpc_ && dc_list_total_count_ >= kCPCModelListSizeLimit)) {
            needWriteDC = false;
        }
        bool isEmplaced = false;
        timer->AddCheckPoint("cpc_pre");
        for (const auto& key : keys) {
            need_dump_cpc = false;
            // 判断是否写入只写 cpc
            if (dc_type_ == kCPCModelTypeLDC) {
                auto iter = data_.find(key);
                // cpc 不存在或者较小时, 全量打 log, 此时不需要写入 dc
                if (iter == data_.end() || iter->second.second.mem_size() < kCPCModelDumpMemSize) {
                    need_dump_cpc = true;
                    needWriteDC = false;
                }
            }

            // 写入 dc set
            if (needWriteDC) {
                property.timestamp = getAndSaveMaxTimestamp();
                writeOpLog(ctx, kCPCModelDCKeyPrefix + key + value, "", property);
                auto dc_iter = dc_data_.find(key);
                if (dc_iter == dc_data_.end()) {
                    absl::flat_hash_set<std::string> dc_set;
                    dc_set.emplace(value);
                    dc_data_[key] = std::make_pair(property, std::move(dc_set));
                    dc_list_total_count_ += 1;
                    dc_list_total_mem_size_ += value.length();
                } else {
                    // 通过 emplace 的 second 参数判断是否新增 (bool)
                    isEmplaced = dc_iter->second.second.emplace(value).second;
                    dc_list_total_count_ += isEmplaced ? 1 : 0;
                    dc_list_total_mem_size_ += isEmplaced ? value.length() : 0;
                    // 只有在 LDC 的场景下, 并且累积了足够多的写入之后, 才需要写入 oplog
                    if (UNLIKELY(dc_type_ == kCPCModelTypeLDC &&
                                 dc_iter->second.second.size() >= kCPCModelCPCOpLogPerUpdate)) {
                        need_dump_cpc = true;
                    }
                }
            }

            // 只有 cpc 类型的数据才会写入 cpc
            if (dc_type_ == kCPCModelTypeLDC) {
                property.timestamp = getAndSaveMaxTimestamp();
                auto iter = data_.find(key);
                std::string logValue;
                if (iter == data_.end()) {
                    auto cpc_model = datasketches::cpc_sketch(kCPCModelLgK);
                    cpc_model.update(value);
                    // 第一次写入 key 的时候, 也需要 dump 一次 oplog
                    if (dc_type_ == kCPCModelTypeLDC) {
                        need_dump_cpc = true;
                        google::protobuf::io::StringOutputStream item_output(&logValue);
                        google::protobuf::io::CodedOutputStream item_stream(&item_output);
                        cpc_model.serialize(item_stream);
                    }
                    data_[key] = std::make_pair(property, std::move(cpc_model));
                } else {
                    iter->second.second.update(value);
                    if (need_dump_cpc) {
                        google::protobuf::io::StringOutputStream item_output(&logValue);
                        google::protobuf::io::CodedOutputStream item_stream(&item_output);
                        iter->second.second.serialize(item_stream);
                    }
                    property.timestamp = std::max(iter->second.first.timestamp, property.timestamp);
                    iter->second.first.timestamp = property.timestamp;
                }
                // dump 完 cpc 的 key 之后, 清除 dc 的数据, 只有在 ldc 的模式下才会 dump cpc 数据
                if (need_dump_cpc) {
                    writeOpLog(ctx, key, logValue, property);
                    delFullDcSet(ctx, key);
                    timer->AddCheckPoint("dump_cpc");
                }
            }
        }

        setTtl(ctx, ttl);
        timer->AddCheckPoint("cpc_write");
        // 判断是否需要 GC
        ++trigger_gc_count_;
        if (trigger_gc_count_ >= kCPCModelGCPerWrite ||
            now >= last_gc_timestamp_ + kCPCModelGCPerSecond) {
            DoGC(ctx, keys, false, now);
            trigger_gc_count_ = 0;
            last_gc_timestamp_ = now;
            timer->AddCheckPoint("do_gc");
        }
        timer->AddCheckPoint("check_gc");
        // 判断是否需要切换到 LDC 类型
        if (dc_type_ == kCPCModelTypeDC && !dont_upgrade_cpc_) {
            if (UNLIKELY(++cpc_transfor_check_cnt_ >= kCPCModelTransforCheckPreWrite)) {
                if (checkNeedTransforToLDCType()) {
                    changeToCPC(ctx);
                    timer->AddCheckPoint("change_cpc");
                }
                cpc_transfor_check_cnt_ = 0;
            }
        }
        return Status::OK();
    }
    // 传入的 range -> <start, end>, 扫描的区间为左闭右开区间 [start, end)
    Status Scan(std::vector<risk_tool::RiskQueryRange> ranges, double* result) {
        if (result == nullptr) {
            return Status::InvalidArgument("result ptr is nullptr");
        }
        if (dc_type_ == kCPCModelTypeLDC) {
            datasketches::cpc_union union_impl(kCPCModelLgK);
            for (const auto& range : ranges) {
                auto start = data_.lower_bound(range.begin);
                auto end = data_.lower_bound(range.end);
                for (; start != end; ++start) {
                    union_impl.update(start->second.second);
                }
            }
            (*result) = union_impl.get_result().get_estimate();
        } else {
            absl::flat_hash_set<std::string> result_set;
            for (const auto& range : ranges) {
                auto start = dc_data_.lower_bound(range.begin);
                auto end = dc_data_.lower_bound(range.end);
                for (; start != end; ++start) {
                    for (const std::string& key : start->second.second) {
                        result_set.emplace(key);
                    }
                }
            }
            (*result) = static_cast<double>(result_set.size());
        }
        return Status::OK();
    }

    // 传入的 range -> <start, end>, 扫描的区间为左闭右开区间 [start, end), 返回列表详情
    Status ScanForList(std::vector<risk_tool::RiskQueryRange> ranges,
                       absl::flat_hash_set<std::string>* result) {
        if (result == nullptr) {
            return Status::InvalidArgument("result ptr is nullptr");
        }
        if (dc_type_ == kCPCModelTypeLDC || dont_upgrade_cpc_ == false) {
            return Status::Internal("ldc mode can not get list detail.");
        } else {
            for (const auto& range : ranges) {
                auto start = dc_data_.lower_bound(range.begin);
                auto end = dc_data_.lower_bound(range.end);
                for (; start != end; ++start) {
                    for (const std::string& key : start->second.second) {
                        (*result).emplace(key);
                    }
                }
            }
        }
        return Status::OK();
    }

    Status BatchDel(partition::CmdContext* ctx, const std::vector<std::string>& keys) {
        for (const auto& key : keys) {
            del(ctx, key);
        }
        for (const std::string& key : keys) {
            delFullDcSet(ctx, key);
        }
        return Status::OK();
    }

    Status DoGC(partition::CmdContext* ctx, const std::vector<std::string>& keys, bool isFull,
                std::time_t now) {
        // 从keys的前缀中获取精度，对该精度下的数据进行过期，每次过期部分数据，key的结束为前缀_(当前时间戳-ttl)
        for (size_t i = 0; i < keys.size(); ++i) {
            uint64_t endTimestamp = getEndTimestamp(now);
            std::string startPrefix =
                keys[i].substr(kCPCModelFieldPrecisionStart, kCPCModelFieldPrecisionSize);
            std::string endPrefix;
            endPrefix += startPrefix;
            endPrefix += std::to_string(endTimestamp);
            doExpire(ctx, startPrefix, endPrefix, isFull ? 0 : kCPCModelMinGCLimit);
        }
        uuidGc(ctx, now);
        return Status::OK();
    }

    // ================ 管理接口调用函数 ================
    Status ScanForManager(const std::string& range_start, const std::string& range_end,
                          absl::flat_hash_map<std::string, std::string>* res) {
        if (res == nullptr) {
            return Status::InvalidArgument("result ptr is nullptr");
        }
        if (dc_type_ == kCPCModelTypeLDC) {
            auto startIter = data_.lower_bound(range_start);
            auto endIter = data_.lower_bound(range_end);
            for (; startIter != endIter && startIter != data_.end(); ++startIter) {
                (*res)[startIter->first] =
                    std::to_string(startIter->second.second.get_estimate() + 0.5);
            }
        } else {
            auto startIter = dc_data_.lower_bound(range_start);
            auto endIter = dc_data_.lower_bound(range_end);
            for (; startIter != endIter && startIter != dc_data_.end(); ++startIter) {
                std::string subRes = "";
                for (const std::string& value : startIter->second.second) {
                    subRes += value + ",";
                }
                (*res)[startIter->first] = std::move(subRes);
            }
        }
        return Status::OK();
    }
    Status Query(const std::vector<std::string>& keys,
                 absl::flat_hash_map<std::string, std::string>* res) {
        if (res == nullptr) {
            return Status::InvalidArgument("result ptr is nullptr");
        }
        if (dc_type_ == kCPCModelTypeLDC) {
            auto iter = data_.begin();
            for (auto key : keys) {
                iter = data_.find(key);
                if (iter != data_.end()) {
                    (*res)[key] = std::to_string(iter->second.second.get_estimate() + 0.5);
                }
            }
        } else {
            auto iter = dc_data_.begin();
            for (auto key : keys) {
                iter = dc_data_.find(key);
                if (iter != dc_data_.end()) {
                    (*res)[key] = std::to_string(iter->second.second.size());
                }
            }
        }
        return Status::OK();
    }
    std::string FieldList(const std::string& range_start, const std::string& range_end) {
        std::string res = "";
        if (dc_type_ == kCPCModelTypeLDC) {
            auto startIter = data_.lower_bound(range_start);
            auto endIter = data_.lower_bound(range_end);
            for (; startIter != endIter && startIter != data_.end(); ++startIter) {
                res += startIter->first + ",";
            }
        } else {
            auto startIter = dc_data_.lower_bound(range_start);
            auto endIter = dc_data_.lower_bound(range_end);
            for (; startIter != endIter && startIter != dc_data_.end(); ++startIter) {
                res += startIter->first + ",";
            }
        }
        return res;
    }
    uint64_t Size() {
        if (dc_type_ == kCPCModelTypeLDC) {
            return data_.size();
        } else {
            return dc_data_.size();
        }
    }
    Status ForceChangeCPC(partition::CmdContext* ctx) {
        if (dont_upgrade_cpc_) {
            return Status::Internal("can not change to cpc cause model dont upgrade cpc");
        }
        changeToCPC(ctx);
        return Status::OK();
    }
    std::string GetDcType() {
        switch (dc_type_) {
        case kCPCModelTypeDC:
            return "DC";
        case kCPCModelTypeLDC:
            return "LDC";
        default:
            return "UNKNOWN<" + std::to_string(dc_type_) + ">";
        }
    }
    std::string RepeatData() {
        std::string res = "";
        for (auto iter = data_uuid_.begin(); iter != data_uuid_.end(); ++iter) {
            res += iter->first + ":" + std::to_string(iter->second.second) + ",";
        }
        return res;
    }

    // ================ 框架调用函数 ================
    Status Init(Allocator* allocator, std::vector<partition::PageInfo> pages) {
        data_ = BtreeMap<std::string, std::pair<Property, datasketches::cpc_sketch>>(
            std::less<std::string>(), Allocator::StlWrapper<std::pair<std::string,
                std::pair<Property, datasketches::cpc_sketch>>>(allocator));
        dc_data_ = BtreeMap<std::string, std::pair<Property, absl::flat_hash_set<std::string>>>(
            std::less<std::string>(),
            Allocator::StlWrapper<std::pair<std::string,
                std::pair<Property, absl::flat_hash_set<std::string>>>>(allocator));
        data_uuid_ = BtreeMap<std::string, std::pair<Property, uint32_t>>(std::less<std::string>(),
            Allocator::StlWrapper<std::pair<std::string,
                std::pair<Property, uint32_t>>>(allocator));
        datasketches::cpc_sketch deserializer;
        uint16_t page_id = 0;
        int64_t total_dc_mem_size = 0;
        for (size_t i = 0; i < pages.size(); ++i) {
            if (i > 0) {
                // ordered by version to protect data correctness
                BYTE_ASSERT(pages[i].header.version() > pages[i - 1].header.version());
            }

            const auto& page = pages[i];
            // 统一结构, 每个 page 开头都是 ttl / dc_type / need_upgrade
            google::protobuf::io::ArrayInputStream input(page.data.data(), page.data.size());
            google::protobuf::io::CodedInputStream stream(&input);
            uint8_t cluster_id = 0;
            bool deleted = false;
            uint64_t timestamp = 0;
            // read ttl
            std::string ttl_data;
            std::string ttl_key;
            if (!ReadKvItemFromStream(&stream, &ttl_key, &ttl_data, &cluster_id, &deleted,
                                      &timestamp)) {
                return Status::DataLoss("Invalid kv item, ttl not found");
            }
            if (ttl_key != kCPCModelTtlKey) {
                return Status::DataLoss("Invalid kv item, ttl key error=" + ttl_key);
            }
            max_timestamp_ = std::max(max_timestamp_, timestamp);
            ttl_ = std::max(ttl_, (int32_t)std::strtoll(ttl_data.c_str(), nullptr, 10));
            // read dc_type
            std::string dc_type_data;
            std::string dc_type_key;
            if (!ReadKvItemFromStream(&stream, &dc_type_key, &dc_type_data, &cluster_id, &deleted,
                                      &timestamp)) {
                return Status::DataLoss("Invalid kv item, dc_type not found");
            }
            if (dc_type_key != kCPCModelTypeKey) {
                return Status::DataLoss("Invalid kv item, dc_type key error=" + dc_type_key);
            }
            max_timestamp_ = std::max(max_timestamp_, timestamp);
            dc_type_ = std::strtol(dc_type_data.c_str(), nullptr, 10);
            if (dc_type_ != kCPCModelTypeDC && dc_type_ != kCPCModelTypeLDC) {
                return Status::DataLoss("Invalid kv item, dc_type value error=" + dc_type_data);
            }
            // read need_upgrade
            std::string dont_upgrade_data;
            std::string dont_upgrade_key;
            if (!ReadKvItemFromStream(&stream, &dont_upgrade_key, &dont_upgrade_data, &cluster_id,
                                      &deleted, &timestamp)) {
                return Status::DataLoss("Invalid kv item, dont_upgrade not found");
            }
            if (dont_upgrade_key != kCPCModelDontUpgradeCPC) {
                return Status::DataLoss("Invalid kv item, dont_upgrade key error=" +
                                        dont_upgrade_key);
            }
            max_timestamp_ = std::max(max_timestamp_, timestamp);
            dont_upgrade_cpc_ = dont_upgrade_data == "0" ? false : true;
            if (dc_type_ == kCPCModelTypeLDC && dont_upgrade_cpc_ == true) {
                return Status::DataLoss("Invalid kv item, dont_upgrade is true with LDC type");
            }

            // 尝试读取 _data
            while (stream.CurrentPosition() < static_cast<int>(page.data.size())) {
                std::string key;
                std::string value_data;
                if (!ReadKvItemFromStream(&stream, &key, &value_data, &cluster_id, &deleted,
                                          &timestamp)) {
                    return Status::DataLoss("Invalid kv item");
                }
                // 读取到分隔 key 终止读取
                if (key == kCPCModelDumpPartEnd) {
                    break;
                }
                google::protobuf::io::ArrayInputStream input(value_data.data(), value_data.size());
                google::protobuf::io::CodedInputStream stream(&input);
                datasketches::cpc_sketch value = deserializer.deserialize(stream);
                BYTE_ASSERT(!deleted);
                Property property;
                property.page_id = page_id;
                property.cluster_id = cluster_id;
                property.deleted = deleted;
                property.timestamp = timestamp;

                data_[std::move(key)] = std::make_pair(property, std::move(value));
                max_timestamp_ = std::max(max_timestamp_, property.timestamp);
            }

            // 尝试读取 _dc_data, 如果是 cpc 模式则需要更新到 cpc_model 中
            while (stream.CurrentPosition() < static_cast<int>(page.data.size())) {
                std::string key;
                std::string value_data;
                if (!ReadKvItemFromStream(&stream, &key, &value_data, &cluster_id, &deleted,
                                          &timestamp)) {
                    return Status::DataLoss("Invalid kv item");
                }
                // 读取到分隔 key 终止读取
                if (key == kCPCModelDumpPartEnd) {
                    break;
                }
                BYTE_ASSERT(!deleted);
                Property property;
                property.page_id = page_id;
                property.cluster_id = cluster_id;
                property.deleted = deleted;
                property.timestamp = timestamp;
                int64_t sub_dc_mem_size = 0;
                absl::flat_hash_set<std::string> dc_data =
                    deserializeHashSet(value_data, &sub_dc_mem_size);
                total_dc_mem_size += sub_dc_mem_size;
                // ldc 模式下需要更新 cpc model
                if (dc_type_ == kCPCModelTypeLDC) {
                    // 读取到 dc data 之后, 如果存在对应的 cpc data, 则需要更新, 不存在需要创建
                    auto cpc_data = data_.find(key);
                    if (cpc_data != data_.end()) {
                        for (const std::string& v : dc_data) {
                            cpc_data->second.second.update(v);
                        }
                    } else {
                        auto value = datasketches::cpc_sketch(kCPCModelLgK);
                        for (const std::string& v : dc_data) {
                            value.update(v);
                        }
                        data_[key] = std::make_pair(property, std::move(value));
                    }
                }
                dc_data_[std::move(key)] = std::make_pair(property, std::move(dc_data));
                max_timestamp_ = std::max(max_timestamp_, property.timestamp);
            }

            // 尝试读取 uuid
            while (stream.CurrentPosition() < static_cast<int>(page.data.size())) {
                std::string key;
                std::string value_data;
                if (!ReadKvItemFromStream(&stream, &key, &value_data, &cluster_id, &deleted,
                                          &timestamp)) {
                    // 如果读到的 uuid 数据异常, 则直接进行抛弃
                    continue;
                }
                BYTE_ASSERT(!deleted);
                Property property;
                property.page_id = page_id;
                property.cluster_id = cluster_id;
                property.deleted = deleted;
                property.timestamp = timestamp;
                int64_t uuidTime = std::strtoll(value_data.c_str(), nullptr, 10);
                if (uuidTime <=  UINT32_MAX) {
                    data_uuid_[key] = std::make_pair(property, uuidTime);
                }
            }
            ++page_id;
        }
        // 计算全部 dc 数据的个数
        int64_t total_dc_field = 0;
        for (const auto& dcSet : dc_data_) {
            total_dc_field += dcSet.second.second.size();
        }
        dc_list_total_count_ = total_dc_field;
        dc_list_total_mem_size_ = total_dc_mem_size;
        return Status::OK();
    }

    static Status BlindDumpNewPages(const std::vector<partition::PageIndex>& pages,
                                    const std::vector<partition::LogItem>& logs,
                                    std::vector<std::pair<uint16_t, std::string>>* new_pages) {
        return Status::Unimplemented("Not impl");
    }

    // 统一结构, 每个 page 开头都必须是 ttl / dc_type / need_upgrade
    std::vector<std::pair<uint16_t, std::string>> DumpNewPages(
        const std::vector<partition::PageIndex>& pages,
        const std::vector<partition::LogItem>& logs) {
        // do not multi-page for cpc model yet
        BYTE_ASSERT(pages.size() <= 1) << pages.size();

        std::vector<std::pair<uint16_t, std::string>> new_pages;
        std::string page;
        google::protobuf::io::StringOutputStream output(&page);
        google::protobuf::io::CodedOutputStream stream(&output);
        // write ttl
        WriteKvItemToStream(&stream, kCPCModelTtlKey, std::to_string(ttl_), 0, false,
                            max_timestamp_);
        // write dc_type
        WriteKvItemToStream(&stream, kCPCModelTypeKey, std::to_string(dc_type_), 0, false,
                            max_timestamp_);
        // write need_upgrade
        WriteKvItemToStream(&stream, kCPCModelDontUpgradeCPC, dont_upgrade_cpc_ ? "1" : "0", 0,
                            false, max_timestamp_);
        // ldc type 需要 write data
        if (dc_type_ == kCPCModelTypeLDC) {
            for (const auto& item : data_) {
                std::string item_data;
                google::protobuf::io::StringOutputStream item_output(&item_data);
                google::protobuf::io::CodedOutputStream item_stream(&item_output);
                item.second.second.serialize(item_stream);
                WriteKvItemToStream(&stream, item.first, std::move(item_data),
                                    item.second.first.cluster_id, item.second.first.deleted,
                                    item.second.first.timestamp);
            }
        }
        // 写入分隔符
        WriteKvItemToStream(&stream, kCPCModelDumpPartEnd, "", 0, false, max_timestamp_);
        // 写入 dc data
        for (const auto& item : dc_data_) {
            WriteKvItemToStream(&stream, item.first, serializeHashSet(item.second.second),
                                item.second.first.cluster_id, item.second.first.deleted,
                                item.second.first.timestamp);
        }
        // 写入分隔符
        WriteKvItemToStream(&stream, kCPCModelDumpPartEnd, "", 0, false, max_timestamp_);
        // 写入 uuid
        for (const auto& item : data_uuid_) {
            WriteKvItemToStream(&stream, item.first, std::to_string(item.second.second),
                                item.second.first.cluster_id, item.second.first.deleted,
                                item.second.first.timestamp);
        }
        stream.Trim();
        new_pages.emplace_back(0, std::move(page));
        return new_pages;
    }

    Status Apply(Allocator* allocator, std::string key_data, std::string value_data,
                 uint64_t cluster_id, uint64_t timestamp, bool deleted) {
        // 通过 key 区分是什么类型的数据
        if (key_data == kCPCModelTtlKey) {
            ttl_ = std::max(ttl_, (int32_t)std::strtoll(value_data.c_str(), nullptr, 10));
        } else if (UNLIKELY(key_data == kCPCModelTypeKey)) {
            int newType = std::strtoll(value_data.c_str(), nullptr, 10);
            // 只能从 dc 变成 ldc
            if (newType != kCPCModelTypeLDC) {
                return Status::Internal("cpc model new type key must be ldc type, but get: " +
                                        value_data);
            }
            dc_type_ = newType;
        } else if (UNLIKELY(key_data == kCPCModelDontUpgradeCPC)) {
            if (value_data == "0") {
                dont_upgrade_cpc_ = false;
            } else {
                // 如果已经是 cpc 模式了, 无法禁止升级
                if (dc_type_ == kCPCModelTypeLDC) {
                    return Status::Internal(
                        "cpc model is already ldc type, can not downgrade. value=" + value_data);
                }
                dont_upgrade_cpc_ = true;
            }
        } else if (key_data.size() > (size_t)kCPCModelFieldDcPrefixSize &&
                   key_data.substr(0, kCPCModelFieldDcPrefixSize) == kCPCModelDCKeyPrefix) {
            if (UNLIKELY(key_data.size() <= (size_t)kCPCModelOplogDCValueStart)) {
                return Status::Internal("cpc model dc_log with error key, key=" + key_data);
            }
            // dc 的 key
            auto dc_key = key_data.substr(kCPCModelFieldDcPrefixSize, kCPCModelFieldKeySize);
            auto dc_value = key_data.substr(kCPCModelOplogDCValueStart);
            auto iter = dc_data_.find(dc_key);
            if (iter == dc_data_.end()) {
                if (!deleted) {
                    Property property;
                    property.page_id = 0;
                    property.cluster_id = cluster_id;
                    property.deleted = deleted;
                    property.timestamp = timestamp;
                    absl::flat_hash_set<std::string> dcSet;
                    dcSet.emplace(dc_value);
                    dc_list_total_count_ += 1;
                    dc_list_total_mem_size_ += dc_value.length();
                    dc_data_[dc_key] = std::make_pair(property, std::move(dcSet));
                }
            } else {
                if (!deleted) {
                    // 此处写入 dc list 不做长度限制拦截
                    bool isEmplaced = iter->second.second.emplace(dc_value).second;
                    dc_list_total_count_ += isEmplaced ? 1 : 0;
                    dc_list_total_mem_size_ += isEmplaced ? dc_value.length() : 0;
                } else {
                    size_t isErased = iter->second.second.erase(dc_value);
                    dc_list_total_count_ -= isErased ? 1 : 0;
                    dc_list_total_mem_size_ -= isErased ? dc_value.length() : 0;
                }
                iter->second.first.timestamp = std::max(iter->second.first.timestamp, timestamp);
            }
            // 将 dc_data 中的内容更新进 cpc 中(cpc 模式)
            if (!deleted && dc_type_ == kCPCModelTypeLDC) {
                auto cpc_iter = data_.find(dc_key);
                if (LIKELY(cpc_iter != data_.end())) {
                    cpc_iter->second.second.update(dc_value);
                } else {
                    datasketches::cpc_sketch cpc_model(kCPCModelLgK);
                    cpc_model.update(dc_value);
                    Property property;
                    property.page_id = 0;
                    property.cluster_id = cluster_id;
                    property.deleted = deleted;
                    property.timestamp = timestamp;
                    data_[dc_key] = std::make_pair(property, std::move(cpc_model));
                }
            }
        } else if (key_data.size() > (size_t)kCPCModelUUIDKeyPrefixSize &&
                   key_data.substr(0, kCPCModelUUIDKeyPrefixSize) == kCPCModelUUIDKeyPrefix) {
            // uuid 的 key
            auto uuid = key_data.substr(kCPCModelUUIDKeyPrefixSize);
            if (deleted) {
                data_uuid_.erase(uuid);
            } else {
                int64_t uuidTime = std::strtoll(value_data.c_str(), nullptr, 10);
                if (uuidTime <=  UINT32_MAX) {
                    Property property;
                    property.page_id = 0;
                    property.cluster_id = cluster_id;
                    property.deleted = deleted;
                    property.timestamp = timestamp;
                    data_uuid_[uuid] = std::make_pair(property, uuidTime);
                }
            }
        } else {
            // ldc 的 key
            if (deleted) {
                data_.erase(key_data);
            } else {
                auto iter = data_.find(key_data);
                if (iter == data_.end() || iter->second.first.timestamp < timestamp) {
                    datasketches::cpc_sketch deserializer;
                    Property property;
                    property.page_id = 0;
                    property.cluster_id = cluster_id;
                    property.deleted = deleted;
                    property.timestamp = timestamp;
                    google::protobuf::io::ArrayInputStream input(value_data.data(),
                                                                 value_data.size());
                    google::protobuf::io::CodedInputStream stream(&input);
                    datasketches::cpc_sketch value = deserializer.deserialize(stream);
                    data_[key_data] = std::make_pair(property, std::move(value));
                }
                // 将 dc_data 中的内容更新进 cpc 中
                auto dc_iter = dc_data_.find(key_data);
                if (dc_iter != dc_data_.end()) {
                    iter = data_.find(key_data);
                    if (LIKELY(iter != data_.end())) {
                        for (const std::string& v : dc_iter->second.second) {
                            iter->second.second.update(v);
                        }
                    }
                }
            }
        }

        // 需要保证 _max_timestamp 最新, 用于保证写入的 oplog 时间戳严格增序
        max_timestamp_ = std::max(max_timestamp_, timestamp);
        return Status::OK();
    }

    // not support multi-page for cpc model yet
    static std::pair<std::vector<partition::PageIndex>, bool> CompactPagesHint(
        std::vector<partition::PageIndex> pages) {
        return {};
    }

    // not support multi-page for cpc model yet
    static std::vector<partition::PageInfo> CompactPages(
        const std::vector<partition::PageInfo>& pages, bool remove_tombstone) {
        return pages;
    }

    Status Merge(Allocator* allocator, std::string key_data, std::string value_data,
                 uint64_t cluster_id, uint64_t timestamp, bool deleted) {
        // 暂时不需要实现, 预留接口, 提供给美国、欧洲等跨区域同步时的 CRDT 使用
        BYTE_ASSERT(false);
        return Status::Internal("not supported merge method");
    }

    int64_t MemSize() {
        // field key size = 12
        // Property size = 12
        // 不计算容器内部消耗, 按照 pointer(8) * size * 2 估算
        // dc_list_total_mem_size_ 为 dc_data_ 所有 value 的内存消耗
        // 总消耗为 pointer + data_ size + dc_data_ size + const
        int64_t cpc_total_mem = 0;
        for (auto subCpc : data_) {
            cpc_total_mem += subCpc.second.second.mem_size();
        }
        return (data_.size() + dc_data_.size()) * (8 * 2 + 12) + dc_list_total_mem_size_ +
               cpc_total_mem + 64;
    }

 private:
    void writeOpLog(partition::CmdContext* ctx, const std::string& key, const std::string& value,
                    const Property& property) {
        if (ctx == nullptr) {
            ModelContext* ctx = GetModelContext();
            ctx->op_logger->WriteKvLog(ctx->cmd_context, ctx->slot_id, ctx->object->ObjectId(),
                                       ctx->object->Key(), ctx->object->ModelId(), key, value,
                                       property);
        } else {
            ctx->op_logger->WriteKvLog(ctx, ctx->slot_id, ctx->object.ObjectId(), ctx->object.Key(),
                                       ctx->object.ModelId(), key, value, property);
        }
    }

    Status doExpire(partition::CmdContext* ctx, std::string startPrefix, std::string endPrefix,
                    int64_t limit) {
        // 清理 cpc 的 key
        auto startIter = data_.lower_bound(startPrefix);
        auto endIter = data_.lower_bound(endPrefix);
        std::vector<std::string> expiredKey;
        for (int64_t count = 0; startIter != endIter && (limit <= 0 || count < limit);
             ++startIter, ++count) {
            expiredKey.emplace_back(startIter->first);
        }
        for (const std::string& key : expiredKey) {
            del(ctx, key);
        }
        // 清理 dc 的 key
        auto dcStartIter = dc_data_.lower_bound(startPrefix);
        auto dcEndIter = dc_data_.lower_bound(endPrefix);
        int64_t count = 0;
        std::vector<std::string> dcExpiredKey;
        for (; dcStartIter != dcEndIter; ++dcStartIter) {
            std::vector<std::string> needDel;
            for (auto iter = dcStartIter->second.second.begin();
                 iter != dcStartIter->second.second.end(); ++iter) {
                needDel.emplace_back(*iter);
                ++count;
                if (UNLIKELY(limit > 0 && count >= limit)) {
                    break;
                }
            }
            delDcSet(ctx, dcStartIter->first, needDel);
            for (const std::string& val : needDel) {
                dcStartIter->second.second.erase(val);
            }
            if (dcStartIter->second.second.size() == 0) {
                dcExpiredKey.emplace_back(dcStartIter->first);
            }

            if (UNLIKELY(limit > 0 && count >= limit)) {
                break;
            }
        }
        for (const std::string& key : dcExpiredKey) {
            dc_data_.erase(key);
        }
        return Status::OK();
    }

    void uuidGc(partition::CmdContext* ctx, uint32_t now) {
        std::vector<std::string> expire_keys;
        uint32_t expiredUUIDTime = now - kCPCModelUUIDExpiredTime;
        for (auto iter = data_uuid_.begin(); iter != data_uuid_.end(); ++iter) {
            if (iter->second.second < expiredUUIDTime) {
                expire_keys.emplace_back(iter->first);
            }
        }
        for (const auto& k : expire_keys) {
            auto iter = data_uuid_.find(k);
            if (iter == data_uuid_.end()) {
                return;
            }
            iter->second.first.deleted = true;
            writeOpLog(ctx, kCPCModelUUIDKeyPrefix + iter->first,
                    std::to_string(iter->second.second), iter->second.first);
            data_uuid_.erase(k);
        }
    }

    void del(partition::CmdContext* ctx, const std::string& key) {
        auto iter = data_.find(key);
        if (iter == data_.end()) {
            return;
        }
        iter->second.first.deleted = true;
        writeOpLog(ctx, key, "", iter->second.first);
        data_.erase(key);
        return;
    }

    void delDcSet(partition::CmdContext* ctx, const std::string& key,
                  const std::vector<std::string>& subKeyList) {
        auto iter = dc_data_.find(key);
        if (iter == dc_data_.end()) {
            return;
        }
        Property tmpProperty = iter->second.first;
        tmpProperty.deleted = true;
        std::string delKey = kCPCModelDCKeyPrefix + key;
        size_t isDeleted;
        for (const auto& subKey : subKeyList) {
            writeOpLog(ctx, delKey + subKey, "", tmpProperty);
            isDeleted = iter->second.second.erase(subKey);
            dc_list_total_count_ -= isDeleted ? 1 : 0;
            dc_list_total_mem_size_ -= isDeleted ? subKey.length() : 0;
        }
        return;
    }

    void delFullDcSet(partition::CmdContext* ctx, const std::string& key) {
        auto iter = dc_data_.find(key);
        if (iter == dc_data_.end()) {
            return;
        }
        dc_list_total_count_ -= iter->second.second.size();
        Property tmpProperty = iter->second.first;
        tmpProperty.deleted = true;
        std::string delKey = kCPCModelDCKeyPrefix + key;
        for (const std::string& subKey : iter->second.second) {
            writeOpLog(ctx, delKey + subKey, "", tmpProperty);
            dc_list_total_mem_size_ -= subKey.length();
        }
        dc_data_.erase(key);
        return;
    }

    inline bool checkNeedTransforToLDCType() {
        return dc_list_total_count_ >= kCPCModelTransforLDCSize;
    }
    void changeToCPC(partition::CmdContext* ctx) {
        dc_type_ = kCPCModelTypeLDC;
        Property property;
        property.page_id = 0;
        property.cluster_id = 0;
        property.deleted = false;
        property.timestamp = getAndSaveMaxTimestamp();
        std::vector<std::string> needDel;
        writeOpLog(ctx, kCPCModelTypeKey, std::to_string(dc_type_), property);
        for (auto iter : dc_data_) {
            std::string logValue;
            auto cpc_model = datasketches::cpc_sketch(kCPCModelLgK);
            property.timestamp = getAndSaveMaxTimestamp();
            for (const std::string& value : iter.second.second) {
                cpc_model.update(value);
            }
            google::protobuf::io::StringOutputStream item_output(&logValue);
            google::protobuf::io::CodedOutputStream item_stream(&item_output);
            cpc_model.serialize(item_stream);
            data_[iter.first] = std::make_pair(property, std::move(cpc_model));
            writeOpLog(ctx, iter.first, logValue, property);
            needDel.emplace_back(iter.first);
        }
        for (const std::string& delKey : needDel) {
            delFullDcSet(ctx, delKey);
        }
    }

    // 序列化 flat_hash_set
    std::string serializeHashSet(const absl::flat_hash_set<std::string>& hashSet) {
        std::string item_data;
        google::protobuf::io::StringOutputStream item_output(&item_data);
        google::protobuf::io::CodedOutputStream item_stream(&item_output);
        item_stream.WriteVarint32(hashSet.size());
        for (const auto& value : hashSet) {
            WriteData(&item_stream, value);
        }
        return item_data;
    }

    // 反序列化 flat_hash_set
    absl::flat_hash_set<std::string> deserializeHashSet(const std::string& valueData,
                                                        int64_t* memSize = nullptr) {
        google::protobuf::io::ArrayInputStream input(valueData.data(), valueData.size());
        google::protobuf::io::CodedInputStream stream(&input);
        absl::flat_hash_set<std::string> resSet;
        uint32_t resSize = 0;
        stream.ReadVarint32(&resSize);
        std::string item;
        int64_t tmpMemSize = 0;
        for (uint32_t i = 0; i < resSize; ++i) {
            if (static_cast<std::size_t>(stream.CurrentPosition()) >= valueData.size()) {
                LOG(ERROR) << "[risk_cpc_model]deserializeHashSet failed,"
                              " read data len overflow.";
                if (memSize != nullptr) {
                    (*memSize) = 0;
                }
                return absl::flat_hash_set<std::string>();
            }
            ReadData(&stream, &item);
            tmpMemSize += resSet.emplace(item).second ? item.length() : 0;
        }
        if (memSize != nullptr) {
            (*memSize) = tmpMemSize;
        }
        return resSet;
    }
    // 设置 ttl, 并且 ttl 需要写入 oplog
    void setTtl(partition::CmdContext* ctx, uint64_t ttl) {
        if (ttl_ < (int32_t)ttl) {
            Property property;
            property.page_id = 0;
            property.cluster_id = 0;
            property.deleted = false;
            property.timestamp = getAndSaveMaxTimestamp();
            ttl_ = ttl;
            writeOpLog(ctx, kCPCModelTtlKey, std::to_string(ttl), property);
        }
        return;
    }
    uint64_t getAndSaveMaxTimestamp() {
        return max_timestamp_ = std::max(GetCurrentTimeInNs(), max_timestamp_ + 1);
    }
    uint64_t getEndTimestamp(std::time_t now) {
        return now - ttl_;
    }
    bool insertUUID(partition::CmdContext* ctx, const std::string& uuid, uint32_t now) {
        if (uuid == "") return true;
        // 找到具体的uuid了
        auto iter = data_uuid_.find(uuid);
        if (iter != data_uuid_.end()) {
            // 找到的uuid已经过期认为不存在
            if (iter->second.second > now - kCPCModelUUIDExpiredTime) {
                return false;
            }
        }
        // 没找到或者已经过期则进行插入
        Property property;
        property.page_id = 0;
        property.cluster_id = 0;
        property.deleted = 0;
        property.timestamp = getAndSaveMaxTimestamp();
        writeOpLog(ctx, kCPCModelUUIDKeyPrefix + uuid, std::to_string(now), property);
        data_uuid_[uuid] = std::make_pair(property, now);
        return true;
    }

 private:
    bool dont_upgrade_cpc_ = false;
    int8_t dc_type_ = kCPCModelTypeDC;
    int16_t trigger_gc_count_ = 0;
    int16_t cpc_transfor_check_cnt_ = 0;
    uint32_t last_gc_timestamp_ = 0;
    int32_t ttl_ = 0;
    int32_t dc_list_total_count_ = 0;
    int32_t dc_list_total_mem_size_ = 0;
    uint64_t max_timestamp_ = 0;
    BtreeMap<std::string, std::pair<Property, datasketches::cpc_sketch>> data_;
    BtreeMap<std::string, std::pair<Property, absl::flat_hash_set<std::string>>> dc_data_;
    BtreeMap<std::string, std::pair<Property, uint32_t>> data_uuid_;

    // For gtest
    FRIEND_TEST(RISKCPCModelTest, LowModel);
};

}  // namespace model
}  // namespace bcache2
