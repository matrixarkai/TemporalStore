// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "model/ips/profile_slot_manager.h"

#include <absl/memory/memory.h>

#include "model/ips/utils.h"

namespace bcache2 {
namespace ips {

SlotManager::~SlotManager() = default;

void SlotManager::Init(const rapidjson::Value& val) {
    for (rapidjson::Value::ConstMemberIterator it = val.MemberBegin(); it != val.MemberEnd();
         ++it) {
        SlotID slot = std::stoi(it->name.GetString());

        if (it->value.IsInt()) {  // "SlotId: fid_num"格式的配置解析
            UpdateSlotMapConf(slot, it->value.GetInt());
        } else {  // 自定义slot shrink的排序配置解析
            UpdateSlotIndexAndWeightConf(slot, it->value);
        }
    }
}

void SlotManager::GetSlotList(std::vector<SlotID>* slot_list) const {
    RW_shared_lock guard(rw_lock_);

    slot_list->reserve(slot_map_.size());
    for (const auto iter : slot_map_) {
        slot_list->emplace_back(iter.first);
    }
}

Status SlotManager::GetSlotCntLimit(SlotID slot, int64_t* cnt) const {
    RW_shared_lock guard(rw_lock_);
    auto iter = slot_map_.find(slot);
    if (iter == slot_map_.end()) {
        return Status::NotFound("NotFound");
    } else {
        *cnt = iter->second;
    }
    return Status::OK();
}

void SlotManager::SetSlotFidLimit(const SlotID slot, int64_t max_cnt) {
    std::unique_lock<RWLock> guard(rw_lock_);
    slot_map_[slot] = max_cnt;
}

void SlotManager::InsertSlotFidLimit(const std::unordered_map<SlotID, int64_t>& slots_conf) {
    std::unique_lock<RWLock> guard(rw_lock_);
    for (auto const& cur_slot_conf : slots_conf) {
        slot_map_[cur_slot_conf.first] = cur_slot_conf.second;
    }
}

void SlotManager::DelSlotFidLimit(const std::vector<SlotID>& slots_vec) {
    std::unique_lock<RWLock> guard(rw_lock_);
    for (SlotID slot : slots_vec) {
        slot_map_.erase(slot);
    }
}

bool SlotManager::ParseArrayJsonConf(const rapidjson::Value& feature_conf,
                                     std::vector<uint64_t>* parse_res) const {
    if (!feature_conf.IsArray()) {
        return false;
    }

    parse_res->reserve(feature_conf.Size());
    for (rapidjson::SizeType i = 0; i < feature_conf.Size(); ++i) {
        if (feature_conf[i].IsUint()) {
            parse_res->emplace_back(feature_conf[i].GetUint());
        } else {
            return false;
        }
    }
    return true;
}

void SlotManager::UpdateSlotMapConf(SlotID slot, int64_t cur_fid_limit) {
    int64_t old_fid_limit;
    if (!GetSlotCntLimit(slot, &old_fid_limit).ok() || cur_fid_limit != old_fid_limit) {
        SetSlotFidLimit(slot, cur_fid_limit);
    }
}

// 自定义slot shrink排序配置格式解析: 更新fid_num、feature_index和feature_weight配置
Status SlotManager::UpdateSlotIndexAndWeightConf(SlotID slot, const rapidjson::Value& slot_conf) {
    if (!slot_conf.HasMember("feature_index") || !slot_conf.HasMember("feature_weight") ||
        !slot_conf.HasMember("max_fid_num") || !slot_conf["max_fid_num"].IsInt()) {
        return Status::InvalidArgument("InvalidArgument");
    }
    // 更新当前slot的最大保留的fid数量配置
    UpdateSlotMapConf(slot, slot_conf["max_fid_num"].GetInt());

    // 解析feature_index
    const rapidjson::Value& feature_index_conf = slot_conf["feature_index"];
    std::vector<uint64_t> feature_index;
    if (!ParseArrayJsonConf(feature_index_conf, &feature_index) || feature_index.size() == 0) {
        return Status::InvalidArgument(
            fmt::format("parse feature_index_conf error, slotId: {}", slot));
    }

    // 解析feature_weight
    const rapidjson::Value& feature_weight_conf = slot_conf["feature_weight"];
    std::vector<uint64_t> feature_weight;
    if (!ParseArrayJsonConf(feature_weight_conf, &feature_weight) ||
        feature_index.size() != feature_weight.size()) {
        // BC_ERROR_DEFAULT_RATE_LIMIT(
        //     "parse feature_weight_conf error or feature_index's size Not equal feature_weight's
        //     size, " "slotId: {}", slot);
        return Status::InvalidArgument("InvalidArgument");
    }
    if (!HasSlotIndexOrWeightConfChanged(slot, feature_index, feature_weight)) {
        // slot的配置数据没有发生变更时，直接返回
        return Status::OK();
    }
    UpdateSlotIndexAndWeightConf(slot, feature_index, feature_weight);
    return Status::OK();
}

void SlotManager::GetSlotIndexAndWeightConf(const SlotID slot, std::vector<uint64_t>* feature_index,
                                            std::vector<uint64_t>* feature_weight) const {
    RW_shared_lock guard(rw_lock_);
    if (slot_feature_index_.find(slot) == slot_feature_index_.end() ||
        slot_feature_weight_.find(slot) == slot_feature_weight_.end()) {
        return;
    }

    const std::vector<uint64_t>& slot_index_conf = *slot_feature_index_.at(slot);
    feature_index->reserve(slot_index_conf.size());
    for (auto index : slot_index_conf) {
        feature_index->emplace_back(index);
    }

    const std::vector<uint64_t>& slot_weight_conf = *slot_feature_weight_.at(slot);
    feature_weight->reserve(slot_weight_conf.size());
    for (auto weight : slot_weight_conf) {
        feature_weight->emplace_back(weight);
    }
}

const std::vector<uint64_t>* SlotManager::GetSlotIndexConf(SlotID slot) const {
    if (slot_feature_index_.find(slot) == slot_feature_index_.end()) {
        return nullptr;
    }
    return slot_feature_index_.at(slot).get();
}

const std::vector<uint64_t>* SlotManager::GetSlotWeightConf(SlotID slot) const {
    if (slot_feature_weight_.find(slot) == slot_feature_weight_.end()) {
        return nullptr;
    }
    return slot_feature_weight_.at(slot).get();
}

void SlotManager::GetSlotJsonConf(rapidjson::Value* slot_conf,
                                  rapidjson::Document::AllocatorType* allocator) const {
    RW_shared_lock guard(rw_lock_);

    if (slot_map_.empty()) {
        slot_conf->AddMember("0", "10000", *allocator);
    } else {
        for (auto iter = slot_map_.begin(); iter != slot_map_.end(); ++iter) {
            SlotID slot = iter->first;
            const std::vector<uint64_t>* feature_indexes = GetSlotIndexConf(slot);
            const std::vector<uint64_t>* feature_weights = GetSlotWeightConf(slot);

            rapidjson::Value solt_json_key(std::to_string(slot).c_str(),
                                           std::to_string(slot).size(), *allocator);
            if (feature_indexes != nullptr && feature_weights != nullptr) {
                rapidjson::Value slot_obj(rapidjson::kObjectType);
                slot_obj.SetObject();
                rapidjson::Value feature_indexes_conf(rapidjson::kArrayType);
                for (auto index : *(feature_indexes)) {
                    feature_indexes_conf.PushBack(index, *allocator);
                }
                slot_obj.AddMember("feature_indexes", feature_indexes_conf, *allocator);

                rapidjson::Value feature_weight_conf(rapidjson::kArrayType);
                for (auto weight : *(feature_weights)) {
                    feature_weight_conf.PushBack(weight, *allocator);
                }
                slot_obj.AddMember("feature_weights", feature_weight_conf, *allocator);

                slot_obj.AddMember("max_fid_num", iter->second, *allocator);

                slot_conf->AddMember(solt_json_key, slot_obj, *allocator);
            } else {
                slot_conf->AddMember(solt_json_key, iter->second, *allocator);
            }
        }
    }
}

// 移除当前slot的自定义shrink排序相关的配置
void SlotManager::DeleteSlotIndexAndWeightConf(const SlotID slot) {
    {
        RW_shared_lock guard(rw_lock_);
        if (slot_feature_index_.find(slot) == slot_feature_index_.end() &&
            slot_feature_weight_.find(slot) == slot_feature_weight_.end()) {
            return;
        }
    }

    {
        std::unique_lock<RWLock> guard(rw_lock_);
        if (slot_feature_index_.find(slot) != slot_feature_index_.end()) {
            slot_feature_index_.erase(slot);
        }

        if (slot_feature_weight_.find(slot) != slot_feature_weight_.end()) {
            slot_feature_weight_.erase(slot);
        }
    }
}

bool SlotManager::HasSlotIndexOrWeightConfChanged(
    SlotID slot, const std::vector<uint64_t>& feature_index,
    const std::vector<uint64_t>& feature_weight) const {
    RW_shared_lock guard(rw_lock_);
    // 检查当前slot的配置是否发生变化
    if (slot_feature_index_.find(slot) != slot_feature_index_.end() &&
        slot_feature_weight_.find(slot) != slot_feature_weight_.end()) {
        // 当前slot的配置之前就存在, 则检查配置值是否发生变化
        if (*(slot_feature_index_.at(slot)) != feature_index ||
            *(slot_feature_weight_.at(slot)) != feature_weight) {
            return true;
        }
    } else {
        // 新增了slot配置
        return true;
    }
    return false;
}

void SlotManager::UpdateSlotIndexAndWeightConf(const SlotID slot,
                                               const std::vector<uint64_t>& feature_index,
                                               const std::vector<uint64_t>& feature_weight) {
    // 更新shrink排序权重相关的配置
    std::unique_lock<RWLock> guard(rw_lock_);
    // 重新初始化当前slot的feature_index配置
    if (slot_feature_index_.find(slot) == slot_feature_index_.end()) {
        slot_feature_index_[slot] = absl::make_unique<std::vector<uint64_t>>();
    } else {
        slot_feature_index_[slot]->clear();
    }
    slot_feature_index_[slot]->reserve(feature_index.size());

    // 重新初始化当前slot的feature_weight配置
    if (slot_feature_weight_.find(slot) == slot_feature_weight_.end()) {
        slot_feature_weight_[slot] = absl::make_unique<std::vector<uint64_t>>();
    } else {
        slot_feature_weight_[slot]->clear();
    }
    slot_feature_weight_[slot]->reserve(feature_weight.size());

    // 更新当前slot的配置
    for (size_t i = 0; i < feature_index.size(); ++i) {
        slot_feature_index_[slot]->emplace_back(feature_index[i]);
        slot_feature_weight_[slot]->emplace_back(feature_weight[i]);
    }
}

}  // namespace ips
}  // namespace bcache2
