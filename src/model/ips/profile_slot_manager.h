// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <rapidjson/document.h>
#include <rapidjson/filereadstream.h>

// #include <butil/third_party/rapidjson/document.h>
// #include <butil/third_party/rapidjson/filereadstream.h>

#include <map>
#include <memory>
#include <unordered_map>
#include <vector>

// #include "bcache/common/rw_lock.h"
// #include "bcache/common/status.h"
// #include "bcache/server/ips_interface/ips_define.h"

#include "common/status.h"
#include "model/ips/ips_define.h"
#include "model/ips/rw_lock.h"

namespace bcache2 {
namespace ips {
class SlotManager {
 public:
    SlotManager() {}
    ~SlotManager();

    void Init(const rapidjson::Value& val);

    void GetSlotList(std::vector<SlotID>* slot_list) const;

    std::unordered_map<SlotID, int64_t> GetSlotConf() {
        // std::shared_lock<RWLock> guard(rw_lock_);
        rw_lock_.lock_shared();
        std::unordered_map<SlotID, int64_t> ret_slot_map_ = slot_map_;
        rw_lock_.unlock_shared();
        return ret_slot_map_;
    }

    Status GetSlotCntLimit(SlotID slot, int64_t* cnt) const;

    void SetSlotFidLimit(const SlotID slot, int64_t max_cnt);

    void InsertSlotFidLimit(const std::unordered_map<SlotID, int64_t>& slots_conf);

    void DelSlotFidLimit(const std::vector<SlotID>& slots_vec);

    void UpdateSlotMapConf(SlotID slot, int64_t fid_num);

    Status UpdateSlotIndexAndWeightConf(SlotID slot, const rapidjson::Value& slot_conf);

    void GetSlotIndexAndWeightConf(const SlotID slot, std::vector<uint64_t>* feature_index,
                                   std::vector<uint64_t>* feature_weight) const;

    void GetSlotJsonConf(rapidjson::Value* slot_conf,
                         rapidjson::Document::AllocatorType* allocator) const;

    void DeleteSlotIndexAndWeightConf(const SlotID slot);

 private:
    bool ParseArrayJsonConf(const rapidjson::Value& val, std::vector<uint64_t>* parse_res) const;
    // 内部方法，非线程安全，调用前需要先获取锁
    const std::vector<uint64_t>* GetSlotIndexConf(SlotID slot) const;
    const std::vector<uint64_t>* GetSlotWeightConf(SlotID slot) const;
    bool HasSlotIndexOrWeightConfChanged(SlotID slot, const std::vector<uint64_t>& feature_index,
                                         const std::vector<uint64_t>& feature_weight) const;
    void UpdateSlotIndexAndWeightConf(const SlotID slot, const std::vector<uint64_t>& feature_index,
                                      const std::vector<uint64_t>& feature_weight);

 private:
    mutable RWLock rw_lock_;
    std::unordered_map<SlotID, int64_t> slot_map_;  // protected by rw_lock_

    std::unordered_map<SlotID, std::unique_ptr<std::vector<uint64_t>>> slot_feature_index_;
    std::unordered_map<SlotID, std::unique_ptr<std::vector<uint64_t>>> slot_feature_weight_;
};

}  // namespace ips
}  // namespace bcache2
