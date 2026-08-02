// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/storage/evicter.h"

#include <byte/algorithm/crc64.h>
#include <byte/include/assert.h>

#include <algorithm>
#include <functional>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/coclosure.h"
#include "common/logging.h"
#include "common/metrics.h"
#include "common/slot.h"
#include "partition/allocator_manager.h"
#include "partition/index/index.h"
#include "partition/metrics.h"
#include "partition/partition.h"
#include "partition/storage/object.h"
#include "partition/storage/object_manager.h"
#include "partition/storage/op_logger.h"
#include "partition/storage/slot_store.h"

DECLARE_uint64(evicter_max_memory_usage);
DECLARE_uint64(evict_count_limit);
DECLARE_uint64(evict_batch_size);
DECLARE_uint64(limit_evict_scan_turns);

namespace bcache2 {
namespace partition {

class Evicter::Actuator {  // defines what action to take after objects to evict are chosen
 public:
    virtual ~Actuator() {}
    virtual Status Evict(const std::vector<Object>& objects) = 0;
    virtual OperationType::OperationTypeEnum GetType() = 0;
};

// for cache-only clusters, i.e., no persistence is required
class Evicter::DelActuator : public Evicter::Actuator {
 public:
    explicit DelActuator(Evicter* evicter) : evicter_(evicter) {}
    ~DelActuator() {}

    Status Evict(const std::vector<Object>& objects) override;
    OperationType::OperationTypeEnum GetType() override { return OperationType::DELETE; }

 private:
    Evicter* evicter_;
};

Status Evicter::DelActuator::Evict(const std::vector<Object>& objects) {
    BYTE_ASSERT(!objects.empty());
    for (const Object& object : objects) {
        uint64_t slot_id = hash_func(object.Key().data(), object.Key().size());
        SlotNode* slot = evicter_->index_->GetSlot(slot_id);
        if (!slot->InMemory()) {
            continue;
        }
        LOG_DEBUG("Evicter try del object")
            .put("PartitionId", evicter_->partition_->GetPartitionID())
            .put("SlotId", slot_id)
            .put("Key", object.Key());
        Status status = evicter_->object_manager_->DeleteObject(
            slot_id, object.Key());  // Index will be updated inside this deletion
        if (!status.ok()) {
            return status;
        }
    }
    return Status::OK();
}

class Evicter::DumpActuator : public Evicter::Actuator {
 public:
    explicit DumpActuator(Evicter* evicter) : evicter_(evicter) {}
    ~DumpActuator() {}

    Status Evict(const std::vector<Object>& objects) override;
    OperationType::OperationTypeEnum GetType() override { return OperationType::DUMP; }

 private:
    Evicter* evicter_;
};

Status Evicter::DumpActuator::Evict(const std::vector<Object>& objects) {
    BYTE_ASSERT(!objects.empty());
    std::vector<uint64_t> dump_slots;
    dump_slots.reserve(objects.size());
    for (const Object& object : objects) {
        // the unit to dump is slots
        uint64_t slot_id = hash_func(object.Key().data(), object.Key().size());
        SlotNode* slot = evicter_->index_->GetSlot(slot_id);
        if (!slot->InMemory()) {
            continue;
        }
        LOG_DEBUG("Evicter try dump slot")
            .put("PartitionId", evicter_->partition_->GetPartitionID())
            .put("slot_id", slot_id)
            .put("key", object.Key());
        BYTE_ASSERT(slot->GetObjectNum() >= 1);
        dump_slots.push_back(slot_id);
    }

    if (dump_slots.empty()) {
        // no need dump
        return Status::OK();
    }

    Status ret_status;
    for (uint64_t slot_id : dump_slots) {
        Status status = evicter_->index_->EvictSlot(slot_id);  // update Index manually here
        if (!status.ok()) {
            ret_status = status;
        }
    }
    return ret_status;
}

std::vector<Object> Evicter::PolicyLru::GetBestObjects(int batch_size) {
    LOG_DEBUG("Lru policy try get best object")
        .put("PartitionId", evicter_->partition_->GetPartitionID());
    int scanned_slots_num = 0;
    int min_scan_slots_num = evicter_->config_.samples().value() * batch_size;
    // this function is used to determine when to stop scanning
    auto func = [&scanned_slots_num, &min_scan_slots_num, &batch_size, this](
                    uint64_t slot_id, SlotNode* slot) -> bool {
        scanned_slots_num++;
        if (!slot->InMemory() || slot->GetObjectNum() == 0) {
            return true;  // keep scanning
        }
        cached_entry_[slot_id] = slot->GetLastUsed();
        LOG_DEBUG("Build lru")
            .put("PartitionId", evicter_->partition_->GetPartitionID())
            .put("slot_id", slot_id)
            .put("access_record", slot->GetLastUsed())
            .put("object_count", slot->GetObjectNum());
        return scanned_slots_num < min_scan_slots_num ||
               cached_entry_.size() < static_cast<size_t>(batch_size);
    };

    if (!iterator_->Scan(
            evicter_->config_.samples().value() * batch_size * FLAGS_limit_evict_scan_turns,
            func)) {
        iterator_ = evicter_->index_->NewSlotIterator();
    }

    LOG_DEBUG("objects to evict")
        .put("PartitionId", evicter_->partition_->GetPartitionID())
        .put("ActualEvictNum", cached_entry_.size())
        .put("ExpectedEvictedNum", batch_size);

    std::multimap<uint64_t, uint64_t, std::greater<uint64_t>> sort_entry;
    for (auto it = cached_entry_.begin(); it != cached_entry_.end();) {
        SlotNode* slot = evicter_->index_->GetSlot(it->first);
        if (slot == nullptr || !slot->InMemory() || slot->GetObjectNum() < 1) {
            it = cached_entry_.erase(it);
            continue;
        }
        LOG_DEBUG("Lru cache entry")
            .put("PartitionId", evicter_->partition_->GetPartitionID())
            .put("slot_id", it->first)
            .put("in_memory", slot->InMemory())
            .put("object_count", slot->GetObjectNum());
        cached_entry_[it->first] = slot->GetLastUsed();  // why update it again here? Because some
                                                         // cached entries are from previous rounds
        sort_entry.emplace(it->second, it->first);
        ++it;
    }

    // get the least recently used slots (and then one object from each slot)
    std::vector<Object> best_objects;
    best_objects.reserve(batch_size);
    for (auto it = sort_entry.rbegin(); batch_size > 0 && it != sort_entry.rend(); batch_size--) {
        SlotNode* slot = evicter_->index_->GetSlot(it->second);
        auto best_obj = slot->GetObjects().front();
        best_objects.push_back(best_obj);
        it++;
    }

    // remove the most recently used slots from cached_entry_,
    // and the remaining slots in cached_entry will be checked next time
    for (auto it = sort_entry.begin(); it != sort_entry.end(); ++it) {
        if (cached_entry_.size() <= evicter_->config_.pool_size().value()) {
            break;
        }
        cached_entry_.erase(it->second);
    }

    // normally, cached_entry_ will not greater than pool_size
    while (UNLIKELY(cached_entry_.size() > evicter_->config_.pool_size().value())) {
        cached_entry_.erase(cached_entry_.begin());
    }

    return best_objects;
}

Evicter::Evicter(Partition* partition, ObjectManager* object_manager, Index* index,
                 OpLogger* op_logger, SlotStore* slot_store, AllocatorManager* allocator_manager,
                 MetricsManager* metrics_manager)
    : partition_(partition),
      object_manager_(object_manager),
      index_(index),
      op_logger_(op_logger),
      slot_store_(slot_store),
      allocator_manager_(allocator_manager) {
    metrics_.Init(metrics_manager);
}

Evicter::~Evicter() {}

Status Evicter::Init(EvicterConfig config) {
    config_.mutable_maxmemory()->set_value(0);
    config_.mutable_pool_size()->set_value(16);
    config_.mutable_samples()->set_value(16);
    config_.mutable_policy_type()->set_value(PolicyType::LRU);
    config_.mutable_dataset_type()->set_value(DatasetType::ALL_KEYS);
    config_.mutable_operation_type()->set_value(OperationType::DUMP);
    config_.MergeFrom(config);
    CreateOrUpdatePolicy();
    return Status::OK();
}

void Evicter::CreateOrUpdatePolicy() {
    if (policy_ == nullptr || policy_->GetType() != config_.policy_type().value()) {
        if (config_.policy_type().value() == PolicyType::LRU) {
            index_->UpdateAccessRecordType(Index::AccessRecordType::kLru);
            policy_.reset(new PolicyLru(this));
        }
    }
    if (actuator_ == nullptr || actuator_->GetType() != config_.operation_type().value()) {
        actuator_.reset(new DumpActuator(this));
        /*
        if (config_.operation_type().value() == EvicterConfig::OperationType::DELETE) {
            actuator_.reset(new DelActuator(this));
        } else {
            actuator_.reset(new DumpActuator(this));
        }
        */
    }
}

Status Evicter::TryEvict() {
    LOG_CALL_DEBUG().put("PartitionId", partition_->GetPartitionID());
    // no memory limit, no need to evict
    if (config_.maxmemory().value() <= 0 && FLAGS_evicter_max_memory_usage <= 0) {
        return Status::OK();
    }

    size_t used = allocator_manager_->GetTotalAllocedSize();
    LOG_DEBUG("Evicter try evict used bytes")
        .put("PartitionId", partition_->GetPartitionID())
        .put("UsedBytes", used)
        .put("MaxMemoryConfig", config_.maxmemory().value())
        .put("MaxMemoryUsageFlag", FLAGS_evicter_max_memory_usage);
    LOG_INFO_SAMPLE("Evicter try evict used bytes")
        .put("PartitionId", partition_->GetPartitionID())
        .put("UsedBytes", used)
        .put("MaxMemoryConfig", config_.maxmemory().value())
        .put("MaxMemoryUsageFlag", FLAGS_evicter_max_memory_usage);

    uint64_t limit = FLAGS_evict_count_limit;
    while ((config_.maxmemory().value() != 0 && used > config_.maxmemory().value()) ||
           (FLAGS_evicter_max_memory_usage != 0 && used > (FLAGS_evicter_max_memory_usage << 20))) {
        TimeCost cost;

        // TODO(zhupengxiang.peter): bug?
        if (config_.maxmemory().value() == PolicyType::NO_EVICTION) {
            metrics_.evict_oom->get()->Increment();
            LOG_WARNING("Evicter policy type no eviction oom!")
                .put("PartitionId", partition_->GetPartitionID());
            return Status::Internal("oom");
        }
        // TODO(tangyunmeng.hinata): Optimization strategy
        if (limit < 1) {
            LOG_WARNING("Evicter evict speed limit")
                .put("PartitionId", partition_->GetPartitionID())
                .put("used", used)
                .put("maxmemory", config_.maxmemory().value());
            break;
        }
        uint64_t real_batch_size = std::min(limit, FLAGS_evict_batch_size);
        limit -= real_batch_size;
        CreateOrUpdatePolicy();
        std::vector<Object> objects = policy_->GetBestObjects(real_batch_size);
        if (objects.empty()) {
            LOG_INFO("No object to evict")
                .put("PartitionId", partition_->GetPartitionID())
                .put("Used", used);
            return Status::OK();
        }
        Status status = actuator_->Evict(objects);
        if (!status.ok()) {
            metrics_.evict_failed->get()->Increment();
            LOG_WARNING("Evicter try evict object error, continue")
                .put("PartitionId", partition_->GetPartitionID())
                .put("Error", status.ToString());
            continue;
        }

        used = allocator_manager_->GetTotalAllocedSize();
        LOG_DEBUG("Evicter used bytes after evict one object")
            .put("PartitionId", partition_->GetPartitionID())
            .put("used_bytes", used);

        metrics_.evict_qps->get()->Increment();
        metrics_.evict_object_qps->get()->Add(objects.size());
        metrics_.evict_latency->get()->Set(cost.GetElapsedInUs());
    }

    return Status::OK();
}

}  // namespace partition
}  // namespace bcache2
