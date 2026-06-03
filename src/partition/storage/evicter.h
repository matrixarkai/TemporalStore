// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/thread/async_thread.h>
#include <gflags/gflags.h>

#include <memory>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "partition/index/index.h"
#include "partition/metrics.h"
#include "partition/storage/object.h"
#include "protocol/server.pb.h"

namespace bcache2 {
namespace partition {

class Partition;
class ObjectManager;
class AllocatorManager;
class Index;
class OpLogger;
class SlotStore;

class Evicter {
 public:
    class Policy {  // only LRU policy is supported now
     public:
        virtual ~Policy() {}
        virtual std::vector<Object> GetBestObjects(int) = 0;
        virtual PolicyType::PolicyTypeEnum GetType() = 0;
    };
    class Actuator;

    Evicter(Partition* partition, ObjectManager* object_manager, Index* index, OpLogger* op_logger,
            SlotStore* slot_store, AllocatorManager* allocator_manager,
            MetricsManager* metrics_manager);
    virtual ~Evicter();

    Status Init(EvicterConfig config);
    Status TryEvict();
    Status UpdateConfig(const EvicterConfig& config) {
        config_.MergeFrom(config);
        return Status::OK();
    }

 private:
    class DumpActuator;
    class DelActuator;
    class PolicyLru : public Policy {
     public:
        explicit PolicyLru(Evicter* evicter)
            : evicter_(evicter), iterator_(evicter_->index_->NewSlotIterator()) {}
        ~PolicyLru() {}

        std::vector<Object> GetBestObjects(int batch_size) override;
        PolicyType::PolicyTypeEnum GetType() override { return PolicyType::LRU; }

     private:
        std::unordered_map<uint64_t, uint64_t> cached_entry_;
        Evicter* evicter_ = nullptr;
        std::unique_ptr<Index::SlotIterator> iterator_;
    };

    void CreateOrUpdatePolicy();

    Partition* partition_ = nullptr;
    ObjectManager* object_manager_ = nullptr;
    Index* index_ = nullptr;
    OpLogger* op_logger_ = nullptr;
    SlotStore* slot_store_ = nullptr;
    AllocatorManager* allocator_manager_ = nullptr;

    EvicterConfig config_;
    std::unique_ptr<Policy> policy_;
    std::unique_ptr<Actuator> actuator_;
    EvicterMetrics metrics_;

    DISALLOW_COPY_AND_ASSIGN(Evicter);
};

}  // namespace partition
}  // namespace bcache2
