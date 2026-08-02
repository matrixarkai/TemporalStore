#include "cache_instance.h"

#include "buffer/string_buffer.h"
#include "buffer/string_view_buffer.h"
#include "common/logging.h"
#include "policy/fifo.h"
#include "policy/slru.h"
#include "storage/dram_storage.h"
#include "storage/pmem_storage.h"
#include "storage/simple_storage.h"

#ifdef BUILD_SSD_CACHE

#include "storage/multi_ssd.h"
#include "storage/ssd_terarkdb.h"

#ifdef BUILD_ZONED_STORE
#include "storage/zoned_store/zoned_store.h"
#endif

#endif

#include <gflags/gflags.h>

// Switch to use Pmem Cache Recover after crash
DECLARE_bool(cache_enable_pmem_data_recovery);
// Switch to use SSD Cache Recover after crash
DECLARE_bool(cache_enable_ssd_data_recovery);
DECLARE_double(allocator_capacity_extra_ratio);

namespace mtcache {

CacheInstance::CacheInstance(std::size_t capacity,
                             ReplacementPolicyType replace_type,
                             StorageEngineType storage_type,
                             std::vector<std::string> paths) {
  CHECK_GT(capacity, 0);
  capacity_ = capacity;
  replacement_type_ = replace_type;
  storage_type_ = storage_type;
  paths_ = std::move(paths);
  initialized_ = false;
  eviction_handler_status_ = false;
}

CacheInstance::~CacheInstance() {
  if (initialized_) Stop();
}

noodle::Result<void, CacheError> CacheInstance::Start() {
  CHECK(storage_type_ != StorageEngineType::kMaxCode);
  CHECK(replacement_type_ != ReplacementPolicyType::kMaxCode);

  // Register related metrics
  if (metric_registry_) {
    RegisterCacheInstanceMetrics();
  } else {
    LOG(WARNING) << "CacheInstance:: invalid metric_registry_. Please "
                    "SetMetricRegistry() before start CacheInstance;";
  }

  switch (storage_type_) {
    case StorageEngineType::kDRAMStorageEngine:
      storage_engine_ = std::make_unique<StorageEngineDram>(
          Convert2AllocatorCapacity(capacity_), this, metric_registry_);
      break;
    case StorageEngineType::kPMEMStorageEngine:
      storage_engine_ = std::make_unique<StorageEnginePMem>(
          Convert2AllocatorCapacity(capacity_), paths_, this, metric_registry_);
      break;
    case StorageEngineType::kSimpleStorageEngine:
      storage_engine_ = std::make_unique<StorageEngineSimple>();
      break;
#ifdef BUILD_SSD_CACHE
    case StorageEngineType::kSSDTerarkDBStorageEngine:
    case StorageEngineType::kMultiSSDStorageEngine:
#ifdef BUILD_ZONED_STORE
    case StorageEngineType::kSSDZonedStoreStorageEngine:
#endif
      storage_engine_ = std::make_unique<StorageEngineMultiSSD>(
          paths_, capacity_, metric_registry_, storage_type_);
      use_device_ = true;
      {
        // Add a eviction function to reclaim space in SSD.
        eviction_handler_status_ = true;
        eviction_cb_ = [&](CacheBufferSharedPtr p) {
          storage_engine_->Delete(p->Key());
        };
      }
      break;
#endif
    default:
      return &Errors::kNotImplemented;
  }

  if (!storage_engine_->Start()) {
    LOG(ERROR) << "CacheInstance::Start(): failed to start storage engine.";
    return &Errors::kStorageEngineUninitialized;
  }
  switch (replacement_type_) {
    case ReplacementPolicyType::kFIFO:
      replacement_policy_ = std::make_unique<ReplacementFIFO>(capacity_);
      break;
    case ReplacementPolicyType::kSLRU: {
      replacement_policy_ = std::make_unique<ReplacementSLRU>(capacity_);
      break;
    }
    default:
      return &Errors::kNotImplemented;
  }

  auto init_res = replacement_policy_->Init();
  if (!init_res.IsOk()) {
    LOG(ERROR) << "CacheInstance::Start(): failed to init replacement policy.";
    return &Errors::kReplacementPolicyUninitialized;
  }

  bool recover_pmem = storage_type_ == StorageEngineType::kPMEMStorageEngine &&
                      FLAGS_cache_enable_pmem_data_recovery;
  bool recover_ssd =
      (storage_type_ == StorageEngineType::kSSDTerarkDBStorageEngine ||
#ifdef BUILD_ZONED_STORE
       storage_type_ == StorageEngineType::kSSDZonedStoreStorageEngine ||
#endif
       storage_type_ == StorageEngineType::kMultiSSDStorageEngine) &&
      FLAGS_cache_enable_ssd_data_recovery;
  LOG(INFO) << "CacheInstance"
            << ", storage_type: " << int(storage_type_)
            << ", recover_ssd: " << recover_ssd
            << ", recover_pmem: " << recover_pmem
            << ", recover_flag: " << FLAGS_cache_enable_ssd_data_recovery;
  if (recover_pmem || recover_ssd) {
    auto res = RecoverData();
    if (!res.IsOk()) {
      // TODO(xiongmu): need reset?
      LOG(WARNING) << "CacheInstance::RecoverData: failed to recover data.";
    }
  }
  initialized_ = true;
  return {};
}

// ReplacementPolicy, StorageEngine should be stopped and destroyed in
// Stop() method but not destructor, because they are created
// in the Start() method.
noodle::Result<void, CacheError> CacheInstance::Stop() {
  bool success = true;
  // StorageEngine must stop before ReplacementPolicy. When the
  // ReplacementPolicy is destroyed, all the CacheBufferSharedPtr in its
  // hashmap will destroy, which will call StorageEngine::Delete/AsyncDelete
  // to delete the CacheBuffer. But we do not want the cache records in PMEM
  // be deleted since they can be reused next time. So we stop the StorageEngine
  // before replacement policy and then StorageEngine::Delete/AsyncDelete
  // will do nothing when the ReplacementPolicy asks it to delete the
  // CacheBuffers. As for DRAM cache, the space occupied by these CacheBuffers
  // will be freed automatically when the DramAllocator is destroyed.
  if (storage_engine_) {
    bool stop_res = storage_engine_->Stop();
    if (!stop_res) {
      LOG(ERROR) << "CacheInstance::Stop(): fail to stop storage engine.";
      success = false;
    }
  }
  if (replacement_policy_) {
    auto res = replacement_policy_->Reset();
    if (!res.IsOk()) {
      LOG(ERROR) << "CacheInstance::Stop(): fail to reset replacement policy.";
      success = false;
    }
  }
  // AsyncWrite callback still need to use Update, unset initialized_ before
  // Stop causes failed check on initialized_
  initialized_ = false;
  // ReplacementPolicy must remain valid until StorageEngine stops because
  // the async tasks in StorageEngine may call ReplacementPolicy::Delete or
  // ReplacementPolicy::UpdateCacheBuffer to update the index.
  replacement_policy_.reset();
  // StorageEngine must remain valid until ReplacementPolicy resets because
  // the CacheBufferSharedPtr in ReplacementPolicy's hashmap will call
  // StorageEngine::Delete or StorageEngine::AsyncDelete when the hashmap
  // destroy. At that time the StorageEngine has stopped and will do nothing
  // in Delete/AsyncDelete.
  //
  // DO NOT uncomment the next line!
  // The storage_engine_ should be destroyed when CacheInstance is destroyed
  // in UnifiedCache. Please refer to comments above `pmem_instance_.reset();`
  // and `dram_instance_.reset();` at UnifiedCache::Stop() for the reason.
  //
  // storage_engine_.reset();
  if (success) {
    return {};
  } else {
    return &Errors::kCacheInstanceStopFailed;
  }
}

void CacheInstance::TEST_JoinPmemWriteExecutor() {
  if (storage_type_ == StorageEngineType::kPMEMStorageEngine) {
    auto* pmem_storage =
        reinterpret_cast<StorageEnginePMem*>(storage_engine_.get());
    pmem_storage->TEST_JoinPmemWriteExecutor();
  }
}

void CacheInstance::HandleEvictedBuffers(
    std::vector<CacheBufferSharedPtr> evicted) {
  if (evicted.empty()) {
    return;
  }
  // Process evicted buffers
  if (eviction_cb_ && eviction_handler_status_) {
    for (auto i = 0; i < evicted.size(); i++) {
      eviction_cb_(evicted[i]);
    }
  }
  // Update the eviction counter metric for the cache instance
  if (eviction_metric_cb_) {
    eviction_metric_cb_(evicted.size());
  }
}

void CacheInstance::PutBypassStorage(CacheBufferSharedPtr value) {
  DCHECK(initialized_);
  PutCacheBufferToPolicy(value, __func__);
}

noodle::Result<CacheBufferSharedPtr, CacheError> CacheInstance::Put(
    const std::string& key, folly::IOBuf value) {
  DCHECK(initialized_);

  // If a key already exists, we will simply create a new cache buffer and
  // update the index to point to the new cache buffer. The old cache buffer
  // will be released when their reference count become zero.
  // TODO(david.gong): We may want to add version info later to support MVCC.

  // Put cache key and value into storage engine
  auto engine_put_result = storage_engine_->Put(key, std::move(value));

  if (engine_put_result.IsOk()) {
    PutCacheBufferToPolicy(engine_put_result.Get(), __func__);
  }

  // The reference to the cache buffer holds even if the cache buffer is evicted
  // during HandleEvictedCacheBuffers.
  return engine_put_result;
}

folly::SemiFuture<noodle::Result<CacheBufferSharedPtr, CacheError>>
CacheInstance::AsyncPut(CacheBufferSharedPtr value,
                        const std::string_view& src) {
  DCHECK(initialized_);
  DCHECK(storage_type_ == StorageEngineType::kDRAMStorageEngine ||
         storage_type_ == StorageEngineType::kPMEMStorageEngine);

  // Insert the existing cache buffer into replacement policy. This cache
  // buffer may not belongs to this CacheInstance (e.g. the input `value`
  // may be a DRAM cache buffer while this CacheInstance is a PMEM instance
  // or the input `value` may be a PMEM cache buffer while this CacheInstance
  // is a DRAM instance).
  PutCacheBufferToPolicy(value, __func__);

  // We can not use reference here because `value` may have been evicted from
  // replacement_policy_ when the callback passed to StorageEngine::AsyncPut
  // is invoked. Then `value` is destroyed and the reference to its `key_` is
  // invalid.
  return storage_engine_->AsyncPut(
      value,
      [this, old_ptr = value->Data(), key = value->Key(), src](
          noodle::Result<CacheBufferSharedPtr, CacheError> async_res) -> void {
        if (async_res.IsOk()) {
          // Update the CacheBuffer at the index of replacement policy.
          CacheBufferSharedPtr new_buffer = std::move(async_res).Get();
          DCHECK(key == new_buffer->Key());
          VLOG(2) << "CacheInstance::AsyncPut success, src=" << src
                  << ", key=" << key << ", size=" << new_buffer->Size()
                  << ". Starting to update buffer in index";
          auto res = Update(key, old_ptr, std::move(new_buffer));
          if (LIKELY(res.IsOk())) {
            VLOG(2) << "CacheInstance::AsyncPut Update index success, src="
                    << src << ", key=" << key;
          } else if (res.GetError() == &Errors::kCacheBufferNotFound) {
            if (metric_registry_) {
              async_put_failed_cache_buffer_not_found_counter_->Increase();
            }
            // The old CacheBuffer has been evicted. So we just discard the
            // new_buffer.
            LOG(WARNING) << "Old buffer not found in AsyncPut, src=" << src
                         << ", key=" << key << ". Deleting the new buffer...";
          } else if (res.GetError() == &Errors::kCacheReplaceMismatch) {
            if (metric_registry_) {
              async_put_failed_cache_replace_mismatch_counter_->Increase();
            }
            // Usually this should never happen, except two cases:
            // 1. Another CacheBuffer with the same key but different value
            //    is inserted into replacement_policy_. But since every
            //    cache
            //    record in MTCache has a different key, this can not
            //    happen.
            // 2. The old CacheBuffer was inserted and later evicted and
            // then
            //    inserted again. In this case two AsyncPut tasks were
            //    pushed
            //    to the background executor. Then the older task's old_ptr
            //    is
            //    invalid and the older task will end up with a
            //    kCacheReplaceMismatch error. But the younger task will
            //    success and all things will get right.
            LOG(WARNING) << "Old buffer data ptr mismatch in AsyncPut, src="
                         << src << ", key=" << key
                         << ". Deleting the new buffer...";
          } else {
            if (metric_registry_) {
              async_put_failed_unknown_error_counter_->Increase();
            }
            LOG(ERROR) << "Unknown error in AsyncPut, src=" << src
                       << ", key=" << key;
          }
        } else {
          if (metric_registry_) {
            async_put_failed_storage_engine_full_counter_->Increase();
          }
          // Fail to AsyncPut `value` to StorageEngine. The old CacheBuffer
          // in `replacement_policy_` must be removed.
          LOG(ERROR) << "Fail to AsyncPut to StorageEngine, src=" << src
                     << ", key=" << key
                     << ". Deleting the new buffer and index...";
          replacement_policy_->Delete(key);
        }
      });
}

void CacheInstance::OnRecoverData(const std::string& key,
                                  CacheBufferSharedPtr buffer) {
  buffer->SetKey(key);

  PutCacheBufferToPolicy(std::move(buffer), __func__);
}

noodle::Result<CacheBufferSharedPtr, CacheError> CacheInstance::Get(
    const std::string& key) {
  DCHECK(initialized_);

  // If there is a SSD instance, we first load entry from SSD and then update
  // the cache policy. The invariant principle is that if the entry is in cache,
  // it must be in SSD. In turn, if the entry is not in SSD, it must be not in
  // cache. However, because DRAM and PMEM may insert entries to the SSD index
  // before L2 write queue actually perform the write. We need to return
  // cache_buffer point as well in such case.
  if (use_device_) {
    auto storage_ret = storage_engine_->Get(key);
    // Found the target & Update the policy!
    auto cache_buffer = replacement_policy_->Get(key);
    if ((nullptr == cache_buffer) &&
        (&Errors::kStorageBufferNotFound == storage_ret.GetError())) {
      return &Errors::kCacheBufferNotFound;
    }
    if (nullptr == cache_buffer) {
      // add new entry into policy.
      auto value = storage_ret.Get();
      uint64_t value_length = value.get()->Size();
      auto string_view = std::make_shared<StringViewBuffer>(value_length);
      string_view->SetKey(key);

      PutCacheBufferToPolicy(string_view, "GetFromDevice");
    }
    // The L2 write back queue might write key to the SSD in this process
    // Simply read from SSD again.
    if ((!storage_ret.IsOk()) &&
        (dynamic_cast<StringViewBufferPtr>(cache_buffer.get()))) {
      storage_ret = storage_engine_->Get(key);
      // If entry on the SSD somehow gets deleted, the cache_buffer is invalid
      // and should be reset, and return as not found.
      if (!storage_ret.IsOk()) {
        cache_buffer.reset();
        return &Errors::kCacheBufferNotFound;
      }
    }
    if ((storage_ret.IsOk()) &&
        (storage_ret.GetError() != &Errors::kStorageBufferNotFound)) {
      return storage_ret;
    } else {
      // This is only for the case when evicted cache entry does not reached SSD
      // yet.
      return cache_buffer;
    }
  }

  auto cache_buffer = replacement_policy_->Get(key);
  if (nullptr == cache_buffer) {
    VLOG(1) << "Cannot find cache buffer with key: " << key;
    return &Errors::kCacheBufferNotFound;
  }
  return cache_buffer;
}

CacheBufferSharedPtr CacheInstance::GetBypassReplacementPolicy(
    const std::string& key) {
  DCHECK(initialized_);
  // As CacheInstance no longer maintains an index layer, we still need to
  // locate the buffer via the replacement policy. We call the Peek() instead of
  // Get() method of replacement policy to avoid impacting eviction decisions.
  return replacement_policy_->Peek(key);
}

bool CacheInstance::Peek(const std::string& key) {
  DCHECK(initialized_);

  // Ignore index from replacement policy, return result according to storage
  // engine. This only works for storage engines with their own index (e.g.
  // ssd_terarkdb).
  return storage_engine_->Peek(key);
}

noodle::Result<void, CacheError> CacheInstance::Delete(const std::string& key) {
  DCHECK(initialized_);

  noodle::Result<void, CacheError> ret;
  auto cache_buffer = replacement_policy_->Delete(key);
  if (nullptr == cache_buffer) {
    VLOG(1) << "Cannot find cache buffer with key: " << key;
    return &Errors::kCacheBufferNotFound;
  }

  // CacheInstance does NOT maintain a delete counter for delete OP, as the
  // UnifiedCache already has one and a buffer is deleted from all member cache
  // instances when it is deleted from the unified cache. But technically, evict
  // and delete have the same outcome for the specified buffer. Thus we reuse
  // the eviction counter here to track delete operations.
  if (eviction_metric_cb_) {
    eviction_metric_cb_(1);
  }

  return ret;
}

noodle::Result<void, CacheError> CacheInstance::Update(
    const std::string& key, const char* old_data_ptr,
    CacheBufferSharedPtr new_buffer) {
  DCHECK(initialized_);

  // Update the cache buffer in replacement policy.
  // The caller of Update method should have already populated key in the new
  // buffer.
  return replacement_policy_->UpdateCacheBuffer(key, old_data_ptr, new_buffer);
}

noodle::Result<void, CacheError> CacheInstance::Reset() {
  DCHECK(initialized_);
  auto reset_result = storage_engine_->Reset();
  if (!reset_result.IsOk()) {
    LOG(ERROR) << "CacheInstance::Reset: failed to reset storage engine!"
               << " Error is: " << reset_result.GetError()->GetMessage();
    initialized_ = false;
    return reset_result.GetError();
  }
  reset_result = replacement_policy_->Reset();
  if (!reset_result.IsOk()) {
    LOG(ERROR) << "CacheInstance::Reset: failed to reset replacement policy!"
               << " Error is: " << reset_result.GetError()->GetMessage();
    initialized_ = false;
  }
  return reset_result;
}

noodle::Result<void, CacheError> CacheInstance::RecoverData() {
  CHECK(!initialized_);
  if (storage_type_ != StorageEngineType::kPMEMStorageEngine &&
      storage_type_ != StorageEngineType::kSSDTerarkDBStorageEngine &&
      storage_type_ != StorageEngineType::kSSDZonedStoreStorageEngine &&
      storage_type_ != StorageEngineType::kMultiSSDStorageEngine) {
    LOG(WARNING) << "CacheInstance::RecoverData: RecoverData is only for "
                    "kPMEMStorageEngine|kSSDTerarkDBStorageEngine|"
                    "kMultiSSDStorageEngine. storage_type_="
                 << static_cast<int>(storage_type_);
    return &Errors::kRecoverUnsupportedFailed;
  }
  return storage_engine_->RecoverData(this);
}

void CacheInstance::SetCapacity(size_t capacity) {
  capacity_ = capacity;
  replacement_policy_->SetCapacity(capacity);
}

std::size_t CacheInstance::GetUsedSpace() const {
  return replacement_policy_->GetUsedSpace();
}

void CacheInstance::RegisterCacheInstanceMetrics() {
  instance_capacity_bytes_gauge_ =
      metric_registry_->MustRegister<noodle::AtomicGauge>(
          noodle::MetricId("instance_capacity_bytes"),
          std::make_shared<noodle::AtomicGauge>());
  instance_used_capacity_bytes_gauge_ =
      metric_registry_->MustRegister<noodle::AtomicGauge>(
          noodle::MetricId("instance_used_capacity_bytes"),
          std::make_shared<noodle::AtomicGauge>());
  instance_item_num_gauge_ =
      metric_registry_->MustRegister<noodle::AtomicGauge>(
          noodle::MetricId("item_exists"),
          std::make_shared<noodle::AtomicGauge>());
  async_put_failed_cache_buffer_not_found_counter_ =
      metric_registry_->MustRegister<noodle::AtomicCounter>(
          noodle::MetricId("failed_async_put_cache_buffer_not_found"),
          std::make_shared<noodle::AtomicCounter>());
  async_put_failed_cache_replace_mismatch_counter_ =
      metric_registry_->MustRegister<noodle::AtomicCounter>(
          noodle::MetricId("failed_async_put_cache_replace__mismatch"),
          std::make_shared<noodle::AtomicCounter>());
  async_put_failed_unknown_error_counter_ =
      metric_registry_->MustRegister<noodle::AtomicCounter>(
          noodle::MetricId("failed_async_put_unknown_error"),
          std::make_shared<noodle::AtomicCounter>());
  async_put_failed_storage_engine_full_counter_ =
      metric_registry_->MustRegister<noodle::AtomicCounter>(
          noodle::MetricId("failed_async_put_storage_engine_full"),
          std::make_shared<noodle::AtomicCounter>());
}

void CacheInstance::PutCacheBufferToPolicy(
    CacheBufferSharedPtr value, const std::string_view& description) {
  // Insert cache buffer into replacement policy
  auto evicted = replacement_policy_->Put(value);
  HandleEvictedBuffers(evicted);
  VLOG(2) << "Handle evicted cache buffers"
          << ", evicted count=" << evicted.size()
          << ", description=" << description;
  if (VLOG_IS_ON(2)) {
    VLOG(2) << "Evicted keys: ";
    for (auto i = 0; i < evicted.size(); ++i) {
      VLOG(2) << "key=" << evicted[i]->Key() << ", ";
    }
  }

  // Update Metrics
  if (metric_registry_) {
    instance_capacity_bytes_gauge_->SetValue(GetCapacity());
    instance_used_capacity_bytes_gauge_->SetValue(GetUsedSpace());
    instance_item_num_gauge_->SetValue(GetItemNum());
  }
  VLOG(2) << "Current capacity = " << GetCapacity()
          << " & current used space = " << GetUsedSpace()
          << " & current item num = " << GetItemNum();
}

size_t CacheInstance::Convert2AllocatorCapacity(size_t capacity) {
  size_t extra_capacity =
      static_cast<size_t>(FLAGS_allocator_capacity_extra_ratio * capacity);
  return capacity + extra_capacity;
}

}  // namespace mtcache
