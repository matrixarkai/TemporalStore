#include "unified_cache.h"

#include "buffer/iobuf_buffer.h"
#include "buffer/string_buffer.h"
#include "common/logging.h"
#include "policy/l1_imp.h"
#include "policy/l2_policy_factory.h"
#include "storage/storage_engine.h"

#ifdef BUILD_ZONED_STORE
#include "storage/zoned_store/metrics.h"
#include "storage/zoned_store/zoned_store.h"
#endif

#include <algorithm>

DECLARE_int32(ssd_engine_type);
DECLARE_bool(cache_collect_latency_summary);
DECLARE_int32(cache_latency_summary_time_window);
DECLARE_bool(cache_pmem_enable_async_write);
DECLARE_bool(mtcache_enable_ssd_promotion);
DECLARE_bool(mtcache_enable_pmem_promotion);
DECLARE_bool(cache_enable_eviction_handler);

namespace mtcache {

// ParseReplacementPolicy is a utility function to parse replacement type from
// config option to enum ReplacementPolicyType.
static ReplacementPolicyType ParseReplacementPolicy(const std::string& policy) {
  if (policy == "FIFO") {
    return ReplacementPolicyType::kFIFO;
  } else if (policy == "SLRU") {
    return ReplacementPolicyType::kSLRU;
  } else {
    LOG(FATAL) << "Invalid cache replacement policy: " << policy;
  }
  return ReplacementPolicyType::kMaxCode;
}

// ParseDataPlacementType is a utility function to parse data placement type
// from config option to enum DRAMPMEMDataPlacementType.
static UnifiedCache::DRAMPMEMDataPlacementType ParseDataPlacementType(
    const std::string& type) {
  if (type == "SideBySide") {
    return UnifiedCache::DRAMPMEMDataPlacementType::kSideBySide;
  } else if (type == "Tiered") {
    return UnifiedCache::DRAMPMEMDataPlacementType::kTiered;
  } else {
    LOG(FATAL) << "Invalid DRAM PMEM data placement type: " << type;
  }
  return UnifiedCache::DRAMPMEMDataPlacementType::kMaxCode;
}

UnifiedCache::CacheHandle::CacheHandle(CacheBufferSharedPtr buffer) {
  buffer_ = std::move(buffer);
  value_ = folly::IOBuf::wrapBufferAsValue(buffer_->Data(), buffer_->Size());
}

UnifiedCache::Handle* UnifiedCache::CacheHandle::Clone() const {
  return static_cast<UnifiedCache::Handle*>(
      new UnifiedCache::CacheHandle(buffer_));
}

UnifiedCache::UnifiedCache(const CacheOptions& opts) {
  placement_type_ =
      ParseDataPlacementType(opts.cache_dram_pmem_data_placement_type);
  data_placement_threshold_ = opts.cache_dram_pmem_data_placement_threshold;

  dram_capacity_ = opts.dram_capacity;
  pmem_capacity_ = opts.pmem_capacity;
  ssd_capacity_ = opts.ssd_capacity;
  // Path
  pmem_paths_ = opts.pmem_paths;
  ssd_paths_ = opts.ssd_paths;
  // Replacement policy for each tier
  dram_replacement_type_ =
      ParseReplacementPolicy(opts.cache_dram_replacement_policy);
  pmem_replacement_type_ =
      ParseReplacementPolicy(opts.cache_pmem_replacement_policy);
  ssd_replacement_type_ =
      ParseReplacementPolicy(opts.cache_ssd_replacement_policy);

  initialized_ = false;
  access_record_cb_ = nullptr;

  cache_metric_id_prefix_ = opts.metric_id_prefix;
  metric_registry_tags_ = opts.metric_registry_tags;

  cache_ssd_instance_only_ = opts.cache_ssd_instance_only;

  VLOG(1) << "UnifiedCache: dram_capacity_ = " << dram_capacity_
          << " cache_ssd_instance_only = " << cache_ssd_instance_only_
          << " pmem_capacity_ = " << pmem_capacity_
          << " ssd_capacity_ = " << ssd_capacity_;
}

UnifiedCache::~UnifiedCache() {
  if (initialized_) {
    Stop();
  }
}

bool UnifiedCache::Start() {
  CHECK(!initialized_);
  auto startTime = std::chrono::steady_clock::now();

  // Create metric registry namespace for each component.
  std::string prefix = cache_metric_id_prefix_;
  unified_registry_ =
      noodle::GetMetricRegistry(prefix + ".unified", metric_registry_tags_);
  CHECK_NOTNULL(unified_registry_);
  dram_registry_ =
      noodle::GetMetricRegistry(prefix + ".dram", metric_registry_tags_);
  CHECK_NOTNULL(dram_registry_);
  pmem_registry_ =
      noodle::GetMetricRegistry(prefix + ".pmem", metric_registry_tags_);
  CHECK_NOTNULL(pmem_registry_);
  ssd_registry_ =
      noodle::GetMetricRegistry(prefix + ".ssd", metric_registry_tags_);
  CHECK_NOTNULL(ssd_registry_);

  // Register related metrics for unified cache
  RegisterComponentMetrics(CacheInstanceType::kUnified);

  // Create and start DRAM cache instance
  CHECK(dram_capacity_ > 0) << "Dram cache capacity must be larger than 0!";
  VLOG(1) << "UnifiedCache::Start: attempt to start a dram instance! "
          << "dram_capacity_ = " << dram_capacity_;
  dram_instance_ = std::make_unique<CacheInstance>(
      dram_capacity_, dram_replacement_type_,
      StorageEngineType::kDRAMStorageEngine, std::vector<std::string>{});
  dram_instance_->SetMetricRegistry(dram_registry_);
  dram_instance_->SetEvictionHandlerStatus(FLAGS_cache_enable_eviction_handler);
  auto start_res = dram_instance_->Start();
  if (!start_res.IsOk()) {
    LOG(ERROR) << "UnifiedCache::Start: Failed to start dram instance! "
               << "Error is: " << start_res.GetError()->GetMessage();
    return false;
  }
  VLOG(1) << "UnifiedCache::Start: Successfully started dram instance! "
          << "dram_capacity_ = " << dram_capacity_;
  // Register related metrics for dram instance
  RegisterComponentMetrics(CacheInstanceType::kDRAM);
  // PMEM cache instance is disabled if pmem_capacity is set to ZERO
  if (pmem_capacity_ > 0) {
    VLOG(1) << "UnifiedCache::Start: attempt to start a pmem instance! "
            << "pmem_capacity_ = " << pmem_capacity_;
    pmem_instance_ = std::make_unique<CacheInstance>(
        pmem_capacity_, pmem_replacement_type_,
        StorageEngineType::kPMEMStorageEngine, pmem_paths_);
    pmem_instance_->SetMetricRegistry(pmem_registry_);
    pmem_instance_->SetEvictionHandlerStatus(
        FLAGS_cache_enable_eviction_handler);
    start_res = pmem_instance_->Start();
    if (!start_res.IsOk()) {
      LOG(ERROR) << "UnifiedCache::Start: Failed to start pmem instance! "
                 << "Error is: " << start_res.GetError()->GetMessage();
      return false;
    }
    VLOG(1) << "UnifiedCache::Start: Successfully started pmem instance! "
            << "pmem_capacity_ = " << pmem_capacity_
            << " 1st pmem_path_ = " << pmem_paths_[0];
    // Register related metrics for pmem instance
    RegisterComponentMetrics(CacheInstanceType::kPMEM);
  }
  // SSD cache instance is disabled if ssd_capacity is set to ZERO
  if (ssd_capacity_ > 0) {
    VLOG(1) << "UnifiedCache::Start: attempt to start a ssd instance! "
            << "ssd_capacity_ = " << ssd_capacity_;
    ssd_cache_instance_ = std::make_unique<CacheInstance>(
        ssd_capacity_, ssd_replacement_type_,
        StorageEngineType::kMultiSSDStorageEngine, ssd_paths_);
    if (static_cast<SSDEngineType>(FLAGS_ssd_engine_type) ==
        SSDEngineType::kZonedStore) {
      zoned_store_registry_ = noodle::GetMetricRegistry(prefix + ".zonedstore");
      CHECK_NOTNULL(zoned_store_registry_);
#ifdef BUILD_ZONED_STORE
      ZonedStoreMetrics::instance()->Start(zoned_store_registry_);
#endif
    }

    ssd_cache_instance_->SetMetricRegistry(ssd_registry_);
    // ssd instance does not need to set eviction handler status because it
    // will always set it to true and delete the record on ssd when a record
    // is evicted.
    start_res = ssd_cache_instance_->Start();
    if (!start_res.IsOk()) {
      LOG(ERROR) << "UnifiedCache::Start: Failed to start ssd instance! "
                 << "Error is: " << start_res.GetError()->GetMessage();
      return false;
    }
    VLOG(1) << "UnifiedCache::Start: Successfully started ssd instance! "
            << "ssd_capacity_ = " << ssd_capacity_
            << " 1st ssd_path_ = " << ssd_paths_[0];
    // Register related metrics for ssd instance
    RegisterComponentMetrics(CacheInstanceType::kSSD);

    // L1 Cache Interface
    l1_cache_.reset(new L1CacheImplement(dram_instance_.get(),
                                         pmem_instance_.get(), ssd_registry_));

    // L2 Policy
    l2_cache_policy_ = L2CachePolicyFactory::CreateL2CachePolicy(
        l1_cache_.get(), ssd_cache_instance_.get(), ssd_registry_);
    l2_cache_policy_->RegisterRemoveL2PolicyHandler(
        [&](CacheBufferSharedPtr buffer) {
          ssd_cache_instance_->Delete(buffer->Key());
        });

    l2_cache_policy_->Start();

    access_record_cb_ = l2_cache_policy_.get();
  }

  initialized_ = true;
  RegisterEvictionHandler();
  RegisterEvictionMetrics();
  if (FLAGS_cache_enable_eviction_handler) {
    if (dram_instance_) {
      if (ssd_cache_instance_) {
        dram_instance_->RegisterPolicyMemEvictionHandler([&](
            CacheBufferSharedPtr buffer) {
          auto ret =
              ssd_cache_instance_->GetBypassReplacementPolicy(buffer->Key());
          if (ret.get() == nullptr) {
            ssd_cache_instance_->PutBypassStorage(buffer);
          }
          ret.reset();
        });
      }
      // DRAM needs to evict to the PMEM in Tiered config
      if (placement_type_ == DRAMPMEMDataPlacementType::kTiered &&
          pmem_instance_) {
        dram_instance_->RegisterPolicyMemEvictionHandler([&](
            CacheBufferSharedPtr buffer) {
          auto ret = pmem_instance_->GetBypassReplacementPolicy(buffer->Key());
          if (ret.get() == nullptr) {
            pmem_instance_->PutBypassStorage(buffer);
          }
          ret.reset();
        });
      }
    }
    if (pmem_instance_) {
      // Need to evict to SSD in either Tiered or SideBySide Mode
      if (ssd_cache_instance_) {
        pmem_instance_->RegisterPolicyMemEvictionHandler([&](
            CacheBufferSharedPtr buffer) {
          auto ret =
              ssd_cache_instance_->GetBypassReplacementPolicy(buffer->Key());
          if (ret.get() == nullptr) {
            ssd_cache_instance_->PutBypassStorage(buffer);
          }
          ret.reset();
        });
      }
    }
  } else {
    DisablePolicyMemEvictionHandler();
  }
  std::chrono::duration<double, std::milli> totalTime =
      std::chrono::steady_clock::now() - startTime;
  LOG(INFO) << "UnifiedCache: Cost " << totalTime.count() << " ms to start.";
  unified_start_time_counter_->SetValue(
      static_cast<int64_t>(totalTime.count()));
  return true;
}

bool UnifiedCache::Stop() {
  CHECK(initialized_);
  bool failed_to_stop = false;
  initialized_ = false;
  if (l2_cache_policy_) {
    l2_cache_policy_->Stop();
    l2_cache_policy_.reset();
  }
  if (l1_cache_) {
    l1_cache_.reset();
  }

  if (ssd_cache_instance_) {
    auto stop_res = ssd_cache_instance_->Stop();
    if (!stop_res.IsOk()) {
      LOG(ERROR) << "UnifiedCache::Stop: Failed to stop l2arc cache instance!"
                 << " Error is: " << stop_res.GetError()->GetMessage();
      failed_to_stop = true;
    }
    ssd_cache_instance_.reset();
  }
  if (pmem_instance_) {
    auto stop_res = pmem_instance_->Stop();
    if (!stop_res.IsOk()) {
      LOG(ERROR) << "UnifiedCache::Stop: Failed to stop pmem cache instance!"
                 << " Error is: " << stop_res.GetError()->GetMessage();
      failed_to_stop = true;
    }
  }
  if (dram_instance_) {
    auto stop_res = dram_instance_->Stop();
    if (!stop_res.IsOk()) {
      LOG(ERROR) << "UnifiedCache::Stop: Failed to stop dram cache instance!"
                 << " Error is: " << stop_res.GetError()->GetMessage();
      failed_to_stop = true;
    }
  }
  // DRAM and PMEM cache StorageEngines in the CacheInstances  must be
  // destroyed after their concurrent hashmap being destroyed inside their
  // Stop() method. This is because their concurrent hashmap may contains
  // the cache buffers to each other due to async cache promotion (PMEM
  // to DRAM) and eviction (DRAM to PMEM).
  //
  // If StorageEngine is destroyed before concurrent hashmap, the dangling
  // PMEM buffers inside the concurrent-hashmap will call
  // PMEMStorageEngine::Delete() from the dtor of RawBuffer and cause
  // use-after-free error.
  pmem_instance_.reset();
  dram_instance_.reset();
  // Keep metrics process-lifetime in the local build. The bundled Noodle
  // prefix deregistration path aborts during shutdown with this toolchain.

  return !failed_to_stop;
}

void UnifiedCache::TEST_JoinPmemWriteExecutor() {
  if (pmem_instance_) {
    pmem_instance_->TEST_JoinPmemWriteExecutor();
  }
}

void UnifiedCache::RegisterComponentMetrics(CacheInstanceType type) {
  switch (type) {
    case CacheInstanceType::kUnified: {
      unified_acquires_counter_ =
          unified_registry_->MustRegister<noodle::AtomicCounter>(
              noodle::MetricId("acquires"),
              std::make_shared<noodle::AtomicCounter>());
      unified_hits_counter_ =
          unified_registry_->MustRegister<noodle::AtomicCounter>(
              noodle::MetricId("hits"),
              std::make_shared<noodle::AtomicCounter>());
      unified_misses_counter_ =
          unified_registry_->MustRegister<noodle::AtomicCounter>(
              noodle::MetricId("misses"),
              std::make_shared<noodle::AtomicCounter>());
      unified_puts_counter_ =
          unified_registry_->MustRegister<noodle::AtomicCounter>(
              noodle::MetricId("puts"),
              std::make_shared<noodle::AtomicCounter>());
      unified_deletes_counter_ =
          unified_registry_->MustRegister<noodle::AtomicCounter>(
              noodle::MetricId("deletes"),
              std::make_shared<noodle::AtomicCounter>());
      unified_insert_pinned_counter_ =
          unified_registry_->MustRegister<noodle::AtomicCounter>(
              noodle::MetricId("insert_pinned"),
              std::make_shared<noodle::AtomicCounter>());
      unified_acquire_latency_ = std::make_shared<noodle::SampleSetTimeSummary>(
          noodle::TimeUnit::kNs,
          std::make_shared<noodle::TimeWindowSampleSet>(
              noodle::Time::FromSec(FLAGS_cache_latency_summary_time_window)));
      unified_registry_->MustRegister<noodle::SampleSetTimeSummary>(
          noodle::MetricId("latency"), unified_acquire_latency_);
      unified_dram_lookup_latency_ =
          std::make_shared<noodle::SampleSetTimeSummary>(
              noodle::TimeUnit::kNs,
              std::make_shared<noodle::TimeWindowSampleSet>(
                  noodle::Time::FromSec(
                      FLAGS_cache_latency_summary_time_window)));
      unified_registry_->MustRegister<noodle::SampleSetTimeSummary>(
          noodle::MetricId("dram_lookup_latency"),
          unified_dram_lookup_latency_);
      unified_pmem_lookup_latency_ =
          std::make_shared<noodle::SampleSetTimeSummary>(
              noodle::TimeUnit::kNs,
              std::make_shared<noodle::TimeWindowSampleSet>(
                  noodle::Time::FromSec(
                      FLAGS_cache_latency_summary_time_window)));
      unified_registry_->MustRegister<noodle::SampleSetTimeSummary>(
          noodle::MetricId("pmem_lookup_latency"),
          unified_pmem_lookup_latency_);
      unified_ssd_lookup_latency_ =
          std::make_shared<noodle::SampleSetTimeSummary>(
              noodle::TimeUnit::kNs,
              std::make_shared<noodle::TimeWindowSampleSet>(
                  noodle::Time::FromSec(
                      FLAGS_cache_latency_summary_time_window)));
      unified_registry_->MustRegister<noodle::SampleSetTimeSummary>(
          noodle::MetricId("ssd_lookup_latency"), unified_ssd_lookup_latency_);
      unified_start_time_counter_ =
          unified_registry_->MustRegister<noodle::AtomicGauge>(
              noodle::MetricId("time_of_unified_cache_start"),
              std::make_shared<noodle::AtomicGauge>());
      break;
    }
    case CacheInstanceType::kDRAM: {
      dram_hits_counter_ = dram_registry_->MustRegister<noodle::AtomicCounter>(
          noodle::MetricId("hits"), std::make_shared<noodle::AtomicCounter>());
      dram_misses_counter_ =
          dram_registry_->MustRegister<noodle::AtomicCounter>(
              noodle::MetricId("misses"),
              std::make_shared<noodle::AtomicCounter>());
      dram_puts_counter_ = dram_registry_->MustRegister<noodle::AtomicCounter>(
          noodle::MetricId("puts"), std::make_shared<noodle::AtomicCounter>());
      dram_evicts_counter_ =
          dram_registry_->MustRegister<noodle::AtomicCounter>(
              noodle::MetricId("evicts"),
              std::make_shared<noodle::AtomicCounter>());
      dram_cache_insert_failed_counter_ =
          dram_registry_->MustRegister<noodle::AtomicCounter>(
              noodle::MetricId("dram_cache_insert_failed"),
              std::make_shared<noodle::AtomicCounter>());
      no_index_dram_buffer_insert_failed_counter_ =
          dram_registry_->MustRegister<noodle::AtomicCounter>(
              noodle::MetricId("no_index_dram_buffer_insert_failed"),
              std::make_shared<noodle::AtomicCounter>());
      break;
    }
    case CacheInstanceType::kPMEM: {
      pmem_hits_counter_ = pmem_registry_->MustRegister<noodle::AtomicCounter>(
          noodle::MetricId("hits"), std::make_shared<noodle::AtomicCounter>());
      pmem_misses_counter_ =
          pmem_registry_->MustRegister<noodle::AtomicCounter>(
              noodle::MetricId("misses"),
              std::make_shared<noodle::AtomicCounter>());
      pmem_puts_counter_ = pmem_registry_->MustRegister<noodle::AtomicCounter>(
          noodle::MetricId("puts"), std::make_shared<noodle::AtomicCounter>());
      pmem_evicts_counter_ =
          pmem_registry_->MustRegister<noodle::AtomicCounter>(
              noodle::MetricId("evicts"),
              std::make_shared<noodle::AtomicCounter>());
      pmem_cache_insert_failed_counter_ =
          pmem_registry_->MustRegister<noodle::AtomicCounter>(
              noodle::MetricId("pmem_cache_insert_failed"),
              std::make_shared<noodle::AtomicCounter>());
      break;
    }
    case CacheInstanceType::kSSD: {
      ssd_hits_counter_ = ssd_registry_->MustRegister<noodle::AtomicCounter>(
          noodle::MetricId("hits"), std::make_shared<noodle::AtomicCounter>());
      ssd_misses_counter_ = ssd_registry_->MustRegister<noodle::AtomicCounter>(
          noodle::MetricId("misses"),
          std::make_shared<noodle::AtomicCounter>());
      ssd_evicts_counter_ = ssd_registry_->MustRegister<noodle::AtomicCounter>(
          noodle::MetricId("evicts"),
          std::make_shared<noodle::AtomicCounter>());
      ssd_cache_insert_failed_counter_ =
          ssd_registry_->MustRegister<noodle::AtomicCounter>(
              noodle::MetricId("ssd_cache_insert_failed"),
              std::make_shared<noodle::AtomicCounter>());
    }
  }
}

void UnifiedCache::SetReplacementPolicyType(
    CacheInstanceType instance_type, ReplacementPolicyType replacement_type) {
  // The replacement policy cannot be changed after a cache instance starts.
  CHECK(!initialized_);

  if (instance_type == CacheInstanceType::kDRAM) {
    dram_replacement_type_ = replacement_type;
  } else if (instance_type == CacheInstanceType::kPMEM) {
    pmem_replacement_type_ = replacement_type;
  } else if (instance_type == CacheInstanceType::kSSD) {
    ssd_replacement_type_ = replacement_type;
  } else {
    LOG(FATAL) << "UnifiedCache::SetReplacementPolicyType: Cannot set "
               << "replacement policy for instance type" << (int)instance_type;
  }
}

ReplacementPolicyType UnifiedCache::GetReplacementPolicyType(
    CacheInstanceType type) const {
  switch (type) {
    case CacheInstanceType::kDRAM:
      return dram_replacement_type_;
    case CacheInstanceType::kPMEM:
      return pmem_replacement_type_;
    case CacheInstanceType::kSSD:
      return ssd_replacement_type_;
    default:
      LOG(FATAL) << "UnifiedCache::GetReplacementPolicyType: Invalid cache "
                 << "instance type: " << (int)type;
  }
  return ReplacementPolicyType::kMaxCode;
}

// Cache capacity is set when initializing cache or reloading cache.
void UnifiedCache::SetCapacity(CacheInstanceType type, size_t capacity) {
  switch (type) {
    case CacheInstanceType::kDRAM:
      dram_capacity_ = capacity;
      if (dram_instance_) {
        dram_instance_->SetCapacity(capacity);
      }
      break;
    case CacheInstanceType::kPMEM:
      pmem_capacity_ = capacity;
      if (pmem_instance_) {
        pmem_instance_->SetCapacity(capacity);
      }
      break;
    case CacheInstanceType::kSSD:
      ssd_capacity_ = capacity;
      if (ssd_cache_instance_) {
        ssd_cache_instance_->SetCapacity(capacity);
      }
      break;
    default:
      LOG(FATAL) << "UnifiedCache::SetCapacity: Invalid cache instance type: "
                 << (int)type;
  }
}

size_t UnifiedCache::GetCapacity(CacheInstanceType type) const {
  switch (type) {
    case CacheInstanceType::kDRAM:
      return dram_capacity_;
    case CacheInstanceType::kPMEM:
      return pmem_capacity_;
    case CacheInstanceType::kSSD:
      return ssd_capacity_;
    default:
      LOG(FATAL) << "UnifiedCache::GetCapacity: Invalid cache instance type: "
                 << (int)type;
  }
  return 0;
}

size_t UnifiedCache::GetUsed(CacheInstanceType type) const {
  DCHECK(initialized_);
  switch (type) {
    case CacheInstanceType::kDRAM:
      return dram_instance_->GetUsedSpace();
    case CacheInstanceType::kPMEM:
      if (pmem_instance_) {
        return pmem_instance_->GetUsedSpace();
      } else {
        return 0;
      }
    case CacheInstanceType::kSSD:
      if (ssd_cache_instance_) {
        return ssd_cache_instance_->GetUsedSpace();
      } else {
        return 0;
      }
    default:
      LOG(FATAL) << "UnifiedCache::GetUsed: Invalid cache instance type: "
                 << (int)type;
  }
  return 0;
}

noodle::Result<CacheBufferSharedPtr, CacheError> UnifiedCache::InsertBuffer(
    CacheInstanceType type, const Key& key, Value value, size_t size) {
  // TODO(dbc)the param `size` here is useless. we can get the size from
  // value.Size(). Besides the space used by this key-value pair is
  // `key.size() + value.Size()`
  switch (type) {
    case CacheInstanceType::kDRAM: {
      dram_puts_counter_->Increase();
      VLOG(2) << "InsertBuffer: insert into DRAM cache, key=" << key
              << ", value size: " << size
              << ", cache type: " << cache_metric_id_prefix_;
      auto put_res = dram_instance_->Put(key, std::move(value));
      if (!put_res.IsOk()) {
        dram_cache_insert_failed_counter_->Increase();
        // reduce the output frequency for `AllocatorOutOfSpace`
        if (put_res.GetError() != &Errors::kAllocatorOutOfSpace) {
          LOG(ERROR)
              << "UnifiedCache::InsertBuffer: Failed to insert buffer into "
              << "DRAM cache instance. Key: " << key
              << ", error: " << put_res.GetError()->GetMessage();
        } else {
          LOG_EVERY_SECOND(ERROR)
              << "Got " << google::COUNTER << "th AllocatorOutOfSpace error in "
              << "UnifiedCache[DRAM]::InsertBuffer, key=" << key;
        }
      }
      return put_res;
    }
    case CacheInstanceType::kPMEM: {
      VLOG(2) << "InsertBuffer: insert to PMEM cache, key=" << key
              << ", value size: " << size
              << ", cache type: " << cache_metric_id_prefix_;
      if (!pmem_instance_) {
        pmem_cache_insert_failed_counter_->Increase();
        LOG(ERROR) << "UnifiedCache::InsertBuffer: Trying to insert into pmem "
                   << "cache instance, but it's disabled!";
        return &Errors::kStorageAccessError;
      }
      pmem_puts_counter_->Increase();
      auto put_res = pmem_instance_->Put(key, std::move(value));
      if (!put_res.IsOk()) {
        pmem_cache_insert_failed_counter_->Increase();
        // reduce the output frequency for `AllocatorOutOfSpace`
        if (put_res.GetError() != &Errors::kAllocatorOutOfSpace) {
          LOG(ERROR) << "InsertBuffer: Failed to insert buffer into "
                     << "PMEM cache instance. Key: " << key
                     << ", error: " << put_res.GetError()->GetMessage();
        } else {
          LOG_EVERY_SECOND(ERROR)
              << "Got " << google::COUNTER << "th AllocatorOutOfSpace error in "
              << "UnifiedCache[PMEM]::InsertBuffer, key=" << key;
        }
      }
      return put_res;
    }
    case CacheInstanceType::kSSD: {
      VLOG(2) << "InsertBuffer: insert to SSD cache, key=" << key
              << ", value size: " << size
              << ", cache type: " << cache_metric_id_prefix_;
      if (!ssd_cache_instance_) {
        ssd_cache_insert_failed_counter_->Increase();
        LOG(ERROR) << "UnifiedCache::InsertBuffer: Trying to insert into SSD "
                   << "cache instance, but it's disabled!";
        return &Errors::kStorageAccessError;
      }
      auto put_res = ssd_cache_instance_->Put(key, std::move(value));
      if (!put_res.IsOk()) {
        ssd_cache_insert_failed_counter_->Increase();
        LOG(ERROR)
            << "UnifiedCache::InsertBuffer: Failed to insert buffer into "
            << "SSD cache instance. Key: " << key
            << ", error: " << put_res.GetError()->GetMessage();
      }
      return put_res;
    }
    default:
      LOG(FATAL) << "UnifiedCache::InsertBuffer: Invalid cache instance type: "
                 << (int)type;
      return &Errors::kUnknownError;
  }
}

void UnifiedCache::AsyncInsertBuffer(CacheInstanceType type,
                                     CacheBufferSharedPtr buffer,
                                     const std::string_view& src) {
  DCHECK(type == CacheInstanceType::kDRAM || type == CacheInstanceType::kPMEM)
      << "Only DRAM & PMEM cache instance can AsyncInsertBuffer for now.";

  if (type == CacheInstanceType::kDRAM) {
    VLOG(2) << "AsyncInsertBuffer: attempt to insert cache buffer into DRAM "
               "CacheInstance, key="
            << buffer->Key() << ", value size: " << buffer->Size();
    DCHECK(dram_instance_ != nullptr) << "DRAM instances is null.";
    dram_puts_counter_->Increase();
    dram_instance_->AsyncPut(std::move(buffer), src);
  } else {
    VLOG(2) << "AsyncInsertBuffer: attempt to insert cache buffer into PMEM "
               "CacheInstance, key="
            << buffer->Key() << ", value size: " << buffer->Size();
    DCHECK(pmem_instance_ != nullptr) << "PMEM instances is null.";
    // This AsyncPut is caused by DRAM cache eviction or insert PMEM in
    // SideBySide mode.
    // For now, Contents of cache items with the same key must be the same.
    // Hence, we can skip insert them into PMEM cache
    auto res = pmem_instance_->GetBypassReplacementPolicy(buffer->Key());
    if (res.get() && res->Data() != nullptr) {
      VLOG(2) << "AsyncInsert-DRAM entry already exists in PMEM, "
                 "skip the insert, key = "
              << buffer->Key();
      res.reset();
      return;
    }
    VLOG(2) << "AsyncInsertBuffer: this cache buffer does not yet exist "
               "in the PMEM cache instance, key="
            << buffer->Key() << ", value size: " << buffer->Size();
    pmem_puts_counter_->Increase();
    pmem_instance_->AsyncPut(std::move(buffer), src);
  }
}

void UnifiedCache::Insert(const Key& key, Value value, size_t size) {
  DCHECK(initialized_);
  VLOG(1) << "Insert: key: " << key << ", value size: " << size
          << ", cache type: " << cache_metric_id_prefix_;
  unified_puts_counter_->Increase();
  if (UNLIKELY(cache_ssd_instance_only_)) {
    if (!ssd_cache_instance_) {
      LOG(FATAL)
          << "UnifiedCache::Insert: SSD only mode enabled but SSD instance "
          << "has not been initialized!";
      return;
    }
    InsertBuffer(CacheInstanceType::kSSD, key, std::move(value), size);
  } else {
    InsertIntoDramAndPmem(key, std::move(value), size);
    // Send a Put access record if the callback is registered
    if (access_record_cb_) {
      access_record_cb_->OnAccess(AccessRecordType::kPut, key);
    }
  }
}

std::optional<UnifiedCache::Value> UnifiedCache::Lookup(const Key& key) {
  DCHECK(initialized_);
  // This method is not friendly to the Value type (Slice) of UnifiedCache.
  // Slice does not have ownership to the innerdata, thus lookup result
  // returned from this method may either cause memory leak or invalid
  // memory access.
  // Callers of unified cache should not use this method.
  LOG(FATAL) << "UnifiedCache::Lookup: Unsupported operation to return a Slice"
             << " by value!";
  return {};
}

void UnifiedCache::Remove(const Key& key) {
  DCHECK(initialized_);
  unified_deletes_counter_->Increase();
  VLOG(1) << "Remove: delete DRAM/PMEM/SSD cache buffer with key: " << key
          << ", cache type: " << cache_metric_id_prefix_;

  if (ssd_cache_instance_) {
    VLOG(10) << "Remove - Delete SSD";
    ssd_cache_instance_->Delete(key);
  }

  if (UNLIKELY(cache_ssd_instance_only_)) {
    return;
  }

  VLOG(10) << "Remove - Delete DRAM";
  dram_instance_->Delete(key);
  if (pmem_instance_) {
    VLOG(10) << "Remove - Delete PMEM";
    pmem_instance_->Delete(key);
  }

  // Send a Delete access record if the callback is registered
  if (access_record_cb_) {
    access_record_cb_->OnAccess(AccessRecordType::kDelete, key);
  }
}

noodle::Result<CacheBufferSharedPtr, CacheError> UnifiedCache::TEST_Insert(
    CacheInstanceType type, const Key& key, Value value, size_t size) {
  return InsertBuffer(type, key, std::move(value), size);
}

UnifiedCache::Handle* UnifiedCache::TEST_Acquire(CacheInstanceType type,
                                                 const Key& key) {
  auto cache_buf = Lookup(type, key);
  if (cache_buf != nullptr) {
    return new CacheHandle(std::move(cache_buf));
  } else {
    return nullptr;
  }
}

void UnifiedCache::TEST_Remove(CacheInstanceType type, const Key& key) {
  DCHECK(initialized_);
  switch (type) {
    case CacheInstanceType::kDRAM:
      dram_instance_->Delete(key);
      break;
    case CacheInstanceType::kPMEM:
      if (pmem_instance_) {
        pmem_instance_->Delete(key);
      }
      break;
    case CacheInstanceType::kSSD:
      if (ssd_cache_instance_) {
        ssd_cache_instance_->Delete(key);
      }
      break;
    default:
      LOG(FATAL) << "UnifiedCache::TEST_Remove: Invalid cache instance type: "
                 << (int)type;
  }
}

void UnifiedCache::RemoveAll() {
  DCHECK(initialized_);
  VLOG(1) << "RemoveAll: remove all entries from DRAM/PMEM/SSD "
          << "Cache Instances."
          << ", cache type: " << cache_metric_id_prefix_;
  // NOTE: this step could take a long time!
  if (ssd_cache_instance_) {
    auto ssd_reset_res = ssd_cache_instance_->Reset();
    if (!ssd_reset_res.IsOk()) {
      LOG(ERROR)
          << "UnifiedCache::RemoveAll(): Failed to reset ssd cache instance!"
          << " Error is: " << ssd_reset_res.GetError()->GetMessage();
      initialized_ = false;
    }
  }
  if (UNLIKELY(cache_ssd_instance_only_)) {
    return;
  }

  auto reset_res = dram_instance_->Reset();
  if (!reset_res.IsOk()) {
    LOG(ERROR)
        << "UnifiedCache::RemoveAll(): Failed to reset dram cache instance!"
        << " Error is: " << reset_res.GetError()->GetMessage();
    initialized_ = false;
  }
  if (pmem_instance_) {
    reset_res = pmem_instance_->Reset();
    if (!reset_res.IsOk()) {
      LOG(ERROR)
          << "UnifiedCache::RemoveAll(): Failed to reset pmem cache instance!"
          << " Error is: " << reset_res.GetError()->GetMessage();
      initialized_ = false;
    }
  }
}

size_t UnifiedCache::Capacity() const {
  // If SideBySide data placement is used, return the combined capacity of DRAM
  // and PMEM instances if the combined value is greater than SSD capacity
  if (placement_type_ == DRAMPMEMDataPlacementType::kSideBySide) {
    if (dram_capacity_ + pmem_capacity_ > ssd_capacity_) {
      return dram_capacity_ + pmem_capacity_;
    } else {
      return ssd_capacity_;
    }
  } else {
    // For tiered data placement, the largest capacity of the three should be
    // returned.
    return 0;
    // return std::max(dram_capacity_, pmem_capacity_, ssd_capacity_);
  }
}

void UnifiedCache::SetCapacity(size_t capacity) {
  // This method is amgiguous for unified cache and should not be used at all.
  LOG(FATAL)
      << "UnifiedCache::SetCapacity: This method is ambiguous. Please use the"
      << " other SetCapacity() method!";
}

size_t UnifiedCache::Size() const {
  // If SideBySide data placement is used, return the larger value of DRAM +
  // PMEM usage and SSD usage
  size_t used = GetUsed(CacheInstanceType::kDRAM);
  if (pmem_instance_) {
    if (placement_type_ == DRAMPMEMDataPlacementType::kSideBySide) {
      used += GetUsed(CacheInstanceType::kPMEM);
    } else {
      used = std::max(GetUsed(CacheInstanceType::kPMEM), used);
    }
  }
  if (ssd_cache_instance_) {
    auto ssd_used = GetUsed(CacheInstanceType::kSSD);
    if (ssd_used > used) {
      used = ssd_used;
    }
  }
  return used;
}

std::unique_ptr<noodle::SummarySnapshot>
UnifiedCache::GetLookupLatencySummarySnapshot() {
  CHECK(initialized_);
  if (FLAGS_cache_collect_latency_summary) {
    return unified_acquire_latency_->GetSnapshot();
  }
  return nullptr;
}

std::unique_ptr<noodle::SummarySnapshot>
UnifiedCache::GetInstanceLookupLatencySummarySnapshot(CacheInstanceType type) {
  CHECK(initialized_);
  if (type == CacheInstanceType::kDRAM) {
    return unified_dram_lookup_latency_->GetSnapshot();
  } else if (type == CacheInstanceType::kPMEM) {
    return unified_pmem_lookup_latency_->GetSnapshot();
  } else if (type == CacheInstanceType::kSSD) {
    return unified_ssd_lookup_latency_->GetSnapshot();
  } else {
    LOG(WARNING) << "Pass wrong CacheInstanceType";
  }
  return nullptr;
}

UnifiedCache::Handle* UnifiedCache::Acquire(const Key& key) {
  DCHECK(initialized_);
  if (UNLIKELY(FLAGS_cache_collect_latency_summary)) {
    auto timer = unified_acquire_latency_->NewTimer();
    return AcquireImpl(key);
  } else {
    return AcquireImpl(key);
  }
}

CacheBufferSharedPtr UnifiedCache::AcquireLookUpImpl(CacheInstanceType type,
                                                     const Key& key) {
  CHECK(initialized_);
  if (UNLIKELY(FLAGS_cache_collect_latency_summary)) {
    if (type == CacheInstanceType::kDRAM) {
      auto timer = unified_dram_lookup_latency_->NewTimer();
      return Lookup(type, key);
    } else if (type == CacheInstanceType::kPMEM) {
      auto timer = unified_pmem_lookup_latency_->NewTimer();
      return Lookup(type, key);
    } else if (type == CacheInstanceType::kSSD) {
      auto timer = unified_ssd_lookup_latency_->NewTimer();
      return Lookup(type, key);
    } else {
      LOG(WARNING) << "Pass wrong CacheInstanceType";
      return nullptr;
    }
  } else {
    return Lookup(type, key);
  }
}

void UnifiedCache::AcquireAsyncSsdToMemPromotion(CacheBufferSharedPtr buf) {
  if (placement_type_ == DRAMPMEMDataPlacementType::kSideBySide &&
      pmem_instance_ != nullptr && buf->Size() > data_placement_threshold_) {
    AsyncInsertBuffer(CacheInstanceType::kPMEM, std::move(buf), "[SsdToPmem]");
  } else {
    AsyncInsertBuffer(CacheInstanceType::kDRAM, std::move(buf), "[SsdToDram]");
  }
}

// Real implementation of the ZeroCopyCache::Acquire interface
UnifiedCache::Handle* UnifiedCache::AcquireImpl(const Key& key) {
  unified_acquires_counter_->Increase();
  UnifiedCache::Handle* handle = nullptr;

  // Send a Get access record if the callback is registered
  if (access_record_cb_) {
    access_record_cb_->OnAccess(AccessRecordType::kGet, key);
  }

  if (UNLIKELY(cache_ssd_instance_only_)) {
    if (!ssd_cache_instance_) {
      LOG(FATAL)
          << "UnifiedCache::AcquireImpl: SSD instance only mode enabled but "
          << "SSD instance has not been initalized!";
      return handle;
    }
    auto ssd_buf = Lookup(CacheInstanceType::kSSD, key);
    if (ssd_buf != nullptr) {
      unified_hits_counter_->Increase();
      handle = new CacheHandle(std::move(ssd_buf));
    } else {
      unified_misses_counter_->Increase();
    }
    return handle;
  }

  // Look up in DRAM instance and return immediately if found the buffer
  auto res_buf = AcquireLookUpImpl(CacheInstanceType::kDRAM, key);
  if (res_buf == nullptr && pmem_instance_) {
    // If a buffer is found in PMEM but not DRAM, insert the buffer into DRAM
    // if the placement_type is kTiered, then return the handle
    res_buf = AcquireLookUpImpl(CacheInstanceType::kPMEM, key);
    if (FLAGS_mtcache_enable_pmem_promotion &&
        placement_type_ == DRAMPMEMDataPlacementType::kTiered &&
        res_buf != nullptr) {
      AsyncInsertBuffer(CacheInstanceType::kDRAM, res_buf, "[PmemToDram]");
    }
  }

  // If the buffer is not found in DRAM and PMEM, we still need to look it up in
  // the SSD instance and insert it into DRAM and(or) PMEM cache instances.
  if (res_buf == nullptr && ssd_cache_instance_) {
    res_buf = AcquireLookUpImpl(CacheInstanceType::kSSD, key);
    // Because we insert cache entries to the SSD before it actually reaches
    // SSD. These entires still residues in DRAM/PMEM physically and cannot be
    // promoted.
    if (FLAGS_mtcache_enable_ssd_promotion && res_buf != nullptr &&
        (dynamic_cast<StringBufferPtr>(res_buf.get()))) {
      AcquireAsyncSsdToMemPromotion(res_buf);
    }
  }

  if (res_buf != nullptr) {
    unified_hits_counter_->Increase();
    VLOG(1) << "AcquireImpl: found entry. Key: " << key
            << ", value size: " << res_buf->Size()
            << ", cache type: " << cache_metric_id_prefix_;
    handle = new CacheHandle(std::move(res_buf));
  } else {
    VLOG(1) << "AcquireImpl: couldn't find entry. Key: " << key
            << ", cache type: " << cache_metric_id_prefix_;
    unified_misses_counter_->Increase();
  }

  return handle;
}

void UnifiedCache::Release(ZeroCopyCache::Handle* handle) {
  if (handle) {
    VLOG(2) << "Releasing CacheHandle " << reinterpret_cast<void*>(handle)
            << ", key=" << handle->key();
    delete handle;
  }
}

UnifiedCache::Handle* UnifiedCache::InsertPinned(const UnifiedCache::Key& key,
                                                 UnifiedCache::Value value,
                                                 size_t size) {
  DCHECK(initialized_);
  unified_insert_pinned_counter_->Increase();
  UnifiedCache::Handle* handle = nullptr;
  if (UNLIKELY(cache_ssd_instance_only_)) {
    if (!ssd_cache_instance_) {
      LOG(FATAL) << "UnifiedCache::InsertPinned: SSD only mode enabled but SSD "
                 << "instance has not been initalized!";
      return handle;
    }
    auto insert_result =
        InsertBuffer(CacheInstanceType::kSSD, key, value, size);
    if (!insert_result.IsOk()) {
      VLOG(1) << "UnifiedCache::InsertPinned: Failed to insert entry into SSD "
              << "instance! Key: " << key << ", value size: " << size
              << ", cache type: " << cache_metric_id_prefix_
              << ", error: " << insert_result.GetError()->GetMessage();
      return handle;
    }
    // Just return the iobuf from user input value so that we can save
    // a GET request to ssd.
    auto iobuf_buf = std::make_shared<IOBufBuffer>(std::move(value));
    iobuf_buf->SetKey(key);
    return new CacheHandle(std::move(iobuf_buf));
  } else {
    if (access_record_cb_) {
      access_record_cb_->OnAccess(AccessRecordType::kPut, key);
    }
    auto insert_result = InsertIntoDramAndPmem(key, std::move(value), size);
    if (!insert_result.IsOk()) {
      VLOG(1) << "InsertPinned: Failed to insert into dram/pmem. error: "
              << insert_result.GetError()->GetMessage() << " Key: " << key
              << ", value size: " << size
              << ", cache type: " << cache_metric_id_prefix_;
      return nullptr;
    }
    // handle will be null only if the system ran out of memory and a
    // std::bad_alloc exception will be thrown.
    handle = new CacheHandle(std::move(insert_result).Get());
  }
  VLOG(2) << "InsertPinned: Successfully inserted and looked "
          << "up buffer with key: " << key
          << ", value size: " << handle->value().length()
          << ", cache type: " << cache_metric_id_prefix_;
  return handle;
}

noodle::Result<CacheBufferSharedPtr, CacheError>
UnifiedCache::InsertIntoDramAndPmem(const Key& key, Value value, size_t size) {
  if (placement_type_ == DRAMPMEMDataPlacementType::kSideBySide) {
    // If PMEM cache instance is unavailable, insert into DRAM instance
    // regardless of the value size; otherwise, insert into either DRAM or
    // PMEM cache instance based on value size.
    if (size <= data_placement_threshold_ || !pmem_instance_) {
      VLOG(2) << "InsertIntoDramAndPmem: use SideBySide data placement, "
                 "data_placement_threshold_ = "
              << data_placement_threshold_
              << ", Insert to DRAM CacheInstance, key=" << key
              << ", value size: " << size;
      return InsertBuffer(CacheInstanceType::kDRAM, key, std::move(value),
                          size);
    } else {
      VLOG(2) << "InsertIntoDramAndPmem: use SideBySide data placement, "
                 "data_placement_threshold_ = "
              << data_placement_threshold_
              << ", Insert to PMEM CacheInstance, key=" << key
              << ", value size: " << size;
      if (FLAGS_cache_pmem_enable_async_write) {
        auto iobuf_buf = std::make_shared<IOBufBuffer>(std::move(value));
        iobuf_buf->SetKey(key);
        AsyncInsertBuffer(CacheInstanceType::kPMEM, iobuf_buf,
                          "[InsertIntoDramAndPmem SideBySide]");
        return std::static_pointer_cast<CacheBuffer>(iobuf_buf);
      } else {
        VLOG(2) << "InsertIntoDramAndPmem: Insert to PMEM CacheInstance "
                   "synchronously."
                << " key= " << key << ", value size: " << size;
        return InsertBuffer(CacheInstanceType::kPMEM, key, std::move(value),
                            size);
      }
    }
  } else {
    // If Tiered data placement mode is used, always insert into DRAM cache.
    // We do not insert the key-value into PMEM cache here and let the eviction
    // handler to insert evicted cache buffer from DRAM cache into PMEM cache.
    VLOG(2) << "InsertIntoDramAndPmem: use Tiered data placement & insert to "
               "DRAM CacheInstance, key="
            << key << ", value size: " << size;
    auto res = InsertBuffer(CacheInstanceType::kDRAM, key, value, size);
    if (!res.IsOk()) {
      // Fail to insert into dram cache, maybe dram cache is out of space.
      // Fall back to insert the data into pmem cache.
      auto iobuf_buf = std::make_shared<IOBufBuffer>(std::move(value));
      iobuf_buf->SetKey(key);
      AsyncInsertBuffer(CacheInstanceType::kPMEM, iobuf_buf,
                        "[InsertIntoPmem Tiered]");
      res = std::static_pointer_cast<CacheBuffer>(iobuf_buf);
    }
    return res;
  }
}

CacheBufferSharedPtr UnifiedCache::Lookup(CacheInstanceType type,
                                          const Key& key) {
  DCHECK(initialized_);
  CacheBufferSharedPtr res_buf(nullptr);
  noodle::Result<CacheBufferSharedPtr, CacheError> get_result(
      &Errors::kCacheBufferNotFound);
  switch (type) {
    case CacheInstanceType::kDRAM: {
      get_result = dram_instance_->Get(key);
      if (get_result.IsOk()) {
        dram_hits_counter_->Increase();
      } else {
        dram_misses_counter_->Increase();
      }
      break;
    }
    case CacheInstanceType::kPMEM: {
      if (pmem_instance_) {
        get_result = pmem_instance_->Get(key);
        if (get_result.IsOk()) {
          pmem_hits_counter_->Increase();
        } else {
          pmem_misses_counter_->Increase();
        }
      }
      break;
    }
    case CacheInstanceType::kSSD: {
      if (ssd_cache_instance_) {
        get_result = ssd_cache_instance_->Get(key);
        if (get_result.IsOk()) {
          ssd_hits_counter_->Increase();
        } else {
          ssd_misses_counter_->Increase();
        }
      }
      break;
    }
    default: {
      LOG(FATAL) << "UnifiedCache::Lookup: Invalid cache instance type: "
                 << ToString(type);
    }
  }

  if (get_result.IsOk()) {
    res_buf = std::move(get_result).Get();
    VLOG(2) << "Lookup: found buffer in " << ToString(type) << ", key: " << key
            << ", cache type: " << cache_metric_id_prefix_;
  } else if (get_result.GetError() == &Errors::kCacheBufferNotFound) {
    VLOG(2) << "Lookup: failed to find buffer in " << ToString(type)
            << ", key: " << key << ", cache type: " << cache_metric_id_prefix_;
  } else {
    LOG(ERROR) << "UnifiedCache::Lookup: in " << ToString(type)
               << " failed with error: " << get_result.GetError()->GetMessage()
               << ", cache type: " << cache_metric_id_prefix_;
  }

  return res_buf;
}

void UnifiedCache::RegisterEvictionHandler() {
  DCHECK(initialized_);
  // dram eviction handler
  dram_instance_->RegisterEvictionHandler([&](CacheBufferSharedPtr buffer) {
    noodle::Result<void, CacheError> insert_result;
    if (placement_type_ == DRAMPMEMDataPlacementType::kTiered &&
        pmem_instance_ != nullptr) {
      if (FLAGS_cache_pmem_enable_async_write) {
        VLOG(2) << "EvictionHandler: DRAM instance evict buffer into PMEM "
                   "instance asynchronously."
                << " key: " << buffer->Key()
                << ", value size: " << buffer->Size();
        AsyncInsertBuffer(CacheInstanceType::kPMEM, std::move(buffer),
                          "[DRAM eviction handler]");
      } else {
        VLOG(2) << "EvictionHandler: DRAM instance evict buffer into PMEM "
                   "instance synchronously."
                << " key: " << buffer->Key()
                << ", value size: " << buffer->Size();
        auto value =
            folly::IOBuf::wrapBufferAsValue(buffer->Data(), buffer->Size());
        InsertBuffer(CacheInstanceType::kPMEM, buffer->Key(), std::move(value),
                     buffer->Size());
      }
    } else if (ssd_cache_instance_) {
      // Check if the item already exist in SSD
      auto ret = ssd_cache_instance_->GetBypassReplacementPolicy(buffer->Key());
      if ((ret.get() == nullptr) && (l2_cache_policy_)) {
        l2_cache_policy_->OnEvict(std::move(buffer));
      }
    }
  });
  // pmem eviction handler
  if (pmem_instance_) {
    pmem_instance_->RegisterEvictionHandler([&](CacheBufferSharedPtr buffer) {
      if (ssd_cache_instance_) {
        auto ret = ssd_cache_instance_->GetBypassReplacementPolicy(buffer->Key());
        if ((ret.get() == nullptr) && (l2_cache_policy_)) {
          l2_cache_policy_->OnEvict(std::move(buffer));
        }
      }
    });
  }
}

void UnifiedCache::RegisterEvictionMetrics() {
  CHECK(initialized_);
  dram_instance_->RegisterEvictionMetricHandler(
      [this](size_t num) { this->dram_evicts_counter_->Increase(num); });
  if (pmem_instance_) {
    pmem_instance_->RegisterEvictionMetricHandler(
        [this](size_t num) { this->pmem_evicts_counter_->Increase(num); });
  }
  if (ssd_cache_instance_) {
    ssd_cache_instance_->RegisterEvictionMetricHandler(
        [this](size_t num) { this->ssd_evicts_counter_->Increase(num); });
  }
}

void UnifiedCache::DeregisterEvictionHandler() {
  DCHECK(initialized_);
  dram_instance_->RegisterEvictionHandler(nullptr);
}

const std::string UnifiedCache::ToString(CacheInstanceType type) {
  switch (type) {
    case CacheInstanceType::kDRAM:
      return "DRAM Instance";
    case CacheInstanceType::kPMEM:
      return "PMEM Instance";
    case CacheInstanceType::kSSD:
      return "SSD Instance";
    case CacheInstanceType::kUnified:
      return "Unified Cache";
    default:
      return "Unknown Instance";
  }
}

}  // namespace mtcache
