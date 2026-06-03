#pragma once

#include "mtcache.h"
#include "policy/access_record_callback.h"
#include "policy/l2_policy.h"

namespace mtcache {

// UnifiedCache implements the ZeroCopyCache interface, for applications
// to load and retrieve cache data in a zerocopy manner. It consists of
// two cache instances: one instance with a DRAM storage engine, and one
// instance with a PMEM storage engine.
class UnifiedCache : public ZeroCopyCache<std::string, folly::IOBuf> {
 public:
  using Key = std::string;
  using Value = folly::IOBuf;

  class CacheHandle final : public ZeroCopyCache::Handle {
   public:
    CacheHandle(CacheBufferSharedPtr buffer);
    // Disable copy constructor and copy assignment operator
    CacheHandle(const CacheHandle& other) = delete;
    CacheHandle& operator=(const CacheHandle& other) = delete;
    ~CacheHandle() = default;

    const Key& key() const override { return buffer_->Key(); }

    const Value& value() const override { return value_; }

    Handle* Clone() const override;

    // Only used for data promotion
    const CacheBufferSharedPtr Buffer() const { return buffer_; }

   private:
    // buffer_ is a pointer to the cache buffer returned by Cache Instances.
    CacheBufferSharedPtr buffer_;

    // The IOBuf of this cache record.
    Value value_;
  };

  // RegisterAccessRecordCallback registers a callback to receive access
  // records.
  void RegisterAccessRecordCallback(AccessRecordCallback* cb) {
    access_record_cb_ = cb;
  }

  // DRAMPMEMDataPlacementType defines how a cache buffer is placed into DRAM
  // and/or PMEM cache instances when there is an acquire operation and it is
  // found in the SSD instance.
  enum class DRAMPMEMDataPlacementType : uint8_t {
    // kSideBySide means a cache buffer should be populated into either DRAM, or
    // PMEM instance, depending on whether the value size exceeds the specified
    // threshold.
    kSideBySide = 0,
    // kTiered means a cache buffer should be populated into the DRAM cache
    // instance and may be evicted to PMEM instance if the eviction handler is
    // enabled.
    kTiered = 1,
    kMaxCode = 2
  };

  // CacheInstanceType is an enum defition to specify the cache instance that an
  // API call should apply to.
  enum class CacheInstanceType : uint8_t {
    kDRAM = 0,    // kDRAM refers to the DRAM cache instance
    kPMEM = 1,    // kPMEM refers to the PMEM cache instance
    kSSD = 2,     // kSSD refers to the SSD cache instance
    kUnified = 3  // kUnified refers to the unified cache itself
  };

  // Construct an UnifiedCache object.
  UnifiedCache(const CacheOptions& opts);

  // Destructor first stops member cache instances, then destruct them by
  // resetting the unique pointers to them.
  ~UnifiedCache();

  // Setters and Getters

  // SetReplacementPolicyType sets the replacement policy of cache instance with
  // type instance_type to replacement_type. The replacement policy of member
  // cache instances cannot be changed after they are started.
  void SetReplacementPolicyType(CacheInstanceType instance_type,
                                ReplacementPolicyType replacement_type);

  // GetReplacementPolicyType returns the replacement policy of the cache
  // instance with specified cache instance type.
  ReplacementPolicyType GetReplacementPolicyType(CacheInstanceType type) const;

  // SetCapacity updates the capacity of the cache instance with the specified
  // type to capacity.
  void SetCapacity(CacheInstanceType type, size_t capacity);

  // GetCapacity returns the capacity of the cache instance with the specified
  // type.
  size_t GetCapacity(CacheInstanceType type) const;

  // GetUsed method returns the used space of the cache instance with the
  // specified type. Please note that this method could only been called after
  // the unified cache is started, or CHECK(initialized_) fails.
  size_t GetUsed(CacheInstanceType type) const;

  // SetDataPlacementType sets the data placement type/policy between DRAM and
  // PMEM instances to type (kSideBySide or kTiered). The updated data placement
  // type only impacts new CacheBuffers that will be pulled into DRAM/PMEM cache
  // instances. Existing CacheBuffers in DRAM/PMEM are unaffected.
  void SetDataPlacementType(DRAMPMEMDataPlacementType type) {
    placement_type_ = type;
  }

  // GetDataPlacementType returns the data placement type between DRAM and PMEM
  // cache instance.
  DRAMPMEMDataPlacementType GetDataPlacementType() const {
    return placement_type_;
  }

  // SetDataPlacementThreshold updates the data placement threshold to the new
  // threshold. The new threshold has impact on newly buffers that are pulled in
  // DRAM and PMEM instances. If the data placement type is kSideBySide, cache
  // buffers whose value is smaller than the threshold are pulled into the DRAM
  // instance; other buffers are pulled into the PMEM instance.
  void SetDataPlacementThreshold(size_t threshold) {
    data_placement_threshold_ = threshold;
  }

  // GetDataPlacementThreshold returns the data placement threshold.
  size_t GetDataPlacementThreshold() const { return data_placement_threshold_; }

  // Copy/move constructors, and copy/move assignment operators are disabled
  UnifiedCache(const UnifiedCache& other) = delete;
  UnifiedCache(UnifiedCache&& other) = delete;

  UnifiedCache& operator=(const UnifiedCache& other) = delete;
  UnifiedCache& operator=(UnifiedCache&& other) = delete;

  // This method should ONLY used by unit tests.
  L2CachePolicy* l2_cache_policy() { return l2_cache_policy_.get(); }

  // Cache interfaces
  bool Start() override;

  bool Stop() override;

  void Insert(const Key& key, Value value, size_t size) override;

  std::optional<Value> Lookup(const Key& key) override;

  void Remove(const Key& key) override;

  void RemoveAll() override;

  size_t Capacity() const override;

  void SetCapacity(size_t capacity) override;

  size_t Size() const override;

  // ZeroCopyCache interfaces
  Handle* Acquire(const Key& key) override;

  void Release(ZeroCopyCache::Handle* handle) override;

  Handle* InsertPinned(const Key& key, Value value, size_t size) override;

  // TEST_Insert method tries to insert a cache buffer (key, value) into cache
  // instance with specified type.
  // This method should ONLY be used by unit tests.
  // The returned error from CacheInstance::Put() is returned is an error
  // occurred; otherwise, a shared pointer to the inserted cache buffer is
  // returned.
  noodle::Result<CacheBufferSharedPtr, CacheError> TEST_Insert(
      CacheInstanceType type, const Key& key, Value value, size_t size);

  // TEST_Acquire method tries to acquire a cache handle with specified key
  // from cache instance with specified type.
  // This method should ONLY be used by unit tests.
  // If a cache buffer is found, a CacheHandle is returned; otherwise, a
  // nullptr is returned.
  Handle* TEST_Acquire(CacheInstanceType type, const Key& key);

  // TEST_Remove method removes a cache buffer of specified key from the cache
  // instance of specified type. Please note that if eviction handlers are
  // enabled, a buffer removed from the specified instance may be evicted into
  // another member instance.
  void TEST_Remove(CacheInstanceType type, const Key& key);

  // Get the metric counter of 'UnifiedCacheAcquireCount'.
  // For unit test only.
  uint64_t TEST_GetUnifiedAcquireCount() {
    return unified_acquires_counter_->GetValue();
  }

  // Get the metric counter of 'UnifiedCachePutCount'.
  // For unit test only.
  uint64_t TEST_GetUnifiedPutCount() {
    return unified_puts_counter_->GetValue();
  }

  // Get the metric counter of 'UnifiedCacheInsertPinnedCount'.
  // For unit test only.
  uint64_t TEST_GetUnifiedInsertPinnedCount() {
    return unified_insert_pinned_counter_->GetValue();
  }

  // Join the PMEM writer executor(s) so that all writing-pmem tasks complete.
  // For test or benchmark only.
  void TEST_JoinPmemWriteExecutor();

  // RegisterEvictionHandler registers an eviction handler to the DRAM cache
  // instance.
  void RegisterEvictionHandler();

  // DeregisterEvictionHandler deregisters the eviction handler from the DRAM
  // cache instance.
  void DeregisterEvictionHandler();

  // GetLookupLatencySummarySnapshot returns a noodle::SummarySnapshot of lookup
  // latency for unified cache. A empty unique_ptr is returned if lookup latency
  // collection is disabled.
  std::unique_ptr<noodle::SummarySnapshot> GetLookupLatencySummarySnapshot();
  std::unique_ptr<noodle::SummarySnapshot>
  GetInstanceLookupLatencySummarySnapshot(CacheInstanceType type);

  // Get the vector of PMEM paths, for unit test only.
  std::vector<std::string> TEST_GetPmemPaths() { return pmem_paths_; }

  // Disable the policy eviction handler with a empty function
  void DisablePolicyMemEvictionHandler() {
    if (dram_instance_) {
      dram_instance_->RegisterPolicyMemEvictionHandler({});
    }
    if (pmem_instance_) {
      pmem_instance_->RegisterPolicyMemEvictionHandler({});
    }
  }

 private:
  // ToString casts the enum CacheInstanceType type into a string for logging
  // purpose.
  const std::string ToString(CacheInstanceType type);
  // RegisterComponentMetrics registers the cache operation (put/get/delete)
  // metrics for a member cache instance (dram/pmem/ssd), or the unified cache
  // itself, depending on the specified cache instance type.
  void RegisterComponentMetrics(CacheInstanceType type);

  // RegisterEvictionMetrics registers the eviction related metrics to
  // corresponding cache instances. Note that eviction metrics are independent
  // from EvictionHandler. They are updated even when eviction handlers are
  // disabled.
  void RegisterEvictionMetrics();

  // InsertIntoDramAndPmem is an internal method to insert a cache buffer (key,
  // value) into DRAM, PMEM or both cache instances, depending on the data
  // placement type. If the insert is successful, a shared pointer to the cache
  // buffer of the inserted buffer is returned; Otherwise, the error returned by
  // InsertBuffer is returned.
  //
  // @param [in] key
  //   The key of the cache buffer to be inserted
  // @param [in] value
  //   The value of the cache buffer to be inserted
  // @param [in] size
  //   The value size of the cache buffer to be inserted
  noodle::Result<CacheBufferSharedPtr, CacheError> InsertIntoDramAndPmem(
      const Key& key, Value value, size_t size);

  // Lookup is an internal method to find a cache buffer of specified key from
  // the specified cache instance. If a buffer is found, a shared_ptr of
  // CacheBuffer is returned; otherwise, a nullptr is returned.
  CacheBufferSharedPtr Lookup(CacheInstanceType type, const Key& key);

  // Insert is an internal method that inserts a cache buffer (key, value) into
  // the cache instance of specified type. The returned error from
  // CacheInstance::Put() is returned is an error occured; otherwise, a shared
  // pointer to the cache buffer of the inserted buffer is returned.
  noodle::Result<CacheBufferSharedPtr, CacheError> InsertBuffer(
      CacheInstanceType type, const Key& key, Value value, size_t size);

  // Mostly like InsertBuffer but in an asynchronous way. The parameter `src`
  // indicates where this method is called (useful in log/debug messages).
  void AsyncInsertBuffer(CacheInstanceType type, CacheBufferSharedPtr buffer,
                         const std::string_view& src);

  // AcquireImpl is the real implementation of the ZeroCopyCache::Acquire
  // interface. UnifiedCache::Acquire is a wrapper of this method with optional
  // query latency collection.
  Handle* AcquireImpl(const Key& key);

  CacheBufferSharedPtr AcquireLookUpImpl(CacheInstanceType type,
                                         const Key& key);
  void AcquireAsyncSsdToMemPromotion(CacheBufferSharedPtr buffer);

  // dram_instance_ is the member DRAM cache instance.
  std::unique_ptr<CacheInstance> dram_instance_;

  // pmem_instance_ is the member PMEM cache instance.
  std::unique_ptr<CacheInstance> pmem_instance_;

  // ssd_cache_instance_ is the member SSD cache instance.
  std::unique_ptr<CacheInstance> ssd_cache_instance_;

  // An implementation of the l1 cache interface, used by the l2_cache_policy.
  // Since l1_cache_ itself does not hold cache instance ownership, in order to
  // simplify life cycle management, unified cache holds l1 cache.
  std::unique_ptr<L1CacheInterface> l1_cache_;

  // L2 Cache Policy  is responsible for migrating data from the different cache
  // instance
  std::unique_ptr<L2CachePolicy> l2_cache_policy_;

  // placement_type_ specifies the data placement type between DRAM and PMEM
  // cache instances.
  // The default value is kSideBySide.
  DRAMPMEMDataPlacementType placement_type_;

  // data_placement_threshold specifies the threshold on whether a cache buffer
  // should be pulled into DRAM or PMEM cache instance if side by side data
  // placement policy is used.
  // The default value is kDefaultDataPlacementThreshold (256).
  size_t data_placement_threshold_;

  // dram_capacity_ specifies the capacity of the DRAM cache instance.
  size_t dram_capacity_;

  // pmem_capacity_ specifies the capacity of the PMEM cache instance.
  size_t pmem_capacity_;

  // ssd_capacity_ specifies the capacity of the SSD cache instance.
  // using the same capacity for each SSD cache instance.
  // TODO(xiongmu): support different size for each SSD instance.
  size_t ssd_capacity_;

  std::vector<std::string> pmem_paths_;

  std::vector<std::string> ssd_paths_;

  // dram_replacement_type_ specifies the replacement policy of the DRAM
  // cache instance.
  // The default value is SLRU.
  ReplacementPolicyType dram_replacement_type_;

  // pmem_replacement_type_ specifies the replacement policy of the PMEM
  // cache instance.
  // The default value is SLRU.
  ReplacementPolicyType pmem_replacement_type_;

  // ssd_replacement_type_ specifies the replacement policy of the SSD
  // cache instance.
  // The default value is LRU.
  ReplacementPolicyType ssd_replacement_type_;

  // initialized_ indicates whether the unified cache object has been
  // initialized.
  // The default value is false.
  bool initialized_;

  AccessRecordCallback* access_record_cb_;

  // Counter metrics for Get(Acquire), Put, and Delete operations in
  // unified cache and member cache instances.
  // Unified cache registry and related metrics
  std::shared_ptr<noodle::MetricRegistry> unified_registry_;
  std::shared_ptr<noodle::AtomicCounter> unified_acquires_counter_;
  std::shared_ptr<noodle::AtomicCounter> unified_hits_counter_;
  std::shared_ptr<noodle::AtomicCounter> unified_misses_counter_;
  std::shared_ptr<noodle::AtomicCounter> unified_puts_counter_;
  std::shared_ptr<noodle::AtomicCounter> unified_deletes_counter_;
  std::shared_ptr<noodle::AtomicCounter> unified_insert_pinned_counter_;
  // unified_acquire_latency is a metric to track the percentiles of query
  // latency to the unified cache in the configured window (e.g. 30 minutes)
  std::shared_ptr<noodle::SampleSetTimeSummary> unified_acquire_latency_;
  std::shared_ptr<noodle::SampleSetTimeSummary> unified_dram_lookup_latency_;
  std::shared_ptr<noodle::SampleSetTimeSummary> unified_pmem_lookup_latency_;
  std::shared_ptr<noodle::SampleSetTimeSummary> unified_ssd_lookup_latency_;
  // cache_start_time_counter_ is used to track the total time of all cache
  // instances start.
  std::shared_ptr<noodle::AtomicGauge> unified_start_time_counter_;

  // Dram cache instance registry and related metrics
  std::shared_ptr<noodle::MetricRegistry> dram_registry_;
  std::shared_ptr<noodle::AtomicCounter> dram_hits_counter_;
  std::shared_ptr<noodle::AtomicCounter> dram_misses_counter_;
  std::shared_ptr<noodle::AtomicCounter> dram_puts_counter_;
  std::shared_ptr<noodle::AtomicCounter> dram_evicts_counter_;

  // Pmem cache instance registry and related metrics
  std::shared_ptr<noodle::MetricRegistry> pmem_registry_;
  std::shared_ptr<noodle::AtomicCounter> pmem_hits_counter_;
  std::shared_ptr<noodle::AtomicCounter> pmem_misses_counter_;
  std::shared_ptr<noodle::AtomicCounter> pmem_puts_counter_;
  std::shared_ptr<noodle::AtomicCounter> pmem_evicts_counter_;

  // SSD cache instance registry and related metrics
  std::shared_ptr<noodle::MetricRegistry> ssd_registry_;
  std::shared_ptr<noodle::AtomicCounter> ssd_hits_counter_;
  std::shared_ptr<noodle::AtomicCounter> ssd_misses_counter_;
  std::shared_ptr<noodle::AtomicCounter> ssd_pulls_counter_;
  std::shared_ptr<noodle::AtomicCounter> ssd_evicts_counter_;

  // Zoned Store registry
  std::shared_ptr<noodle::MetricRegistry> zoned_store_registry_;

  // Error-related metric
  // dram_cache_insert_failed_counter_ is used to track the number of failed
  // inserts to dram cache instances
  std::shared_ptr<noodle::AtomicCounter> dram_cache_insert_failed_counter_;
  // pmem_cache_insert_failed_counter_ is used to track the number of failed
  // inserts to pmem cache instances
  std::shared_ptr<noodle::AtomicCounter> pmem_cache_insert_failed_counter_;
  // ssd_cache_insert_failed_counter_ is used to track the number of failed
  // inserts to ssd cache instances
  std::shared_ptr<noodle::AtomicCounter> ssd_cache_insert_failed_counter_;
  // dram_buffer_insert_failed_counter_ is used to track the number of failed
  // inserts to the temporary buffer in DRAM when using SideBySide data
  // placement
  std::shared_ptr<noodle::AtomicCounter>
      no_index_dram_buffer_insert_failed_counter_;

  std::string cache_metric_id_prefix_;

  std::map<std::string, std::string> metric_registry_tags_;

  bool cache_ssd_instance_only_{false};  // by default is false
};

}  // namespace mtcache
