#pragma once

#include "policy/l1_interface.h"
#include "policy/replacement_policy.h"
#include "storage/storage_engine.h"

#include <noodle/metric/metric_registry.h>

#include <cstddef>
#include <memory>

namespace mtcache {

// A CacheInstance is an instance that can be used to cache data.
//
// When an instance is constructed, the cache capacity, replacement policy,
// and storage engine type should be specified. CacheInstance doesn't store
// cached data in itself, nor does it implement any cache replacement policy.
// Instead, it depends the underlying storage engine and replacement policy
// to do so.

// CacheInstance provides a Put method to store an buffer into the cache, or
// update an buffer if it's already in the cache. If the cache is already full,
// the replacement policy would pick an existing buffer to evict. The evicted
// buffer could be feed into other Cache Instances if an eviction handler is
// installed.

// CacheInstance also provides a Get method to retrieve the value of a specified
// key.

// Multiple CacheInstances could be composed together to form a
// multi-tier/multi-component cache. Each instance MAY use its own storage
// engine and replacement policy.

class CacheInstance : public StorageEngine::RecoverDataCallback,
                      public StorageEngine::GCCopyCallback,
                      public L1CacheInterface {
 public:
  // Constructor: instantiate a CacheInstance using specified cache replacement
  // type, storage engine type, and capacity. Note that this 'capacity' is
  // usable capacity for user data, not the capacity for allocator. The 'path'
  // is the storage path which is useful only for pmem and ssd cache.
  CacheInstance(std::size_t capacity, ReplacementPolicyType replace_type,
                StorageEngineType storage_type, std::vector<std::string> paths);

  // Default Destructor.
  virtual ~CacheInstance();

  // RecoverDataCallback::OnRecoverData would insert the key, cachebuffer pairs
  // recovered by a storage engine into index and replacement policy
  void OnRecoverData(const std::string& key,
                     CacheBufferSharedPtr buffer) override;

  // This method is inherited from StorageEngine::GCCopyCallback::Update.
  // This method updates the buffer of the specified key to the
  // 'new_buffer'. The param 'key' is used to find the corresponding buffer
  // in the index layer. The param 'old_data_ptr' is used to check whether
  // the data pointer in the existing buffer is equal to 'old_data_ptr'.
  // If the CacheBuffer is found matches with 'old_data_ptr',
  // this method will atomically update the CacheBuffer.
  // If no CacheBuffer is found for the 'key',a kCacheBufferNotFound error is
  // returned.
  // If the CacheBuffer is found but 'old_data_ptr' does not match the data
  // pointer in CacheBuffer, a kCacheReplaceMismatch error is returned.
  noodle::Result<void, CacheError> Update(
      const std::string& key, const char* old_data_ptr,
      CacheBufferSharedPtr new_buffer) override;

  // Start method initiates the underlying replacement policy, and starts the
  // storage engine.
  // If replacement policy fails to initiate, a status with code
  // kReplacementPolicyUninitialized is returned; if storage engine fails to
  // start, a status with code kStorageEngineUninitialized is returned.
  // Otherwise, the following call to the returned value is true:
  // noodle::Result<void, CacheError>::IsOk().
  noodle::Result<void, CacheError> Start();

  // Stop method shuts down the storage engine and replacement policy. It may
  // also flush some meta data in memory into persistent storage (PMEM/SSD) to
  // help fast restart.
  // After calling this method, replacement policy and storage engine are
  // deleted automatically by the unique_ptr.
  noodle::Result<void, CacheError> Stop();

  // Put method stores the key-value pair into the cache instance. If the buffer
  // doesn't exist, it's added into the cache; Otherwise, the cached buffer is
  // updated to the new value. Callers that already hold a reference to the old
  // value will continue use the old value. The put method may call the
  // registered eviction handler to handle an buffer that is evicted after the
  // new buffer is stored. If the underlying replacement policy is not ready, a
  // kReplacementPolicyUninitialized error is returned; If the underlying
  // storage engine is not ready, a kStorageEngineUninitialized error is
  // returned. If the storage engine is full, a kStorageFull error is returned.
  // If there is a media access error in the storage engine, a
  // kStorageAccessError is returned. If the Put operation is successful, a
  // shared pointer to the cache buffer is returned.
  noodle::Result<CacheBufferSharedPtr, CacheError> Put(const std::string& key,
                                                       folly::IOBuf value);

  // Put the key-value pair to the cache instance asynchronously. This method
  // is almost the same as `Put` except it is in an asynchronous way.
  //
  // The parameter `value` is the cache buffer to be inserted. The parameter
  // `src` is the help message to indicate the caller that calls this method.
  // For example, the `src` may be "[InsertIntoPMEM side by side]",
  // "[DRAM eviction handler]", etc.
  //
  // This method returns a SemiFuture of the new-inserted cache buffer or error.
  // Usually the caller should ignore the returned SemiFuture, except he wants
  // to block until the async task is done by the worker thread (e.g. UT may
  // want to wait in this way).
  //
  // This method should only be used for PMEM instance. DRAM instance should
  // use `Put` instead.
  folly::SemiFuture<noodle::Result<CacheBufferSharedPtr, CacheError>> AsyncPut(
      CacheBufferSharedPtr value, const std::string_view& src);

  // Get method retrieves the value of the specified key. If the key is found, a
  // CacheBufferSharedPtr is returned. If the key couldn't be found, a
  // kCacheBufferNotFound error is returned, and the caller may check other
  // cache instances or cloud store to retrieve the data. If the key is found in
  // the index of cache instance, but the related data couldn't be found in the
  // storage engine, a kStorageBufferNotFound error is returned. This error
  // shouldn't happen normally, and the caller should at least log the error.
  // Similar to Put method, a kReplacementPolicyUninitialized or
  // kStorageEngineUninitialized error is returned if the underlying replacement
  // policy or storage engine is not ready, respectively.
  noodle::Result<CacheBufferSharedPtr, CacheError> Get(const std::string& key);

  // Similar to the Get method, GetBypassReplacementPolicy also retrieves the
  // value of the specified key. If the key is not found, a nullptr is returned.
  // Different from the Get method, GetBypassReplacementPolicy totally bypasses
  // the underlying replacement policy, and thus has NO impact on which cache
  // buffers should be evicted when the cache is full.
  CacheBufferSharedPtr GetBypassReplacementPolicy(
      const std::string& key) override;

  // If the key **definitely** does not exist in the storage, then this method
  // returns false, else true.
  // This check is potentially lighter-weight than invoking Get(). One way
  // to make this lighter weight is to avoid doing any IOs.
  bool Peek(const std::string& key);

  // Delete method deletes the cached buffer with the specified key. If the
  // buffer is found and successfully deleted, IsOk() is true in the returned
  // noodle::Result. Otherwise, a kCacheBufferNotFound error is returned.
  noodle::Result<void, CacheError> Delete(const std::string& key);

  // Reset method removes all cached buffers from the cache instance, and
  // resets all stats. If the reset operation is successful, IsOk() is true in
  // the returned noodle::Result. If the storage engine fails to reset the
  // storage media, a kStorageResetFailed error is returned. If the replacement
  // policy fails to reset, a kReplacementResetFailed error is returned. Upon
  // such failures, the cache may contain corrupted buffers, and the cache
  // instance should no longer been used.
  noodle::Result<void, CacheError> Reset();

  // RecoverData method may be called during initialization, to recover buffers
  // that were previously cached in persistent storage media. If the operation
  // is successful, IsOk() is true in the returned noodle::Result. If the
  // underlying storage engine is not persistent (e.g. DRAM), a kNotImplemented
  // error is returned. If the storage engine couldn't fully recover cached
  // data, a kStorageDataRecoverFailed error is returned. The cache instance
  // could either proceed with partially recovered data, or reset the cache to a
  // clean state.
  noodle::Result<void, CacheError> RecoverData();

  // RegisterEvictionHandler registers the EvictionHandler callback function.
  void RegisterEvictionHandler(
      std::function<void(CacheBufferSharedPtr)> callback) {
    eviction_cb_ = callback;
  }

  // RegisterEvictionMetricHandler registers the EvictionMetric handling
  // callback function.
  void RegisterEvictionMetricHandler(std::function<void(size_t)> callback) {
    eviction_metric_cb_ = callback;
  }

  // SetEvictionHandlerStatus enables/disables eviction handling of the cache
  // instance.
  void SetEvictionHandlerStatus(bool status) {
    eviction_handler_status_ = status;
  }

  // GetEvictionHandlerStatus returns whether eviction handler of the cache
  // instance is enabled or not.
  bool GetEvictionHandlerStatus() const { return eviction_handler_status_; }

  // GetCapacity returns the capacity of the cache instance
  std::size_t GetCapacity() const { return capacity_; }

  // SetCapacity updates the capacity of the cache instance
  void SetCapacity(size_t capacity);

  // GetUsedCapacity returns the used capacity of the cache instance
  // (including value size and key size).
  std::size_t GetUsedSpace() const;

  // GetItemNum returns the number of items of the cache instance. Use the
  // number of items in index, which may be smaller than the number in storage.
  std::size_t GetItemNum() const { return replacement_policy_->GetItemNum(); }

  // SetMetricRegistry sets the metric registry. This method is needed only
  // if the selected storage engine uses a log-based allocator.
  void SetMetricRegistry(std::shared_ptr<noodle::MetricRegistry> registry) {
    CHECK(!initialized_);
    metric_registry_ = registry;
  }

  // For unit test only
  StorageEngine* TEST_GetStorageEngine() { return storage_engine_.get(); }

  // Join the PMEM writer executor(s) so that all writing-pmem tasks complete.
  // For test or benchmark only.
  void TEST_JoinPmemWriteExecutor();

  // Add the cache item '*value' to the replacement policy only, but not the
  // associated storage engine.  This method should only be called as the
  // handler in another cache instance's (e.g., L1 cache) replacement policy.
  void PutBypassStorage(CacheBufferSharedPtr value);

  void RegisterPolicyMemEvictionHandler(
      std::function<void(CacheBufferSharedPtr)> func) {
    VLOG(0) << "RegisterPolicyMemEvictionHandler";
    replacement_policy_->RegisterMemEvictionHandler(func);
  }

 private:
  // HandleEvictedBuffers is a helper function that calls registered eviction
  // handlers to process evicted buffers if eviction handling is enabled.
  void HandleEvictedBuffers(std::vector<CacheBufferSharedPtr> evicted);

  void RegisterCacheInstanceMetrics();

  void PutCacheBufferToPolicy(CacheBufferSharedPtr value,
                              const std::string_view& description);

  // Convert capacity to allocator_capacity by multiplying capacity by
  // (1 + FLAGS_allocator_capacity_extra_ratio).
  //
  // This function is only used for DRAM and PMEM cache. SSD cache doesn't
  // need allocator_capacity.
  size_t Convert2AllocatorCapacity(size_t capacity);

  // capacity_ is the capacity of the cache instance.
  // Note that this only counts the space used to store key and value buffers,
  // Extra memory used by the storage engine or cache replacement policy is not
  // counted.
  std::size_t capacity_;

  // replacement_type_ specifies the cache replacement policy used by this cache
  // instance.
  ReplacementPolicyType replacement_type_;

  // storage_type_ specifies the underlying storage engine used by this cache
  // instance.
  StorageEngineType storage_type_;

  // storage path for pmem or ssd cache. Dram cache will just ignore this field.
  std::vector<std::string> paths_;

  // Used to indicate whether the cache instance uses external devices as the
  // storage engine
  bool use_device_{false};

  // initialized_ defines whether the cache instance has been successfully
  // initialized.
  bool initialized_;

  // replacement_policy_ is an unique pointer that points to the underlying
  // replacement policy object.
  std::unique_ptr<ReplacementPolicy> replacement_policy_;

  // storage_engine_ is an unique pointer that points to the underlying storage
  // engine object.
  std::unique_ptr<StorageEngine> storage_engine_;

  // Eviction handler callback function
  std::function<void(CacheBufferSharedPtr)> eviction_cb_;

  // eviction_handler_status indicates whether eviction handler is enabled or
  // disabled.
  bool eviction_handler_status_;

  // Eviction metric callback function
  std::function<void(size_t num)> eviction_metric_cb_;

  // metric_registry_ is mainly for storage engines with log-based
  // allocators to report their GC metrics and for storage engines to report
  // failed async put
  std::shared_ptr<noodle::MetricRegistry> metric_registry_;
  // Capacity-related metrics
  std::shared_ptr<noodle::AtomicGauge> instance_capacity_bytes_gauge_;
  std::shared_ptr<noodle::AtomicGauge> instance_used_capacity_bytes_gauge_;
  std::shared_ptr<noodle::AtomicGauge> instance_item_num_gauge_;

  // Metrics for the number of failed async puts in the cache instance

  // The old CacheBuffer is not found the index. So AsycPut fails, we just
  // discard new_buffer. async_put_failed_cache_buffer_not_found_counter_ is
  // used to track this case.
  std::shared_ptr<noodle::AtomicCounter>
      async_put_failed_cache_buffer_not_found_counter_;

  // When two AsyncPut tasks updating the same key are pushed to a background
  // executor. Then the older task's old_ptr is invalid and the older task will
  // end up with a kCacheReplaceMismatch error. But the younger task will
  // success and all things will get right.
  // async_put_failed_cache_replace_mismatch_counter_ is used to count the
  // number of kCacheReplaceMismatch errors
  std::shared_ptr<noodle::AtomicCounter>
      async_put_failed_cache_replace_mismatch_counter_;
  // Fail to AsyncPut `value` to StorageEngine. The old CacheBuffer
  // in `replacement_policy_` must be removed.
  // async_put_failed_storage_engine_full_counter_ is used to track
  // this case.
  std::shared_ptr<noodle::AtomicCounter>
      async_put_failed_storage_engine_full_counter_;
  std::shared_ptr<noodle::AtomicCounter>
      async_put_failed_unknown_error_counter_;
};

}  // namespace mtcache
