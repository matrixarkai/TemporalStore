#pragma once

#include "buffer/cache_buffer.h"
#include "common/cache_error.h"

#include <folly/futures/Future.h>
#include <folly/io/IOBuf.h>
#include <noodle/base/result.h>

#include <memory>
#include <string>

namespace mtcache {

// StorageEngineType is the enum definition of all storage engine types
// supported by MTCache.
enum class StorageEngineType : uint8_t {
  // DRAM Storage Engine
  kDRAMStorageEngine = 0,
  // PMEM Storage Engine
  kPMEMStorageEngine = 1,
  // SSD Storage Engine (based on TerarkDB)
  kSSDTerarkDBStorageEngine = 2,
  // Simple DRAM storage engine for integration and unit tests
  kSimpleStorageEngine = 3,
  // Multi-Device SSD Storage Engine
  kMultiSSDStorageEngine = 4,
  // SSD Storage Engine (based on Zoned Store)
  kSSDZonedStoreStorageEngine = 5,
  // Maximum/Invalid storage engine type
  kMaxCode
};

enum class SSDEngineType : uint8_t { kTerarkDB = 0, kZonedStore = 1 };

// StorageEngine is an abstract class than provides an uniform interface for
// CacheInstances to store, retrieve, and delete buffers from underlying storage
// engine.
class StorageEngine {
 public:
  // This class defines the pure virtual interface on which to receive the key
  // and CacheBuffer of recovered data after crashes or server reboots.
  class RecoverDataCallback {
   public:
    virtual ~RecoverDataCallback() {}

    // Insert the key and buffer into the cache index in replacement policy.
    virtual void OnRecoverData(const std::string& key,
                               CacheBufferSharedPtr buffer) = 0;
  };

  // This class defines the interface on which to receive the key, old data
  // pointer and new cache buffer when GC copies the old data to a new location.
  // The class who owns the cache index (i.e. CacheInstance) should implement
  // this function and update the cache buffer pointer with the new one.
  //
  // @Param:
  //  - key:
  //    used to find the corresponding CacheBuffer in the index.
  //  - old_data_ptr:
  //    used to check whether the data pointer in the CacheBuffer is equal to
  //    'old_data_ptr'.
  //
  // @Return:
  //  - If the CacheBuffer is found and 'old_data_ptr' matches the data pointer
  //    in CacheBuffer, this method will atomically update the buffer pointer in
  //    the CacheBuffer and return no error.
  //  - If no CacheBuffer is found for the 'key', kCacheBufferNotFound error is
  //    returned.
  //  - If the CacheBuffer is found but 'old_data_ptr' does not match the data
  //    pointer in CacheBuffer, kCacheReplaceMismatch error is returned.
  class GCCopyCallback {
   public:
    virtual ~GCCopyCallback() {}

    virtual noodle::Result<void, CacheError> Update(
        const std::string& key, const char* old_data_ptr,
        CacheBufferSharedPtr new_buffer) = 0;
  };

  // Default constructor
  StorageEngine() = default;

  // Default destructor.
  virtual ~StorageEngine() {}

  // Start method initializes the storage engine. It returns true if the
  // operation is successful; otherwise it returns false.
  // The caller that calls `Start` method must call `Stop` explicitly.
  virtual bool Start() = 0;

  // Stop method stops the storage engine. It returns true if the operation is
  // successful; otherwise it returns false. After the storage engine is stopped
  // (regardless whether it returns true or false), the caller (e.g.
  // CacheInstance) should no longer access data in the storage engine.
  virtual bool Stop() = 0;

  // Put method stores the key-value pair into the storage engine.
  // If the storage engine is already full, a kStorageFull error is returned;
  // if there is an IO or PMEM device access error, a kStorageAccessError is
  // returned. Otherwise, a CacheBuffer shared pointer is returned.
  virtual noodle::Result<CacheBufferSharedPtr, CacheError> Put(
      const std::string& key, folly::IOBuf value) = 0;

  // AsyncPut method stores the key-value pair into the storage engine
  // asynchronously. The callback function receives a shared pointer of the
  // new inserted cache buffer, and it should do the approriate work with this
  // new cache buffer.
  //
  // This method returns a SemiFuture of the new-inserted cache buffer or error.
  // It is the same with the parameter passed to the callback function. Usually
  // the caller should ignore the returned SemiFuture, except he wants to block
  // until the async task is done by the worker thread (e.g. UT may want to wait
  // in this way).
  //
  // This method should only be used by PMEM storage engine. DRAM engine should
  // use `Put` instead.
  using AsyncPutCb =
      std::function<void(noodle::Result<CacheBufferSharedPtr, CacheError>)>;
  virtual folly::SemiFuture<noodle::Result<CacheBufferSharedPtr, CacheError>>
  AsyncPut(CacheBufferSharedPtr buffer, AsyncPutCb cb) {
    return &Errors::kStorageUnsupported;
  }

  // Get method should ONLY been called for SSD-based storage engines, as the
  // cache buffer already contain a pointer to access cache value for DRAM/PMEM
  // based storage engines. The storage engine looks up the cache value using
  // the `key`. If the lookup is successful, the value is returned in a
  // StringBuffer; If the buffer cannot be found, kStorageBufferNotFound error
  // is returned. If this method is called on a DRAM/PMEM storage engine, a
  // kStorageUnsupported error is returned.
  virtual noodle::Result<CacheBufferSharedPtr, CacheError> Get(
      const std::string& key) = 0;

  // If the key **definitely** does not exist in the storage, then this method
  // returns false, else true.
  // This check is potentially lighter-weight than invoking Get(). One way
  // to make this lighter weight is to avoid doing any IO/memory-copy.
  virtual bool Peek(const std::string& key) { return false; }

  // This Delete method removes the buffer identified by CacheBuffer buffer, and
  // mark the memory or disk space used by the value of the cached buffer
  // recyclable. Nothing is returned if the operation is successful. If the
  // buffer cannot be found, kStorageBufferNotFound error is returned; If there
  // is an IO or PMEM access error, kStorageAccessError is returned.
  virtual noodle::Result<void, CacheError> Delete(CacheBufferPtr buffer) {
    return &Errors::kStorageUnsupported;
  }

  // AsyncDelete method removes the buffer idetified by `buffer` asynchronously.
  //
  // This method returns a SemiFuture of the an empty CacheBufferSharedPtr or
  // error. Usually the caller should ignore the returned SemiFuture, except he
  // wants to block until the async task is done by the worker thread (e.g. UT
  // may want to wait in this way).
  //
  // This method should only be used by PMEM storage engine. DRAM engine should
  // use `Put` instead.
  virtual folly::SemiFuture<noodle::Result<CacheBufferSharedPtr, CacheError>>
  AsyncDelete(CacheBufferPtr buffer) {
    return &Errors::kStorageUnsupported;
  }

  // This Delete method removes the buffer identified by `key`, which is used
  // only for ssd storage engine. Other storage engines should always return
  // kStorageUnsupported error.
  virtual noodle::Result<void, CacheError> Delete(const std::string& key) {
    return &Errors::kStorageUnsupported;
  }

  // Reset method clears all data from the storage engine and resets all related
  // stats. If an error occured, the storage engine would enter a degraded or
  // error state, and return a kCacheResetFailed error. The cache instance
  // should be disabled upon receiving such an error.
  virtual noodle::Result<void, CacheError> Reset() = 0;

  // RecoverData method is called by the cache instance to recover cached buffer
  // after server failures. This method should be a NO-OP for DRAM-based storage
  // engines. For each recovered buffer, the callback method is called to insert
  // the buffer into the index (hashtable) of the replacement policy that
  // implements this callback. If an error occurred during the recover process,
  // a kCacheRecoverFailed error is returned. It's up to the caller (e.g. cache
  // instance) to decide whether it can use the partially recovered data or
  // reset the entire storage. If the method is not implemented, a
  // kNotImplemented error is returned.
  virtual noodle::Result<void, CacheError> RecoverData(
      RecoverDataCallback* callback) = 0;

  // This method is used to set engine's capacity if available.
  virtual void SetCapacity(uint64_t cap) {}

 protected:
  // initialized_ is a state variable indicating whether the storage engine has
  // been properly initialized
  bool initialized_{false};
};

}  // namespace mtcache
