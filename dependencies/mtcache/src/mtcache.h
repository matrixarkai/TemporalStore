#pragma once

#include <folly/io/IOBuf.h>

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace mtcache {

// The abstract interface for a cache.
//
// A cache is a semi-persistent mapping from keys to values: cache entries
// are inserted manually, and are stored in the cache until either manually
// removed or automatically evicted.
template <class Key, class Value>
class Cache {
 public:
  virtual ~Cache() = default;

  // Start this multi-tiered cache (DRAM, PMEM, SSD cache). If any of them
  // fails to start, return false and log the error messages. Otherwise,
  // return true.
  virtual bool Start() = 0;

  // Stop this multi-tiered cache (DRAM, PMEM, SSD cache). If any of them
  // fails to stop, return false and log the error messages. Otherwise,
  // return true.
  virtual bool Stop() = 0;

  // Associates value with key in this cache. Remembers that the value
  // occupies "size" units of cache capacity. Replaces any previously
  // associated value.
  //
  // NOTE: although the "value" parameter is passed by value, zero-copy
  // semantics can be achieved here through copy elision.
  virtual void Insert(const Key& key, Value value, size_t size = 1) = 0;

  // Returns the value associated with key in this cache, or std::nullopt if
  // there is no cached value for key.
  virtual std::optional<Value> Lookup(const Key& key) = 0;

  // Removes any entry corresponding to key from this cache.
  virtual void Remove(const Key& key) = 0;

  // Removes all entries from this cache.
  virtual void RemoveAll() = 0;

  // Returns the current capacity of this cache.
  virtual size_t Capacity() const = 0;

  // Sets the new capacity of this cache. If the new capacity is smaller than
  // the current cache size, some cache entries may be evicted to free up space.
  virtual void SetCapacity(size_t capacity) = 0;

  // Returns the total size of all items in this cache, including removed but
  // still pinned items.
  virtual size_t Size() const = 0;
};

// The abstract interface for a zero-copy cache.
//
// In order to provide zero-copy capability, the cache must explicitly manage
// the lifetime of the cached data. Specifically, a cache lookup must place a
// pin on the result, and the caller is responsible for releasing the pin once
// the result is no longer needed. References to a pinned cache entry must
// remain valid until all outstanding pins are released.
//
// ZeroCopyCache simplifies this cache entry lifetime management by wrapping
// the cache lookup result inside an opaque handle: a cache lookup that places
// a pin on the cache entry returns a cache handle, which can be used later
// to release the pin.
template <class Key, class Value>
class ZeroCopyCache : public Cache<Key, Value> {
 public:
  // If the cache contains an entry for "key", places a pin on the cache entry
  // and returns a handle to the pinned cache entry. Otherwise return nullptr.
  //
  // If a handle is returned, the caller must call "Release" when it no
  // longer needs the cached value. This functionality is useful to prevent
  // the value from being evicted from the cache until it is no longer being
  // used.
  class Handle {
   public:
    virtual ~Handle() {}
    virtual const Key& key() const = 0;
    virtual const Value& value() const = 0;
    virtual Handle* Clone() const = 0;
  };

  virtual ~ZeroCopyCache() = default;

  virtual Handle* Acquire(const Key& key) = 0;

  // Releases the pinning done by an earlier "Acquire". After this call,
  // the caller should no longer depend on the handle still being valid.
  virtual void Release(Handle* handle) = 0;

  // Same as "Insert" except that the newly inserted value will be pinned in
  // the cache. The caller should call "Release" on the returned handle when
  // it wants to release the pin.
  virtual Handle* InsertPinned(const Key& key, Value value,
                               size_t size = 1) = 0;

  // ScopedLookup
  //
  // If you have some code that looks like this:
  //   Handle handle = cache->Acquire(key);
  //   if (handle) {
  //     if (something) {
  //       ...do something...
  //       cache->Release(handle);
  //       return;
  //     } else if (something else) {
  //       ...do something else...
  //       c->Release(handle);
  //       return;
  //     }
  //   }
  // Then ScopedLookup will make the code simpler.  It automatically
  // releases the handle when the instance goes out of scope.
  // Example:
  //   ScopedLookup lookup(cache, key);
  //   if (lookup.Found()) {
  //     if (something) {
  //       ...do something...
  //     } else if (something else) {
  //       ...do something else...
  //     }
  //   }
  class ScopedLookup {
   public:
    ScopedLookup(ZeroCopyCache* cache, const Key& key)
        : cache_(cache), handle_(cache_->Acquire(key)) {}

    ~ScopedLookup() {
      if (handle_ != nullptr) cache_->Release(handle_);
    }
    const Key& key() const { return handle_->key(); }
    const Value& value() const { return handle_->value(); }
    bool Found() const { return handle_ != nullptr; }

   private:
    ZeroCopyCache* const cache_;
    Handle* const handle_;
  };
};

// The "CacheOptions" struct is used to init the multi-tiered cache.
struct CacheOptions {
  // The capacity for dram cache. When the space of inserted key-value pairs
  // exceed this capacity, replacement policy will evict some key-value pairs
  // to free some memory space.
  //
  // Note that this capacity includes the space used to store KEY and VALUE.
  // It does NOT include the space used by storage layout metadata and
  // allocator metadata (e.g. the header of the record memory layout,
  // the header of records in log-allocator and the meta of jemalloc-allocator).
  // So the actual used space may exceed the capacity a bit.
  //
  // Furthermore, when the evicted key-value pairs are still being accessed,
  // they will not be deleted until the last reader release the reference to
  // them. And if log-allocator is used, the GC worker will also take some
  // space for garbage-collection purpose. So the actual used space may exceed
  // this capacity too.
  //
  // A hard limit is used to throttle the actual used space including all the
  // above overheads, which is controlled by the config naming
  // `allocator_capacity_extra_ratio`. Its default value is 0.3, meaning
  // at most (130% * dram_capacity) space may be used.
  //
  // In most cases, the used space will mot reach the hard limit, unless the
  // most of cache records are being accessed simultaneously, which should
  // seldom happen. If the used space really reach the hard limit, the
  // Insert/Put API will return an out-of-space error.
  //
  size_t dram_capacity{0};

  // The capacity for pmem cache. Similar to dram_capacity.
  //
  // PMEM cache is disabled if this capacity is equal to 0.
  size_t pmem_capacity{0};

  // The capacity for ssd cache. When the space of inserted key-value pairs
  // exceed this capacity, replacement policy will evict some key-value pairs
  // to free some space.
  //
  // Note that this capacity includes the space used to store KEY and VALUE.
  // It does NOT include the space used by storage layout metadata and other
  // space overhead in ssd engine (terarkdb or zonedstore). So the actual
  // used space may exceed this capacity.
  //
  // SSD cache is disabled if this capacity is equal to 0.
  size_t ssd_capacity{0};

  // Pmem cache path on DAX file system.
  // The number of paths must be equal to FLAGS_used_num_numa_nodes.
  // Note that the paths defined here must be in the order of NUMA nodes, e.g:
  // if there are two paths: /a and /b, then /a must be
  // at the PMEM of the 1st NUMA node and /b must be at the 2nd NUMA node.
  std::vector<std::string> pmem_paths;

  // ssd cache path on SSD
  std::vector<std::string> ssd_paths;

  // Replacement policy inside each tier (DRAM, PMEM and SSD)
  //   - FIFO => First-In First-Out replacement policy
  //   - SLRU => Segmented Least Recently Used replacement policy
  std::string cache_dram_replacement_policy;
  std::string cache_pmem_replacement_policy;
  std::string cache_ssd_replacement_policy;

  // Replacement policy between DRAM and PMEM
  // Options:
  //   - SideBySide => Cache items are inserted into DRAM or PMEM cache
  //                   instance, based on whether the value size is greater
  //                   than a threshold.
  //   - Tiered => Cache items are inserted in DRAM cache instance initially,
  //               and evicted from DRAM cache into PMEM cache if eviction
  //               handler is enabled.
  std::string cache_dram_pmem_data_placement_type;

  // Threshold to determine whether a cache item should be placed in DRAM or
  // PMEM cache instance if SideBySide data placement type is used. If the
  // value size is smaller than the threshold, the item is placed into DRAM
  // instance; otherwise, it is placed into PMEM instance.
  size_t cache_dram_pmem_data_placement_threshold;

  std::string metric_id_prefix;

  std::map<std::string, std::string> metric_registry_tags;

  bool cache_ssd_instance_only{false};
};

// This class is a builder for both `Cache` and `ZeroCopyCache`. The users
// should always use this class to obtain a instance of MTCache.
class MTCacheBuilder {
 public:
  // Build a `Cache` instance.
  static std::unique_ptr<Cache<std::string, folly::IOBuf>> BuildCache(
      const CacheOptions& opts);

  // Build a `ZeroCopyCache` instance.
  static std::unique_ptr<ZeroCopyCache<std::string, folly::IOBuf>>
  BuildZeroCopyCache(const CacheOptions& opts);
};

}  // namespace mtcache
