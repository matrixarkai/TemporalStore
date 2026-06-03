#include "cache_instance.h"
#include "debug_utils.h"
#include "mtcache.h"
#include "simple_lru_cache.h"
#include "storage/zoned_store/zoned_store.h"
#include "unified_cache.h"

#include <absl/time/clock.h>
#include <folly/String.h>
#include <libmemcached/memcached.h>
#include <noodle/metric/bytedance_metric_report_buidler.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <set>
#include <shared_mutex>
#include <stdio.h>
#include <typeinfo>
#include <unordered_map>
#include <vector>

#undef NDEBUG
#include <cassert>

namespace mtcache {
// A SimpleLRUCache wrapper with shared mutex.
// TODO(guokuankuan) We may want to change size & capacity to std::atomic
// values.
class ConcurrentSimpleLRUCache : public Cache<std::string, std::string> {
 public:
  ConcurrentSimpleLRUCache(uint64_t capacity = (1 << 20))
      : simple_lru_(capacity) {}

  ~ConcurrentSimpleLRUCache(){};

  virtual bool Start() override { return true; }
  virtual bool Stop() override { return true; }

  virtual void Insert(const std::string& key, std::string value,
                      size_t size) override {
    DEBUG_TIME_TRACE_START("SimpleCache::Insert:lock");
    std::lock_guard<std::shared_mutex> lock(m_);
    DEBUG_TIME_TRACE_END("SimpleCache::Insert:lock");

    DEBUG_TIME_TRACE_START("SimpleCache::Insert:Insert()");
    simple_lru_.Insert(key, value, size);
    DEBUG_TIME_TRACE_END("SimpleCache::Insert:Insert()");
  }

  virtual std::optional<std::string> Lookup(const std::string& key) override {
    std::lock_guard<std::shared_mutex> lock(m_);
    return simple_lru_.Lookup(key);
  }

  virtual void Remove(const std::string& key) override {
    std::lock_guard<std::shared_mutex> lock(m_);
    simple_lru_.Remove(key);
  }

  virtual void RemoveAll() override {
    std::lock_guard<std::shared_mutex> lock(m_);
    simple_lru_.RemoveAll();
  }

  virtual size_t Capacity() const override {
    std::shared_lock<std::shared_mutex> lock(m_);
    return simple_lru_.Capacity();
  }

  virtual void SetCapacity(size_t capacity) override {
    std::lock_guard<std::shared_mutex> lock(m_);
    simple_lru_.SetCapacity(capacity);
  }

  virtual size_t Size() const override {
    std::shared_lock<std::shared_mutex> lock(m_);
    return simple_lru_.Size();
  };

 private:
  SimpleLRUCache<std::string, std::string> simple_lru_;

  // TODO(guokuankuan): Use folly::SharedMutex instead
  mutable std::shared_mutex m_;
};

// This is a memcached client wrapper. It uses only the server `localhost:11211`
class MemcachedWrapper : public Cache<std::string, std::string> {
 public:
  MemcachedWrapper(const uint64_t capacity) {}
  // TODO(guokuankuan) destory clients
  ~MemcachedWrapper() { ResetClients(); }

  memcached_st* get_client();

  virtual bool Start() override { return true; }
  virtual bool Stop() override { return true; }

  virtual void Insert(const std::string& key, std::string value,
                      size_t size) override;

  virtual std::optional<std::string> Lookup(const std::string& key) override;

  // TODO(guokuankuan)
  virtual void Remove(const std::string& key) override {}

  // TODO(guokuankuan)
  virtual void RemoveAll() override {}

  // TODO(guokuankuan)
  virtual size_t Capacity() const override { return 0; }

  // TODO(guokuankuan)
  virtual void SetCapacity(size_t capacity) override {}

  // TODO(guokuankuan)
  virtual size_t Size() const override { return 0; };

  // As preload workers finish, need to reset the mapping between client threads
  // and server threads.
  void ResetClients() {
    for (const auto& client : clients_) {
      memcached_free(client.second);
    }
    clients_.clear();
  }

 private:
  // aka memcached_server_st*
  memcached_server_list_st servers_ = nullptr;

  std::map<std::thread::id, memcached_st*> clients_;

  mutable std::mutex m_;
};

// This helper class takes different policy and storage engine to initialize
// target cacheInstance.
class FlexibleCache : public Cache<std::string, std::string> {
 public:
  FlexibleCache(uint64_t capacity, const std::string& policy,
                const std::string& engine,
                const std::vector<std::string>& pmem_paths,
                const std::vector<std::string>& ssd_paths);

  ~FlexibleCache() {
    noodle::GetGlobalMetricRegistry()->Deregister("ti.mtcache.bench");
    noodle::GetGlobalMetricRegistry()->Deregister("ti.mtcache.zonedstore");
  }

  virtual bool Start() override { return true; }
  virtual bool Stop() override { return true; }

  virtual void Insert(const std::string& key, std::string value,
                      size_t size) override;

  virtual std::optional<std::string> Lookup(const std::string& key) override;

  virtual void Remove(const std::string& key) override {
    instance_->Delete(key);
  }

  virtual void RemoveAll() override { instance_->Reset(); }

  virtual size_t Capacity() const override { return instance_->GetCapacity(); }

  virtual void SetCapacity(size_t capacity) override {
    instance_->SetCapacity(capacity);
  }

  virtual size_t Size() const override { return instance_->GetUsedSpace(); };

  void CalculateSpaceAmplification() const;

 private:
  StorageEngineType engine_;
  std::vector<std::string> paths_;
  std::unique_ptr<CacheInstance> instance_;
  std::shared_ptr<noodle::MetricRegistry> registry_;
  std::shared_ptr<noodle::MetricRegistry> zoned_store_registry_;
};

// This helper class initializes the target UnifiedCache instance with a
// different replacement policy and placement policy.
class MultiTierCache : public Cache<std::string, std::string> {
 public:
  MultiTierCache(uint64_t dram_capacity, uint64_t pmem_capacity,
                 uint64_t ssd_capacity, const std::string& policy,
                 const std::vector<std::string>& pmem_paths,
                 const std::vector<std::string>& ssd_paths,
                 std::string& dram_pmem_data_placement_type,
                 bool enable_eviction,
                 size_t side_by_side_dram_pmem_placement_threshold,
                 std::string& ssd_storage_engine);

  ~MultiTierCache();

  void PrintLatency(noodle::SummarySnapshot* snapshot, std::string comments);
  void PrintCacheStats(std::string metrics, std::string comments);
  void PrintMeasurement();

  virtual bool Start() override { return true; }
  virtual bool Stop() override { return true; }

  virtual void Insert(const std::string& key, std::string value,
                      size_t size) override;

  virtual std::optional<std::string> Lookup(const std::string& key) override;

  virtual void Remove(const std::string& key) override;

  virtual void RemoveAll() override;

  virtual size_t Capacity() const override;

  virtual void SetCapacity(size_t capacity) override;

  virtual size_t Size() const override;

 private:
  std::vector<std::string> pmem_paths_;
  std::vector<std::string> ssd_paths_;
  std::unique_ptr<UnifiedCache> cache_;
};

}  // namespace mtcache
