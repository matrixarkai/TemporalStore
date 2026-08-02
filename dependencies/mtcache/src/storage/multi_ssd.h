#pragma once

#include "ssd.h"
#include "storage_engine.h"

#include <noodle/metric/metric_registry.h>

#include <shared_mutex>
#include <vector>

namespace mtcache {

// StorageEngineMultiSSD defines the storage engine that stores data in SSD.
class StorageEngineMultiSSD : public StorageEngine {
 public:
  StorageEngineMultiSSD(const std::vector<std::string>& paths,
                        uint64_t capacity,
                        std::shared_ptr<noodle::MetricRegistry> registry,
                        StorageEngineType ssdcache_type =
                            StorageEngineType::kSSDZonedStoreStorageEngine)
      : paths_(paths),
        capacity_(capacity),
        storage_engine_metric_registry_(std::move(registry)),
        ssdcache_type_(ssdcache_type) {}

  ~StorageEngineMultiSSD() override = default;

  bool Start() override;

  bool Stop() override;

  noodle::Result<CacheBufferSharedPtr, CacheError> Put(
      const std::string& key, folly::IOBuf value) override;

  noodle::Result<CacheBufferSharedPtr, CacheError> Get(
      const std::string& key) override;

  bool Peek(const std::string& key) override;

  noodle::Result<void, CacheError> Delete(const std::string& key) override;

  noodle::Result<void, CacheError> Reset() override;

  noodle::Result<void, CacheError> RecoverData(
      RecoverDataCallback* callback) override;

  // manage multi-device
  bool AddDevice(const std::string& path);

  bool RemoveDevice(const std::string& path);

 private:
  bool Init();

  std::unique_ptr<StorageEngineSSD> CreateStorageByDevicePath(
      const std::string& path);

  virtual uint32_t Hash(const std::string& key) const;

 private:
  using MutexType = std::shared_timed_mutex;

  mutable MutexType mutex_;

  std::vector<std::unique_ptr<StorageEngineSSD>> storages_;

  std::vector<std::string> paths_;

  // SSD vaild capacity, all devices share the same capacity.
  uint64_t capacity_ = 0;

  std::shared_ptr<noodle::MetricRegistry> storage_engine_metric_registry_;

  StorageEngineType ssdcache_type_;
};

}  // namespace mtcache
