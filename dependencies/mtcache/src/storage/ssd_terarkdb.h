#pragma once

#include "rocksdb/db.h"
#include "ssd.h"

#include <noodle/metric/metric_registry.h>

#include <atomic>
#include <shared_mutex>
#include <thread>

namespace mtcache {

// StorageEngineTerarkDB defines the storage engine that stores data in SSD,
// use TerarkDB as storage engine.

class StorageEngineTerarkDB : public StorageEngineSSD {
 public:
  explicit StorageEngineTerarkDB(
      std::string db_path, std::shared_ptr<noodle::MetricRegistry> registry);

  ~StorageEngineTerarkDB() override = default;

  bool Start() override;

  bool Stop() override;

  std::string path() override { return db_path_; }

  noodle::Result<CacheBufferSharedPtr, CacheError> Put(
      const std::string& key, folly::IOBuf value) override;

  noodle::Result<CacheBufferSharedPtr, CacheError> Get(
      const std::string& key) override;

  bool Peek(const std::string& key) override;

  noodle::Result<void, CacheError> Delete(const std::string& key) override;

  noodle::Result<void, CacheError> Reset() override;

  noodle::Result<void, CacheError> RecoverData(
      RecoverDataCallback* callback) override;

  bool IsDataRecovered() { return recover_finished_.load(); }

 private:
  noodle::Result<void, CacheError> DoRecoverData(RecoverDataCallback* callback);

 private:
  using MutexType = std::shared_timed_mutex;
  mutable MutexType mutex_;

  const std::string db_path_;
  std::unique_ptr<rocksdb::DB> db_;
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families_;
  std::vector<rocksdb::ColumnFamilyHandle*> handles_;
  rocksdb::ColumnFamilyHandle* handle_;

  std::unique_ptr<std::thread> recover_thread_;
  std::atomic<bool> recover_finished_{false};

  std::shared_ptr<noodle::MetricRegistry> storage_engine_metric_registry_;
  std::shared_ptr<noodle::AtomicGauge> ssd_recover_time_counter_;
  std::shared_ptr<noodle::AtomicCounter> ssd_recover_records_counter_;
  void RegisterStorageEngineMetrics();
};

}  // namespace mtcache
