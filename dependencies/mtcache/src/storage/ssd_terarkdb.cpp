#include "ssd_terarkdb.h"

#include "buffer/string_buffer.h"
#include "buffer/string_view_buffer.h"

#include <gflags/gflags.h>

#include <filesystem>

DECLARE_bool(cache_ssd_data_recovery_in_background);

namespace mtcache {

static const char* kSSDCacheDefaultCFName = "default-ssd-cache";

StorageEngineTerarkDB::StorageEngineTerarkDB(
    std::string db_path, std::shared_ptr<noodle::MetricRegistry> registry)
    : StorageEngineSSD(),
      db_path_(std::move(db_path)),
      storage_engine_metric_registry_(std::move(registry)) {
  LOG(INFO) << "db_path=" << db_path_;
}

bool StorageEngineTerarkDB::Start() {
  std::unique_lock<MutexType> lock(mutex_);

  RegisterStorageEngineMetrics();

  column_families_.clear();
  handles_.clear();
  handle_ = nullptr;

  rocksdb::DBOptions options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  options.wal_bytes_per_sync = 32768;
  options.bytes_per_sync = 32768;
  options.delayed_write_rate = 300UL << 20;
  // When benchmark vesus ZonedStore, we should uncomment these lines
  // options.use_direct_reads = true;
  // options.allow_mmap_reads = false;
  // options.use_direct_io_for_flush_and_compaction = true;
  // options.max_background_jobs = 2;

  auto cfopt = rocksdb::ColumnFamilyOptions();
  cfopt.num_levels = 2;
  cfopt.compression = rocksdb::kNoCompression;

  column_families_.emplace_back(kSSDCacheDefaultCFName, cfopt);
  column_families_.emplace_back("default", cfopt);

  // If the path for ssd does not exists, we must create the directoy.
  if (!std::filesystem::exists(db_path_)) {
    std::error_code err_code;
    std::filesystem::create_directories(db_path_, err_code);
    if (err_code) {
      LOG(FATAL) << "Fail to create dirctory for ssd cache, path=[" << db_path_
                 << "], error msg is: " << google::StrError(err_code.value());
    }
  }
  if (std::filesystem::is_regular_file(db_path_)) {
    LOG(FATAL) << "[" << db_path_ << "] is file! We need directory!";
  }
  // Open
  rocksdb::DB* db = nullptr;
  auto s =
      rocksdb::DB::Open(options, db_path_, column_families_, &handles_, &db);

  if (!s.ok()) {
    LOG(ERROR) << "ERROR to Open TerarkDB, status=" << s.ToString();
    return false;
  }

  // ListColumnFamilies
  std::vector<std::string> column_families;
  s = rocksdb::DB::ListColumnFamilies(rocksdb::DBOptions(), db_path_,
                                      &column_families);
  if (!s.ok()) {
    LOG(ERROR) << "ERROR to ListColumnFamilies TerarkDB, status="
               << s.ToString();
    return false;
  }

  for (auto& cf : column_families) {
    LOG(INFO) << "TerarkDB column_family: " << cf;
  }

  // do
  db_.reset(db);
  if (handles_.empty()) {
    LOG(ERROR) << "ERROR to create ColumnFamily";
    return false;
  }
  handle_ = handles_[0];

  initialized_ = true;
  LOG(INFO) << "Start StorageEngineTerarkDB success";
  return true;
}

bool StorageEngineTerarkDB::Stop() {
  std::unique_lock<MutexType> lock(mutex_);

  initialized_ = false;

  if (recover_thread_) {
    recover_thread_->join();
    recover_thread_.reset();
  }

  handle_ = nullptr;

  for (auto handle : handles_) {
    delete handle;
  }
  handles_.clear();

  column_families_.clear();

  db_.reset();

  return true;
}

noodle::Result<CacheBufferSharedPtr, CacheError> StorageEngineTerarkDB::Put(
    const std::string& key, folly::IOBuf value) {
  std::shared_lock<MutexType> lock(mutex_);

  CHECK(initialized_);
  auto options = rocksdb::WriteOptions();
  options.sync = false;
  options.disableWAL = true;
  auto wb = std::make_unique<rocksdb::WriteBatch>();
  wb->Put(handle_, rocksdb::Slice(key.data(), key.length()),
          rocksdb::Slice(reinterpret_cast<const char*>(value.data()),
                         value.length()));

  auto s = db_->Write(options, wb.get());
  if (!s.ok()) {
    LOG(ERROR) << "Failed to PUT cache to terarkdb, key=" << key
               << ", error=" << s.ToString();
  }
  // Return an empty StringBuffer here because ssd cache instance should get the
  // data from ssd each time `CacheIntance::Get` is called
  auto res = std::make_shared<StringViewBuffer>(value.length());
  res->SetKey(key);
  return std::static_pointer_cast<CacheBuffer>(res);
}

noodle::Result<CacheBufferSharedPtr, CacheError> StorageEngineTerarkDB::Get(
    const std::string& key) {
  std::shared_lock<MutexType> lock(mutex_);

  CHECK(initialized_);
  std::string value;
  auto s = db_->Get(rocksdb::ReadOptions(), handle_,
                    rocksdb::Slice(key.data(), key.size()), &value);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return &Errors::kStorageBufferNotFound;
    } else {
      LOG(ERROR) << "Failed to GET Cache, key=" << key
                 << ", error=" << s.ToString();
      return &Errors::kUnknownError;
    }
  }
  auto res = std::make_shared<StringBuffer>(std::move(value));
  res->SetKey(key);
  return std::static_pointer_cast<CacheBuffer>(res);
}

bool StorageEngineTerarkDB::Peek(const std::string& key) {
  std::string value;
  auto s = db_->Get(rocksdb::ReadOptions(), handle_,
                    rocksdb::Slice(key.data(), key.length()), &value);
  return s.ok();
}

noodle::Result<void, CacheError> StorageEngineTerarkDB::Delete(
    const std::string& key) {
  std::shared_lock<MutexType> lock(mutex_);

  CHECK(initialized_);
  auto options = rocksdb::WriteOptions();
  options.sync = true;
  auto wb = std::make_unique<rocksdb::WriteBatch>();
  wb->Delete(handle_, key);

  auto s = db_->Write(options, wb.get());
  if (!s.ok()) {
    LOG(ERROR) << "Failed to DELETE Cache, key=" << key
               << ", error=" << s.ToString();
  }
  return {};
}

noodle::Result<void, CacheError> StorageEngineTerarkDB::Reset() {
  std::unique_lock<MutexType> lock(mutex_);

  CHECK(initialized_);

  auto name = handle_->GetName();
  // Clean
  LOG(INFO) << "Reset SSD Cache, TerarkDB path=" << db_path_;
  auto s = db_->DropColumnFamily(handle_);
  if (!s.ok()) {
    LOG(WARNING) << "DropColumnFamily cf_name=" << name
                 << " fail: " << s.ToString();
  }

  // Destroy handle
  s = db_->DestroyColumnFamilyHandle(handle_);
  if (!s.ok()) {
    LOG(WARNING) << "DestroyColumnFamilyHandle cf_name=" << name
                 << " fail: " << s.ToString();
  }
  handle_ = nullptr;
  handles_[0] = handle_;

  // Reopen handle
  auto cfopt = rocksdb::ColumnFamilyOptions();
  cfopt.num_levels = 2;
  s = db_->CreateColumnFamily(cfopt, kSSDCacheDefaultCFName, &handle_);
  if (!s.ok()) {
    LOG(WARNING) << "DestroyColumnFamilyHandle cf_name=" << name
                 << " fail: " << s.ToString();
  } else {
    LOG(WARNING) << "Reopen success cf_name=" << name;
  }
  handles_[0] = handle_;

  return {};
}
noodle::Result<void, CacheError> StorageEngineTerarkDB::DoRecoverData(
    RecoverDataCallback* callback) {
  std::shared_lock<MutexType> lock(mutex_);

  DCHECK(callback != nullptr);

  LOG(INFO) << "Start RecoverData";
  if (UNLIKELY(!initialized_)) {
    LOG(INFO) << "Cancel RecoverData before starting";
    return &Errors::kStorageEngineUninitialized;
  }
  auto iter = std::unique_ptr<rocksdb::Iterator>(
      db_->NewIterator(rocksdb::ReadOptions(), handle_));

  auto startTime = std::chrono::steady_clock::now();
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    // engine may be stopped by Stop()
    if (UNLIKELY(!initialized_)) {
      LOG(INFO) << "Cancel RecoverData";
      return &Errors::kStorageEngineUninitialized;
    }

    uint64_t value_length = iter->value().size();
    auto string_view = std::make_shared<StringViewBuffer>(value_length);
    if (callback) {
      callback->OnRecoverData(iter->key().ToString(), string_view);
      ssd_recover_records_counter_->Increase();
    }
  }

  std::chrono::duration<double, std::milli> recoverTime =
      std::chrono::steady_clock::now() - startTime;
  LOG(INFO) << "ssd_terarkdb: Cost " << recoverTime.count()
            << "ms to recover data.";
  ssd_recover_time_counter_->SetValue(
      static_cast<int64_t>(recoverTime.count()));

  LOG(INFO) << "Finish RecoverData";
  recover_finished_.store(true);
  return {};
}

noodle::Result<void, CacheError> StorageEngineTerarkDB::RecoverData(
    RecoverDataCallback* callback) {
  CHECK(initialized_);
  DCHECK(callback != nullptr);

  if (FLAGS_cache_ssd_data_recovery_in_background) {
    recover_thread_.reset(new std::thread(
        [this, callback = callback]() { DoRecoverData(callback); }));
    return {};
  } else {
    return DoRecoverData(callback);
  }
}

void StorageEngineTerarkDB::RegisterStorageEngineMetrics() {
  ssd_recover_time_counter_ =
      storage_engine_metric_registry_->MustRegister<noodle::AtomicGauge>(
          noodle::MetricId(db_path_ + "_ssd_terarkdb_recover_time"),
          std::make_shared<noodle::AtomicGauge>());

  ssd_recover_records_counter_ =
      storage_engine_metric_registry_->MustRegister<noodle::AtomicCounter>(
          noodle::MetricId(db_path_ + "_ssd_terarkdb_recover_records"),
          std::make_shared<noodle::AtomicCounter>());
}

}  // namespace mtcache
