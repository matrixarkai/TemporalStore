#include "multi_ssd.h"

#include "buffer/string_buffer.h"
#include "common/hash.h"
#include "storage/ssd_terarkdb.h"

#ifdef BUILD_ZONED_STORE
#include "storage/zoned_store/zoned_store.h"
#endif

DECLARE_int32(ssd_engine_type);

namespace mtcache {

bool StorageEngineMultiSSD::Start() {
  std::unique_lock<MutexType> lock(mutex_);

  if (!Init()) {
    LOG(ERROR) << "Start StorageEngineMultiSSD failed";
    // clear
    for (auto& s : storages_) {
      s->Stop();
    }
    storages_.clear();

    return false;
  }

  initialized_ = true;
  LOG(INFO) << "Start StorageEngineMultiSSD success";
  return true;
}

bool StorageEngineMultiSSD::Init() {
  // paths_ to storages_
  for (int i = 0; i < paths_.size(); ++i) {
    auto& path = paths_[i];
    LOG(INFO) << "Init multi-ssd cache[" << i << "], path=" << path
              << ", capacity = " << capacity_;

    auto storage = CreateStorageByDevicePath(path);
    if (!storage) {
      LOG(ERROR) << "Init create storage failed, path=" << path;
      return false;
    }
    storages_.push_back(std::move(storage));
  }

  for (auto& storage : storages_) {
    if (!storage->Start()) {
      LOG(ERROR) << "Init create storage failed, path=" << storage->path();
      return false;
    }
  }

  if (storages_.empty()) {
    LOG(INFO) << "paths is empty";
    return false;
  }

  return true;
}

bool StorageEngineMultiSSD::Stop() {
  std::unique_lock<MutexType> lock(mutex_);

  for (auto& s : storages_) {
    s->Stop();
  }
  storages_.clear();

  initialized_ = false;
  return true;
}

noodle::Result<CacheBufferSharedPtr, CacheError> StorageEngineMultiSSD::Put(
    const std::string& key, folly::IOBuf value) {
  CHECK(initialized_);
  std::shared_lock<MutexType> lock(mutex_);
  if (storages_.empty()) {
    return &Errors::kStorageEngineUninitialized;
  }

  auto idx = Hash(key) % storages_.size();
  return storages_[idx]->Put(key, std::move(value));
}

noodle::Result<CacheBufferSharedPtr, CacheError> StorageEngineMultiSSD::Get(
    const std::string& key) {
  CHECK(initialized_);
  std::shared_lock<MutexType> lock(mutex_);
  if (storages_.empty()) {
    return &Errors::kStorageEngineUninitialized;
  }

  auto idx = Hash(key) % storages_.size();
  return storages_[idx]->Get(key);
}

bool StorageEngineMultiSSD::Peek(const std::string& key) {
  CHECK(initialized_);
  std::shared_lock<MutexType> lock(mutex_);

  auto idx = Hash(key) % storages_.size();
  return storages_[idx]->Peek(key);
}

noodle::Result<void, CacheError> StorageEngineMultiSSD::Delete(
    const std::string& key) {
  CHECK(initialized_);
  std::shared_lock<MutexType> lock(mutex_);

  if (storages_.empty()) {
    return &Errors::kStorageEngineUninitialized;
  }

  auto idx = Hash(key) % storages_.size();
  return storages_[idx]->Delete(key);
}

noodle::Result<void, CacheError> StorageEngineMultiSSD::Reset() {
  CHECK(initialized_);
  std::unique_lock<MutexType> lock(mutex_);

  if (storages_.empty()) {
    return &Errors::kStorageEngineUninitialized;
  }
  noodle::Result<void, CacheError> result;
  for (const auto& storage : storages_) {
    auto res = storage->Reset();
    if (!res.IsOk()) {
      result = res;
    }
  }
  return result;
}

noodle::Result<void, CacheError> StorageEngineMultiSSD::RecoverData(
    RecoverDataCallback* callback) {
  CHECK(initialized_);
  DCHECK(callback != nullptr);

  std::shared_lock<MutexType> lock(mutex_);
  if (storages_.empty()) {
    return &Errors::kStorageEngineUninitialized;
  }

  noodle::Result<void, CacheError> last_error{};
  auto startTime = std::chrono::steady_clock::now();
  for (auto& storage : storages_) {
    auto ret = storage->RecoverData(callback);
    if (!ret.IsOk()) {
      last_error = ret;
    }
  }
  std::chrono::duration<double, std::milli> recoverTime =
      std::chrono::steady_clock::now() - startTime;
  LOG(INFO) << "MultiSSD: Cost " << recoverTime.count()
            << "ms to recover data.";

  return last_error;
}

uint32_t StorageEngineMultiSSD::Hash(const std::string& key) const {
  return mur_mur_hash2(key.data(), key.length());
}

// manage multi-device
bool StorageEngineMultiSSD::AddDevice(const std::string& path) {
  std::unique_lock<MutexType> lock(mutex_);
  for (const auto& storage : storages_) {
    if (path == storage->path()) {
      LOG(WARNING) << "AddDevice failed, path already exists, path=" << path;
      return false;
    }
  }

  auto storage = CreateStorageByDevicePath(path);
  if (storage == nullptr || !storage->Start()) {
    LOG(WARNING) << "AddDevice failed, create storage failed, path=" << path;
    return false;
  }

  storages_.push_back(std::move(storage));
  return true;
}

bool StorageEngineMultiSSD::RemoveDevice(const std::string& path) {
  std::unique_lock<MutexType> lock(mutex_);
  for (auto iter = storages_.begin(); iter != storages_.end(); ++iter) {
    if (iter->get()->path() == path) {
      storages_.erase(iter);
      return true;
    }
  }
  LOG(WARNING) << "RemoveDevice failed, not exists, path=" << path;
  return false;
}

std::unique_ptr<StorageEngineSSD>
StorageEngineMultiSSD::CreateStorageByDevicePath(const std::string& path) {
  if (FLAGS_ssd_engine_type == static_cast<int>(SSDEngineType::kTerarkDB) ||
      ssdcache_type_ == StorageEngineType::kSSDTerarkDBStorageEngine) {
    return std::make_unique<StorageEngineTerarkDB>(
        path, storage_engine_metric_registry_);
  }
#ifdef BUILD_ZONED_STORE
  return std::make_unique<StorageEngineZonedStore>(path, capacity_);
#else
  LOG(WARNING)
      << "ZonedStore SSD engine requested but BUILD_ZONED_STORE is OFF; "
      << "falling back to RocksDB-compatible SSD engine";
  return std::make_unique<StorageEngineTerarkDB>(
      path, storage_engine_metric_registry_);
#endif
}

}  // namespace mtcache
