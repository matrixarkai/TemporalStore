#include "storage/zoned_store/zoned_store.h"

#include "common/logging.h"
#include "storage/zoned_store/gc.h"
#include "storage/zoned_store/index.h"
#include "storage/zoned_store/util.h"
#include "storage/zoned_store/zone_manager.h"
#include "zoned_store.h"

#include <noodle/test_util/sync_point.h>

#include <cstring>
#include <memory>
#include <string>
#include <utility>

// Do we need to revovery old SSD data? default: false
DECLARE_bool(cache_enable_ssd_data_recovery);

namespace mtcache {

DEFINE_uint64(zonedstore_num_user_bufs, 2, "Zoned Store: number of user bufs");
DEFINE_uint64(zonedstore_num_gc_bufs, 2, "Zoned Store: number of gc bufs");
DEFINE_uint64(zonedstore_per_buf_capacity, 16,
              "Zoned Store: the capacity buffer (MB)");
DEFINE_double(zonedstore_flush_threshold, 0.8, "Zoned Store: flush threshold");
DEFINE_int32(zonedstore_zone_mode, 1, "Zone Mode: 0 is small; 1 is large;");
DEFINE_bool(zonedstore_use_async_read, false,
            "Zoned Store: use libbytedisk's async read API");

const uint64_t StorageEngineZonedStore::kNotExist = 0xffffffffffffffff;
const uint64_t StorageEngineZonedStore::kMemoryAddrFlags = 0xffffffffffff0000;
const uint64_t StorageEngineZonedStore::kSSDLBAFlags = 0xfffffffffff80000;
const uint64_t StorageEngineZonedStore::kSSDRecordSizeFlags = 0x7ff80;
const int StorageEngineZonedStore::kAlignSize = 4096;
const uint64_t StorageEngineZonedStore::kRecordStateFlags = 0x3;

StorageEngineZonedStore::StorageEngineZonedStore(std::string db_path,
                                                 uint64_t capacity)
    : StorageEngineZonedStore(
          db_path, capacity, FLAGS_zonedstore_zone_mode,
          FLAGS_zonedstore_num_user_bufs, FLAGS_zonedstore_num_gc_bufs,
          (FLAGS_zonedstore_per_buf_capacity << 20),
          FLAGS_zonedstore_flush_threshold, FLAGS_cache_enable_ssd_data_recovery) {
}

StorageEngineZonedStore::StorageEngineZonedStore(
    std::string db_path, uint64_t ssd_capactiy, int zone_mode,
    uint32_t user_bufs, uint32_t gc_bufs, uint32_t capacity_per_buf,
    double flush_threshold, bool using_existing_db)
    : db_path_(std::move(db_path)),
      ssd_capacity_(ssd_capactiy),
      zone_mode_(zone_mode),
      user_bufs_(user_bufs),
      gc_bufs_(gc_bufs),
      capacity_per_buf_(capacity_per_buf),
      flush_threshold_(flush_threshold),
      using_existing_db_(using_existing_db) {}

bool StorageEngineZonedStore::Start() {
  LOG(INFO) << "ZonedStore Engine Starting, reuse db: " << using_existing_db_;
  std::string target_path = db_path_;
  auto dev = NewDevice(target_path.c_str(), ssd_capacity_, zone_mode_);
  zone_manager_ = NewZoneManager(dev, using_existing_db_);
  index_ = std::make_shared<Index>();
  auto index_updater = std::make_shared<IndexUpdater>(index_);
  encoder_ = std::make_shared<BufferEncoder>(2 * capacity_per_buf_);
  buffer_manager_ = std::make_shared<BufferManager>(
      user_bufs_, gc_bufs_, zone_manager_, encoder_, index_updater,
      capacity_per_buf_, flush_threshold_);
  gc_worker_ =
      std::make_shared<GCWorker>(buffer_manager_, zone_manager_, encoder_,
                                 index_updater, kMaxRecordLength);
  // update index
  // FIXME(fangliming) : Change oplog's offset to colored_ptr to support
  // recovery logic
  std::function<int(const char* buf)> index_cb([&](const char* meta_buf) {
    const char* p = meta_buf;
    // size: 8 + oplogs
    uint64_t size = 0;
    uint64_t used = 0;

    p = GetFixedUint64(p, &size);
    LOG(INFO) << "ZonedStore Recover index_cb"
              << ", total data size(from meta): " << size;
    const char* oplog = p;
    while (used < size) {
      uint32_t key_len = 0;
      std::string key{};
      uint64_t ssd_colored_ptr = 0;
      // one op log
      oplog = encoder_->DeserializeOplog(oplog, &key_len, &key,
                                         &ssd_colored_ptr, nullptr);
      index_->Put(key, ssd_colored_ptr);
      used = (oplog - meta_buf);
    }
    DCHECK(used == size);
    return used;
  });
  zone_manager_->Recovery(index_cb);

  buffer_manager_->Start();
  gc_worker_->Start();
  initialized_ = true;
  LOG(INFO) << "ZonedStore Engine Started, reuse db: " << using_existing_db_;
  return true;
}

bool StorageEngineZonedStore::Stop() {
  // TODO(lvyanqi)
  // Recover from ssd
  gc_worker_->Stop();
  buffer_manager_->Stop();
  initialized_ = false;
  return true;
}

noodle::Result<CacheBufferSharedPtr, CacheError> StorageEngineZonedStore::Put(
    const std::string& key, folly::IOBuf user_buf) {
  // Caller may invalid the value_ref, so we need to copy
  auto value = folly::IOBuf::copyBuffer(user_buf.data(), user_buf.length());
  ZonedStoreMetrics::counterAdd(ZonedStoreMetrics::zoned_store_put_qps);
  ZonedStoreMetrics::counterAdd(ZonedStoreMetrics::zoned_store_put_throughput,
                                value->length());
  ZonedStoreMetrics::ScopedLatency latency(
      ZonedStoreMetrics::zoned_store_put_latency);

  DCHECK_GT(key.size(), 0);
  DCHECK_GT(value->length(), 0);

  uint32_t value_length = value->length();
  Index::ValueType shared_value =
      std::make_pair(std::move(value), Index::kSoftDel);
  index_->Put(key, shared_value);
  noodle::Result<void, CacheError> result = buffer_manager_->Put(
      {key, std::get<Index::MemoryColoredPtr>(shared_value).first},
      WriteBufferType::kUserDataBuf);
  if (!result.IsOk()) {
    return &Errors::kStorageAccessError;
  }

  ZonedStoreMetrics::counterAdd(ZonedStoreMetrics::zoned_store_used,
                                value_length + key.size());
  // Return an empty StringBuffer here because ssd cache instance
  // should get the data from ssd each time `CacheIntance::Get` is called
  // Compatible with ssd_terarkdb
  auto res = std::make_shared<StringViewBuffer>(value_length);
  res->SetKey(key);
  return std::static_pointer_cast<CacheBuffer>(res);
}

noodle::Result<CacheBufferSharedPtr, CacheError> StorageEngineZonedStore::Get(
    const std::string& key) {
  ZonedStoreMetrics::counterAdd(ZonedStoreMetrics::zoned_store_get_qps);
  ZonedStoreMetrics::ScopedLatency latency(
      ZonedStoreMetrics::zoned_store_get_latency);

  auto value = index_->Get(key);
  if (std::holds_alternative<Index::MemoryColoredPtr>(value)) {
    // [1].Record resides in memory
    // we have hold a `shared_ptr` to it, so we will be fine
    // if `IOBuf` is flushed.
    auto& shared_value = std::get<Index::MemoryColoredPtr>(value);
    NOODLE_DTEST_SYNC_POINT("StorageEngineZonedStore::Get_MemoryCase");
    NOODLE_DTEST_SYNC_POINT("StorageEngineZonedStore::Get_MemoryCase2");
    auto result = std::make_shared<StringBuffer>(
        std::string(reinterpret_cast<const char*>(shared_value.first->data()),
                    shared_value.first->length()));
    result->SetKey(key);
    return std::static_pointer_cast<CacheBuffer>(result);
  }

  // Record resides in SSD or NotFound.
  uint64_t color_ptr = std::get<uint64_t>(value);
  uint32_t size = 0;
  uint64_t offset = 0;

  if (color_ptr == kNotExist) {
    // [2].NotFound
    return &Errors::kStorageBufferNotFound;
  }

  // [3].SSD
  std::tie(size, offset) = DecodeColoredPtr(color_ptr);
  // `offset` should also be 4kb aligned(direct io).
  uint64_t aligned_delta = offset % kAlignSize;
  // If `offset` are not aligned, we should read more data(4kb) into
  // buf and real record is contained in this buf.
  // For example, offset = 0x100 and we want to read one page(4kb),
  // actually we should read all data within [0x0,0x200].
  if (aligned_delta != 0) {
    size++;
  }
  std::shared_ptr<char> buf(
      static_cast<char*>(memalign(kAlignSize, size * kAlignSize)), ::free);
  int read_result =
      zone_manager_->Read(buf.get(), offset - aligned_delta, size * kAlignSize);
  DCHECK_EQ(read_result, 0);
  if (read_result != 0) {
    return &Errors::kStorageAccessError;
  }
  // Refer to codec.h for bits format.
  uint32_t value_length;
  std::string user_value;
  bool is_corrupted;
  encoder_->DeserializeData(buf.get() + aligned_delta, &value_length,
                            &user_value, &is_corrupted);
  if (is_corrupted) {
    // TODO(fangliming) : retry when data is pinned.
    return &Errors::kStorageInvalidCacheBuffer;
  }
  auto result = std::make_shared<StringBuffer>(user_value);
  result->SetKey(key);

  return std::static_pointer_cast<CacheBuffer>(result);
}

bool StorageEngineZonedStore::Peek(const std::string& key) {
  auto val = index_->Get(key);
  return std::holds_alternative<Index::MemoryColoredPtr>(val) ||
         std::get<uint64_t>(val) != kNotExist;
}

noodle::Result<void, CacheError> StorageEngineZonedStore::Delete(
    const std::string& key) {
  index_->SoftDelete(key);
  return {};
}

noodle::Result<void, CacheError> StorageEngineZonedStore::Reset() {
  Stop();
  index_.reset();
  zone_manager_.reset();
  buffer_manager_.reset();
  gc_worker_.reset();
  Start();
  return {};
}

noodle::Result<void, CacheError> StorageEngineZonedStore::RecoverData(
    RecoverDataCallback* callback) {
  CHECK(initialized_);
  LOG(INFO) << "ZonedStore recovered on startup, now updating policy";
  std::shared_ptr<Index> index = GetIndex();

  uint64_t counter = 0;
  index->ScanIndexForRecover([&](const std::string& key, Index::ValueType value) {
    uint32_t size = 0;
    if (std::holds_alternative<Index::MemoryColoredPtr>(value)) {
      auto buf = std::get<Index::MemoryColoredPtr>(value);
      size = buf.first->length();
    } else {
      uint64_t ssd_ptr = std::get<uint64_t>(value);
      auto pair = DecodeColoredPtr(ssd_ptr);
      size = pair.first;
    }
    auto string_view = std::make_shared<StringViewBuffer>(size);
    if (callback) {
      callback->OnRecoverData(key, string_view);
      counter++;
    }
  });

  LOG(INFO) << "Zonedstore recovered policy count: " << counter;
  return {};
}

}  // namespace mtcache
