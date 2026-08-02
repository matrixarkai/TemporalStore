#pragma once

#include "buffer/string_buffer.h"
#include "buffer/string_view_buffer.h"
#include "storage/ssd.h"
#include "storage/zoned_store/buffer_manager.h"
#include "storage/zoned_store/codec.h"
#include "storage/zoned_store/gc.h"
#include "storage/zoned_store/index.h"
#include "storage/zoned_store/metrics.h"
#include "storage/zoned_store/zone_manager.h"

#include <folly/io/IOBuf.h>
#include <gflags/gflags.h>
#include <noodle/base/result.h>

#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
#include <mutex>
#include <thread>
#include <vector>

namespace mtcache {
// StorageEngineZonedStore defines the storage engine that stores data in SSD,
// organized in zone group.

class StorageEngineZonedStore : public StorageEngineSSD {
 public:
  // We should not exceed the ssd_capacity.
  StorageEngineZonedStore(std::string db_path, uint64_t ssd_capacity,
                          int zone_mode, uint32_t user_bufs, uint32_t gc_bufs,
                          uint32_t capacity_per_buf, double flush_threshold,
                          bool using_existing_db);

  // The capacity here is the same as Cache policy's capacity.
  StorageEngineZonedStore(std::string db_path, uint64_t capacity);

  ~StorageEngineZonedStore() override = default;

  void SetCapacity(uint64_t cap) override { ssd_capacity_ = cap; }

  bool Start() override;

  bool Stop() override;

  std::string path() override { return db_path_; }

  // `key` would only be added once, its initial state is
  // `kNormal`.
  // FIXME(fangliming) : currently only `kSoftDel` is supported.
  noodle::Result<CacheBufferSharedPtr, CacheError> Put(
      const std::string& key, folly::IOBuf value) override;

  // Retrive record with `key`.
  // If entry's state is `kSoftDel`, change to `kNormal`.
  noodle::Result<CacheBufferSharedPtr, CacheError> Get(
      const std::string& key) override;

  bool Peek(const std::string& key) override;

  // Cache eviction will call this function.
  // We don't actually delete key from zonedstore's index.
  // We simply mark it as `kSoftDel`, before it's recycled
  // it could still be accessed.
  noodle::Result<void, CacheError> Delete(const std::string& key) override;

  // Change state from `kPinned` to `kNormal`.
  // Otherwise no operation is performed.
  noodle::Result<void, CacheError> UnPin(const std::string& key);

  // Change entry's state from `kSoftDel` or `kNormal`
  // to `kPinned`.
  noodle::Result<void, CacheError> Pin(const std::string& key);

  noodle::Result<void, CacheError> Reset() override;

  noodle::Result<void, CacheError> RecoverData(
      RecoverDataCallback* callback) override;

  uint64_t GetDiskUsedSpace() { return zone_manager_->GetUsedSpace(); }

  // For Test only.
  std::shared_ptr<Index> GetIndex() const { return index_; }
  std::shared_ptr<BufferManager> GetBufferManager() const {
    return buffer_manager_;
  }

  // Refer to `Index`'s comment.
  // Flags are meant to retrive certain bits from uint64_t.
  static const uint64_t kNotExist;
  static const uint64_t kMemoryAddrFlags;
  static const uint64_t kSSDRecordSizeFlags;
  static const uint64_t kSSDLBAFlags;
  static const uint64_t kRecordStateFlags;
  static const int kAlignSize;

 private:
  const std::string db_path_;

  std::shared_ptr<Index> index_;

  std::shared_ptr<GCWorker> gc_worker_;

  std::shared_ptr<BufferEncoder> encoder_;

  std::shared_ptr<ZoneManager> zone_manager_;

  std::shared_ptr<BufferManager> buffer_manager_;

  uint64_t ssd_capacity_;

  // ZoneMode::Large or ZoneMode::Small
  // TODO (guokuankuan) Remove `Small` mode later soon.
  int zone_mode_;

  uint32_t user_bufs_;
  uint32_t gc_bufs_;
  uint32_t capacity_per_buf_;
  double flush_threshold_;

  bool using_existing_db_;

  // User value max length;
  const int kMaxRecordLength = (16 << 20);
};

}  // namespace mtcache
