#include "storage/zoned_store/gc.h"

#include "common/logging.h"
#include "storage/zoned_store/buffer_manager.h"
#include "storage/zoned_store/index.h"
#include "storage/zoned_store/util.h"
#include "storage/zoned_store/zone_manager.h"
#include "storage/zoned_store/zoned_store.h"

#include <folly/io/IOBuf.h>

#include <chrono>
#include <cstdint>
#include <deque>
#include <malloc.h>
#include <memory>
#include <utility>
#include <variant>

namespace mtcache {
GCWorker::GCWorker(std::shared_ptr<BufferManager> bm_ptr,
                   std::shared_ptr<ZoneManager> zone_manager_ptr,
                   std::shared_ptr<BufferEncoder> encoder,
                   std::shared_ptr<IndexUpdater> index_updater,
                   int max_record_length)
    : buffer_manager_(bm_ptr),
      encoder_(std::move(encoder)),
      zone_manager_(std::move(zone_manager_ptr)),
      index_updater_(std::move(index_updater)),
      record_buf_(static_cast<char*>(memalign(
          StorageEngineZonedStore::kAlignSize,
          max_record_length + 3 * StorageEngineZonedStore::kAlignSize))) {
  DCHECK(record_buf_);
  enabled_gc_.store(false, std::memory_order_release);
}

GCWorker::~GCWorker() {
  DCHECK(!GCEnabled());
  free(record_buf_);
}

void GCWorker::Start() {
  LOG(INFO) << "GC Worker starting" << std::endl;
  DCHECK(!GCEnabled());
  enabled_gc_.store(true, std::memory_order_release);
  background_thread_ = std::thread(&GCWorker::GC, this);
}

void GCWorker::Stop() {
  LOG(INFO) << "GC Worker stopping" << std::endl;
  DCHECK(GCEnabled());
  enabled_gc_.store(false, std::memory_order_release);
  background_thread_.join();
}

int GCWorker::Notify() {
  notify_cv_.notify_one();
  return 0;
}

// Return value is not that useful.
int GCWorker::GC() {
  LOG(INFO) << "GC Worker started, looping..." << std::endl;
  while (GCEnabled()) {
    auto lk = std::unique_lock<std::mutex>(notify_lock_);
    notify_cv_.wait_for(lk, std::chrono::seconds(1));

    // `ZoneManager` specifies which group to recycle and related mode.
    // FIXME(fangliming) : use `is_lossy`.
    bool is_lossy;
    int16_t gc_group_id;
    std::tie(gc_group_id, is_lossy) = zone_manager_->FindGCGroup();

    // 2.Read and construct valid data.(If we really have gc work).
    if (gc_group_id != -1) {
      // We see all data within a zone could be sacrified sicne they are
      // only cached objects
      // When we process these records (still valid in zonedstore's index)
      // we need to remove them from index to make them invisiable to the
      // policy layer.
      // TODO (guokuankuan) After we implement pinned data, we need to
      // migrate pinned data.
      LOG(INFO) << "GCWorker::GC(), try to reclaim a group, id: " << gc_group_id;
      zone_manager_->LoadMetaData(gc_group_id,
                                  [this, is_lossy](const char* buf) {
                                    return ProcessMetadata(buf, is_lossy);
                                  });
      zone_manager_->ResetGroup(gc_group_id);
      LOG(INFO) << "GCWorker::GC(), reset finished, id: " << gc_group_id;
    }
  }
  return 0;
}

int GCWorker::ProcessMetadata(const char* oplogs, bool is_lossy) const {
  ZonedStoreMetrics::ScopedLatency latency(
      ZonedStoreMetrics::codec_deserializedata_latency);
  const char* current_oplog = oplogs;
  // See codec.h for layout comment.
  uint64_t valid_bytes = 0;
  uint64_t processed_bytes = 0;
  // current_oplog points to first bytes of current oplog memory area.
  current_oplog = GetFixedUint64(oplogs, &valid_bytes);
  while (processed_bytes < valid_bytes) {
    WriteBuffer::BufferDataType data{};
    // current_oplog = ConstructSingleRecord(current_oplog, data);
    std::string key;
    current_oplog = ConstructSingleKey(current_oplog, key);
    processed_bytes = (current_oplog - oplogs);

    // Process Data.
    DCHECK(
        std::holds_alternative<Index::SSDColoredPtr>(index_updater_->Get(key)));
    bool is_deleted =
        index_updater_->DeleteIf(key, [&](Index::RecordStateType type) {
          if (is_lossy) {
            return type <= Index::kNormal;
          } else {
            return type == Index::kSoftDel;
          }
        });
    // FIXME(fangliming) : delete this assertion.
    DCHECK(is_deleted);

    // TODO (guokuankuan) If the data is pinned, we migrate it and update index.
    /*
    auto shared_value_ptr(std::move(data.second));
    index_update_listener_->OnUpdateIndex(data.first, shared_value_ptr);
    buffer_manager_->Put(std::make_pair(data.first, shared_value_ptr),
                         WriteBufferType::kGCBuf);
    */
  }
  DCHECK(processed_bytes == valid_bytes);

  return 0;
}

const char* GCWorker::ConstructSingleKey(const char* oplog,
                                         std::string& key) const {
  uint32_t key_len = 0;
  uint64_t value_offset = 0;
  return encoder_->DeserializeOplog(oplog, &key_len, &key, &value_offset,
                                    nullptr);
}

const char* GCWorker::ConstructSingleRecord(
    const char* oplog, WriteBuffer::BufferDataType& data) const {
  // Oplog fields.
  uint32_t key_len = 0;
  std::string key{};
  uint64_t value_offset = 0;
  // Data fields.
  uint32_t value_length = 0;
  char* value_buf = nullptr;
  // Other.
  const int aligned_size = StorageEngineZonedStore::kAlignSize;

  // 1.Decode oplog buffer.
  oplog =
      encoder_->DeserializeOplog(oplog, &key_len, &key, &value_offset, nullptr);
  // 2.Prepare offset.
  auto val = index_updater_->Get(key);
  DCHECK(std::holds_alternative<Index::SSDColoredPtr>(val));
  auto colored_ptr = std::get<Index::SSDColoredPtr>(val);
  uint32_t record_read_length = 0;
  std::tie(record_read_length, value_offset) = DecodeColoredPtr(colored_ptr);
  record_read_length *= aligned_size;
  int aligned_delta = 0;
  // Address must be aligned, we may read more bytes.
  if ((aligned_delta = value_offset % aligned_size) != 0) {
    record_read_length += aligned_size;
    value_offset -= aligned_delta;
  }
  // 3.Read from disk and extract data.
  int read_result =
      zone_manager_->Read(record_buf_, value_offset, record_read_length);
  DCHECK_EQ(read_result, 0);
  // Ownership is transfered to IOBuf and return to caller.
  value_buf = static_cast<char*>(malloc(record_read_length));
  DCHECK(value_buf);
  encoder_->DeserializeData(record_buf_ + aligned_delta, &value_length,
                            value_buf, nullptr);
  DCHECK_GE(value_length, 0);
  data = std::make_pair(
      std::move(key),
      folly::IOBuf::takeOwnership(value_buf, record_read_length, value_length));

  return oplog;
}

}  // namespace mtcache
