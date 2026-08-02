#include "storage/zoned_store/buffer_manager.h"

#include "common/hash.h"
#include "common/logging.h"
#include "storage/zoned_store/codec.h"
#include "storage/zoned_store/gc.h"
#include "storage/zoned_store/index.h"
#include "storage/zoned_store/util.h"
#include "storage/zoned_store/zone_manager.h"
#include "storage/zoned_store/zoned_store.h"

#include <double-conversion/utils.h>
#include <folly/Random.h>
#include <gtest/gtest.h>
#include <noodle/test_util/sync_point.h>

#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

namespace mtcache {

WriteBuffer::WriteBuffer(WriteBufferType type, uint32_t capacity)
    : buf_q_(),
      buf_type_(type),
      key_size_(0),
      value_size_(0),
      size_(0),
      capacity_(capacity) {
  if (type == WriteBufferType::kCodecDataBuf) {
    value_q_.reset(static_cast<char*>(
        memalign(StorageEngineZonedStore::kAlignSize, capacity)));
    encoded_buf_wp_ = value_q_.get();
  }
}

void WriteBuffer::push_back(BufferDataType data) {
  size_ += (data.first.size() + data.second->length());
  key_size_ += data.first.size();
  value_size_ += data.second->length();
  if (buf_type_ != WriteBufferType::kMetaDataBuf) {
    DCHECK_GT(data.first.size(), 0);
    DCHECK_GT(data.second->length(), 0);
  } else {
    DCHECK_GT(data.second->length(), 0);
  }
  buf_q_.push_back(std::move(data));
}

std::deque<WriteBuffer::BufferDataType> WriteBuffer::steal_buf_q() {
  size_ = 0;
  key_size_ = 0;
  value_size_ = 0;
  return std::move(buf_q_);
}

BufferManager::BufferManager(uint32_t user_bufs, uint32_t gc_bufs,
                             std::shared_ptr<ZoneManager> zone_mgr_ptr,
                             std::shared_ptr<BufferEncoder> encoder,
                             std::shared_ptr<IndexUpdater> index_updater,
                             uint32_t capacity_per_buf, double flush_threshold,
                             int flush_size)
    : user_bufs_(user_bufs),
      gc_bufs_(gc_bufs),
      buffer_locks_(user_bufs + gc_bufs + meta_bufs_),
      codec_queue_(flush_size),
      flush_queue_(flush_size + 1),
      cap_per_buf_(capacity_per_buf),
      zone_manager_(std::move(zone_mgr_ptr)),
      encoder_(std::move(encoder)),
      index_updater_(std::move(index_updater)),
      flush_threshold_(flush_threshold),
      enable_background_writing_(false),
      free_space_in_zone_(zone_manager_->GetZoneCapacity() - header_size_ -
                          footer_size_) {
  DCHECK_GT(user_bufs, 0);
  DCHECK_GT(gc_bufs, 0);

  buffer_pools_.reserve(user_bufs + gc_bufs + meta_bufs_);
  for (int i = 0; i < user_bufs; i++) {
    buffer_pools_.push_back(std::make_unique<WriteBuffer>(
        WriteBufferType::kUserDataBuf, capacity_per_buf));
  }
  for (int i = 0; i < gc_bufs; i++) {
    buffer_pools_.push_back(std::make_unique<WriteBuffer>(
        WriteBufferType::kGCBuf, capacity_per_buf));
  }
  buffer_pools_.push_back(std::make_unique<WriteBuffer>(
      WriteBufferType::kMetaDataBuf, capacity_per_buf));
}

noodle::Result<void, CacheError> BufferManager::Put(
    std::pair<std::string, Index::ValueMemoryType> value,
    WriteBufferType type) {
  auto index = ShardingIndex(type);
  std::unique_lock buf_lock(buffer_locks_[index]);
  auto& buf = buffer_pools_[index];
  buf->push_back(value);
  auto cap = buf->capacity();
  auto size = buf->size();

  // If size/capacity >= threshold, current buffer --> immutable buffer.
  if (cap * flush_threshold_ <= size) {
    codec_queue_.blockingWrite(std::move(buf));
    buffer_pools_[index] = std::make_unique<WriteBuffer>(type, cap_per_buf_);
  }
  return {};
}

void BufferManager::Start() {
  DCHECK(!WriteEnabled());
  SetWriteEnabled(true);
  buffer_flush_thread_ = std::thread(&BufferManager::FlushBuffers, this);
  codec_thread_ = std::thread(&BufferManager::CodecBuffers, this);
}

void BufferManager::Stop() {
  DCHECK(WriteEnabled());
  SetWriteEnabled(false);
  buffer_flush_thread_.join();
  codec_thread_.join();
}

bool BufferManager::WriteEnabled() {
  return enable_background_writing_.load(std::memory_order_acquire);
}

void BufferManager::SetWriteEnabled(bool status) {
  enable_background_writing_.store(status, std::memory_order_release);
}

void BufferManager::CodecBuffers() {
  while (WriteEnabled()) {
    std::unique_ptr<WriteBuffer> buffer_ptr;
    NOODLE_DTEST_SYNC_POINT("BufferManager::CodecBuffers_BEGIN");
    if (codec_queue_.tryReadUntil(
            std::chrono::system_clock::now() + std::chrono::milliseconds(100),
            buffer_ptr)) {
      CodecBuffer(buffer_ptr);
    }
    NOODLE_DTEST_SYNC_POINT("BufferManager::CodecBuffers_DONE");
  }
  LOG(INFO) << "CodecBuffers() stopped";
}

void BufferManager::CodecBuffer(
    const std::unique_ptr<WriteBuffer>& buffer_ptr) {
  ZonedStoreMetrics::ScopedLatency latency(
      ZonedStoreMetrics::codec_serializedata_latency);
  DCHECK(buffer_ptr);
  DCHECK_GE(buffer_ptr->size(), buffer_ptr->capacity() * flush_threshold_);
  NOODLE_DTEST_SYNC_POINT("BufferManager::CodecBuffer_BEGIN");

  int align_size = StorageEngineZonedStore::kAlignSize;
  // 1.If we can't flush both new encoded record and accumulated encoded
  // records, we first flush accumulated one, then start a new round of
  // accumulation.
  // 2.`accu_values` and `accu_logs` determines how many free space
  // we need in the zone if we call `DoFlush`, `accu_flush_ptr` contains
  // accumulated encoded data.
  uint32_t flush_size =
      AlignedTo(encoder_->CalculateEncodedDataSize(buffer_ptr), align_size);
  std::unique_ptr<WriteBuffer> accu_flush_ptr(
      new WriteBuffer(WriteBufferType::kCodecDataBuf, flush_size));
  uint32_t accu_values = 0;
  // There's extra 8 bytes 'oplog_size' at the front of oplog part.
  uint32_t accu_logs = encoder_->oplog_header_size;
  uint32_t accu_flush_bytes = 0;

  for (auto& unit : buffer_ptr->get_buf_q()) {
    // unit.first = user key[string]
    // unit.second = pair<user value[IOBuf],state>
    uint32_t key_len = unit.first.size();
    uint32_t value_len = unit.second->length();
    uint32_t one_value = encoder_->data_fixed_part_size_ + value_len;
    uint32_t one_log = encoder_->oplog_fixed_part_size_ + key_len;
    accu_values += one_value;
    accu_logs += one_log;
    accu_flush_bytes = CalculateFlushBytes(accu_values, accu_logs);
    if (accu_flush_bytes > free_space_in_zone_) {
      // Next zone is always a clean zone
      free_space_in_zone_ =
          zone_manager_->GetZoneCapacity() - header_size_ - footer_size_;
      accu_logs = one_log + encoder_->oplog_header_size;
      accu_values = one_value;
      accu_flush_bytes = CalculateFlushBytes(accu_values, accu_logs);
      if (accu_flush_ptr->count() > 0) {
        flush_queue_.blockingWrite(std::move(accu_flush_ptr));
        accu_flush_ptr.reset(
            new WriteBuffer(WriteBufferType::kCodecDataBuf, flush_size));
        NOODLE_DTEST_SYNC_POINT("BufferManager::SplitBuffer");
      }
    }
    char* buf_wp = accu_flush_ptr->get_encoded_wp();
    buf_wp = encoder_->SerializeData(unit.second, buf_wp);
    accu_flush_ptr->set_encoded_wp(buf_wp);
    accu_flush_ptr->push_back(std::move(unit));
  }
  if (accu_flush_ptr->count() > 0) {
    free_space_in_zone_ -= accu_flush_bytes;
    flush_queue_.blockingWrite(std::move(accu_flush_ptr));
  }
  NOODLE_DTEST_SYNC_POINT("BufferManager::CodecBuffer_END");
}

void BufferManager::FlushBuffers() {
  while (WriteEnabled()) {
    std::unique_ptr<WriteBuffer> buffer_ptr;
    if (flush_queue_.tryReadUntil(
            std::chrono::system_clock::now() + std::chrono::milliseconds(100),
            buffer_ptr)) {
      FlushBuffer(std::move(buffer_ptr));
      NOODLE_DTEST_SYNC_POINT("BufferManager::FlushBuffers_END");
    }
  }
  LOG(INFO) << "FlushBuffers() stopped";
}

void BufferManager::FlushBuffer(std::unique_ptr<WriteBuffer> buffer_ptr) {
  DCHECK(buffer_ptr);
  NOODLE_DTEST_SYNC_POINT("BufferManager::FlushBuffer_BEGIN");
  uint32_t new_oplog_size = encoder_->CalculateEncodedOpLogSize(buffer_ptr);
  uint32_t old_oplog_size = buffer_pools_[buffer_pools_.size() - 1]->size();
  uint32_t oplog_size =
      new_oplog_size + old_oplog_size + encoder_->oplog_header_size;
  int align_size = StorageEngineZonedStore::kAlignSize;
  oplog_size = AlignedTo(oplog_size, align_size);

  uint32_t data_size =
      AlignedTo(encoder_->CalculateEncodedDataSize(buffer_ptr), align_size);
  const char* data_sptr = buffer_ptr->get_value_q();
  DCHECK(data_sptr);

  int has_available_space = zone_manager_->EnsureAvailableSpace(
      static_cast<int>(data_size), static_cast<int>(oplog_size));
  if (has_available_space == 0) {
    // Flush all previous oplogs first, then flush encoded data.
    // At last, notify to open new zoneGroup.
    uint32_t size = buffer_pools_[buffer_pools_.size() - 1]->size();
    DCHECK_GT(size, 0);
    std::deque<WriteBuffer::BufferDataType> stealed_q_oplog(
        buffer_pools_[buffer_pools_.size() - 1]->steal_buf_q());
    auto aggregated_iobuf = AggregateIOBuf(stealed_q_oplog, size);
    int append_ok = zone_manager_->Append(
        reinterpret_cast<char*>(aggregated_iobuf->writableData()),
        static_cast<int>(aggregated_iobuf->length()), DataType::META_LOG,
        nullptr);
    DCHECK_EQ(append_ok, 0);
    zone_manager_->FinishGroup();
    NOODLE_DTEST_SYNC_POINT("BufferManager::FinishGroup");

    has_available_space = zone_manager_->EnsureAvailableSpace(
        static_cast<int>(data_size), static_cast<int>(oplog_size));
    assert(has_available_space);
  }

  // FIXME(fangliming): When return value is -1(fail), what should we do?
  // Write userdata onto SSD and get the offset.
  uint64_t batch_begin_offset = 0;
  int append_ok = zone_manager_->Append(data_sptr, static_cast<int>(data_size),
                                        DataType::DATA, &batch_begin_offset);
  DCHECK_EQ(append_ok, 0);
  // Once we have offset, create oplog and notify index to process it.

  auto update_entry_cb = [& updater = this->index_updater_](
      const std::string& key, Index::ValueType new_value) {
    return updater->UpdateIndex(key, new_value);
  };
  std::unique_ptr<::folly::IOBuf> iobuf_oplog =
      encoder_->SerializeOplog(buffer_ptr->get_buf_q(), update_entry_cb,
                               batch_begin_offset, new_oplog_size);
  CHECK(iobuf_oplog.get() != nullptr);
  std::shared_ptr<::folly::IOBuf> shared_iobuf_oplog(std::move(iobuf_oplog));
  buffer_pools_[buffer_pools_.size() - 1]->push_back(
      std::make_pair(std::string(""), std::move(shared_iobuf_oplog)));
  NOODLE_DTEST_SYNC_POINT("BufferManager::FlushBuffer_END");
}

std::unique_ptr<::folly::IOBuf> BufferManager::AggregateIOBuf(
    const std::deque<WriteBuffer::BufferDataType>& iobuf_q, uint32_t size) {
  // At the very front of every chunks of oplogs, there is 8 bytes for
  // recording chunk's valid bytes.
  int align_size = StorageEngineZonedStore::kAlignSize;
  size += encoder_->oplog_header_size;
  if (size & 0xfff) {
    size += (align_size - size % align_size) % align_size;
  }
  // DIRECT IO alignment requirement.
  char* aggregated_ptr = static_cast<char*>(memalign(align_size, size));
  char* write_point = aggregated_ptr + encoder_->oplog_header_size;
  DCHECK(aggregated_ptr);

  for (const auto& data_pair : iobuf_q) {
    std::memcpy(write_point, data_pair.second->data(),
                data_pair.second->length());
    write_point += data_pair.second->length();
  }
  PutFixedUint64(aggregated_ptr, write_point - aggregated_ptr);
  auto iobuf = ::folly::IOBuf::takeOwnership(aggregated_ptr, size);
  DCHECK(iobuf);

  return iobuf;
}

uint64_t BufferManager::CalculateFlushBytes(uint32_t accu_values,
                                            uint32_t accu_logs) const {
  DCHECK(ZoneMode::LARGE == zone_manager_->GetZoneMode());
  int align_size = StorageEngineZonedStore::kAlignSize;
  return AlignedTo(accu_values, align_size) + AlignedTo(accu_logs, align_size);
}

int BufferManager::ShardingIndex(WriteBufferType type) const {
  int index = 0;
  auto key = ::folly::Random::rand64();
  index = mur_mur_hash2(&key, 8);
  if (type == WriteBufferType::kUserDataBuf) {
    index = index % user_bufs_;
  } else if (type == WriteBufferType::kGCBuf) {
    index = index % gc_bufs_ + user_bufs_;
  } else {
    index = index % meta_bufs_ + user_bufs_ + gc_bufs_;
  }

  return index;
}

}  // namespace mtcache
