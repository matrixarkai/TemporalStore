#pragma once

#include "storage/storage_engine.h"
#include "storage/zoned_store/index.h"

#include <folly/MPMCQueue.h>
#include <folly/ProducerConsumerQueue.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <noodle/base/result.h>

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace mtcache {

enum class WriteBufferType : uint8_t {
  // Value means priority, the less, the higher.
  kUserDataBuf = 0,

  kMetaDataBuf = 1,

  kGCBuf = 2,

  kCodecDataBuf = 3
};

class WriteBuffer {
 public:
  using BufferDataType = std::pair<std::string, Index::ValueMemoryType>;
  WriteBuffer() = default;

  explicit WriteBuffer(WriteBufferType type,
                       uint32_t capacity = 10485760);  // 10MB

  uint32_t capacity() const { return capacity_; }
  uint32_t size() const { return size_; }
  uint32_t count() const { return buf_q_.size(); }
  WriteBufferType buf_type() const { return buf_type_; }
  uint32_t key_size() const { return key_size_; }
  uint32_t value_size() const { return value_size_; }
  void push_back(BufferDataType data);
  std::deque<BufferDataType> steal_buf_q();
  std::deque<BufferDataType>& get_buf_q() { return buf_q_; }
  const char* get_value_q() { return value_q_.get(); }
  char* get_encoded_wp() { return encoded_buf_wp_; }
  void set_encoded_wp(char* new_wp) { encoded_buf_wp_ = new_wp; }

 private:
  std::deque<BufferDataType> buf_q_;
  std::unique_ptr<char, decltype(std::free)*> value_q_{nullptr, ::free};
  WriteBufferType buf_type_{WriteBufferType::kUserDataBuf};
  uint32_t key_size_;
  uint32_t value_size_;
  uint32_t size_;
  uint32_t capacity_;
  char* encoded_buf_wp_;
};

// `BufferManager` managers user data, gc data and metadata buffer.
// All persistant data should go through `BufferManager` layer.
class ZoneManager;
class BufferEncoder;
class IndexUpdater;
class BufferManager {
 public:
  BufferManager() : flush_queue_(2) {}
  ~BufferManager() { DCHECK(!WriteEnabled()); }

  BufferManager(uint32_t user_bufs, uint32_t gc_bufs,
                std::shared_ptr<ZoneManager> zone_mgr_ptr,
                std::shared_ptr<BufferEncoder> encoder,
                std::shared_ptr<IndexUpdater> index_updater,
                uint32_t capacity_per_buf = (1 << 21),  // 2MB
                double flush_threshold = 0.8, int flush_size = 10);

  // Called by gc thread or user thread or background flush write thread.
  noodle::Result<void, CacheError> Put(
      std::pair<std::string, Index::ValueMemoryType> value,
      WriteBufferType type);

  // Called when initialized to begin background writing task.
  void Start();

  // Called when destroyed, it will wait for thread to finish its current job.
  // Before being deconstructed, called must explicitly call `Stop`.
  void Stop();

  // Core member function dealing with the buffer-flushing job.
  // Run in long-running background thread.
  // Keep consuming(flushing) immutable buffer from `flush_queue_`.
  void FlushBuffers();

  void CodecBuffers();

  bool WriteEnabled();

  void SetWriteEnabled(bool status);

  // Divide 'WriteBuffer' with small units if necessary and flush small units
  // instead of the whole 'WriteBuffer' in order to reduce space amplification.
  void FlushBuffer(std::unique_ptr<WriteBuffer> bufqfer_ptr);

  void CodecBuffer(const std::unique_ptr<WriteBuffer>& buffer_ptr);

  // Helper function of 'FlushBuffers'.
  // 1. Calculate size of encoded oplog and data, if new total oplog
  // can't be hold in zone, flush it alone.
  // 2. Flush encoded data, if there isn't enough space in current zone group,
  // flush previous oplog first.
  // 3. Encode new metadata and push it back into `BufferManager`.
  // void DoFlush(std::unique_ptr<WriteBuffer> buffer_ptr);

  // Generate continous oplog iobufs physically.
  // Its contents are already encoded.
  std::unique_ptr<::folly::IOBuf> AggregateIOBuf(
      const std::deque<WriteBuffer::BufferDataType>& iobuf_q, uint32_t size);

  // For Test.
  const ::folly::MPMCQueue<std::unique_ptr<WriteBuffer>>& get_flush_queue()
      const {
    return codec_queue_;
  }

  // For Test.
  const std::vector<std::unique_ptr<WriteBuffer>>& get_buffer_pools() const {
    return buffer_pools_;
  }

 private:
  uint64_t CalculateFlushBytes(uint32_t accu_values, uint32_t accu_logs) const;
  int ShardingIndex(WriteBufferType type) const;

  uint32_t user_bufs_;
  uint32_t gc_bufs_;
  const uint32_t meta_bufs_ = 1;

  // Currently, we suppose op_log buffer number are one.
  // Layout: User data + GC data + Metadata
  std::vector<std::unique_ptr<WriteBuffer>> buffer_pools_;
  std::vector<std::mutex> buffer_locks_;

  // When `WriteBuffer` is full enough, it enters `flush_queue_`
  // and wait for flushing thread to flush them.
  ::folly::MPMCQueue<std::unique_ptr<WriteBuffer>> codec_queue_;

  ::folly::MPMCQueue<std::unique_ptr<WriteBuffer>> flush_queue_;

  uint32_t cap_per_buf_;

  std::shared_ptr<ZoneManager> zone_manager_;

  std::shared_ptr<BufferEncoder> encoder_;

  std::shared_ptr<IndexUpdater> index_updater_;

  std::thread buffer_flush_thread_;
  std::thread codec_thread_;

  double flush_threshold_;

  std::atomic<bool> enable_background_writing_;

  const uint32_t header_size_ = 4096;

  const uint32_t footer_size_ = 4096;

  uint64_t free_space_in_zone_;
};

}  // namespace mtcache
