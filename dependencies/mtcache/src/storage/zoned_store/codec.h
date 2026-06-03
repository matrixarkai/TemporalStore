#pragma once

#include "storage/storage_engine.h"
#include "storage/zoned_store/buffer_manager.h"
#include "storage/zoned_store/index.h"
#include "storage/zoned_store/metrics.h"

#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>

#include <deque>
#include <memory>
#include <vector>
#include <xxhash.h>

namespace mtcache {
class BufferEncoder {
 public:
  // Encoded layout:
  // - data:
  // HashOfLengthAndValue[16] + Length[4] + Value[VAR]
  // - metadata:
  // HashOfLengthKeyAndOffset[16] + Length[4] + Key[VAR] + Offset[8]
  // Currently, Hash[16] is calculated by xxh3.
  // Metadata zone layout:
  // LengthOfMetadata[8] + Metadata(1)[VAR] + ... + Metadata(n)[VAR]
  BufferEncoder(int buf_size);
  ~BufferEncoder() = default;
  const int data_fixed_part_size_ = 20;
  const int oplog_fixed_part_size_ = 28;
  const int oplog_header_size = 8;
  struct XXH128_T {
    XXH64_hash_t first;
    XXH64_hash_t second;
  };

  // Encode user value in `src` and write result to `dst`.
  // Return `dst`+`encoded_length`.
  char* SerializeData(const std::shared_ptr<::folly::IOBuf>& src,
                      char* dst) const;

  // Construct op_logs and notify `index` of the update.
  // Data shoule be persisted first.
  // @buf_q: it contains multiple key-value pairs.
  // @batch_begin_offset: offset of the first k-v userdata pair on SSD.
  // @return value: Encoded IOBuf of op_logs.
  std::unique_ptr<::folly::IOBuf> SerializeOplog(
      const std::deque<WriteBuffer::BufferDataType>& buf_q,
      Index::UpdateEntryCallback update_entry_cb, uint64_t batch_begin_offset,
      uint32_t oplog_size);

  // Return encoded op_logs sizes of one WriteBuffer.
  // Refer to `BufferManager::AggregateIOBuf` for meaning of the extra 8bytes.
  uint32_t CalculateEncodedOpLogSize(
      const std::unique_ptr<WriteBuffer>& buffer_ptr) {
    return buffer_ptr->key_size() +
           oplog_fixed_part_size_ * buffer_ptr->count();
  }

  // Return
  uint32_t CalculateEncodedDataSize(
      const std::unique_ptr<WriteBuffer>& buffer_ptr) {
    return buffer_ptr->value_size() +
           data_fixed_part_size_ * buffer_ptr->count();
  }

  // Decode byte sequence starting at `src` to `length`,`value` and
  // `is_corrupted`. Return next record's decode starting address.
  const char* DeserializeData(const char* src, uint32_t* length, char* value,
                              bool* is_corrupted) const;
  const char* DeserializeData(const char* src, uint32_t* length,
                              std::string* value, bool* is_corrupted) const;

  // Decode byte sequence starting at `src` to `length`,`value`,`oplog`,
  // `offset` and `is_corrupted`.
  // Return next record's decode starting address.
  const char* DeserializeOplog(const char* src, uint32_t* length,
                               std::string* oplog, uint64_t* offset,
                               bool* is_corrupted) const;
  uint64_t get_xxh_seed() const { return xxh_seed_; }

 private:
  const int align_size_;
  const uint64_t xxh_seed_ = 0;
};

}  // namespace mtcache
