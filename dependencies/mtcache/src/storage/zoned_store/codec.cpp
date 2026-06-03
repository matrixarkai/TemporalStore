#include "storage/zoned_store/codec.h"

#include "common/logging.h"
#include "storage/zoned_store/buffer_manager.h"
#include "storage/zoned_store/index.h"
#include "storage/zoned_store/util.h"
#include "storage/zoned_store/zoned_store.h"

#include <sys/types.h>

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <malloc.h>
#include <variant>

namespace mtcache {

BufferEncoder::BufferEncoder(int buf_size)
    : align_size_(StorageEngineZonedStore::kAlignSize) {}

char* BufferEncoder::SerializeData(const std::shared_ptr<::folly::IOBuf>& src,
                                   char* dst) const {
  DCHECK(dst);
  DCHECK(src);
  DCHECK_GT(src->length(), 0);

  // Currently, use xxhash3 to calculate checksum.
  // TODO(fangliming) : use real 128 hash.
  XXH128_T xxhash{};
  char* current_encoded_ptr = dst;
  uint32_t value_len = src->length();

  // 1.Ignore Hash first.
  current_encoded_ptr += sizeof(XXH128_T);
  // 2.Length.
  current_encoded_ptr = PutFixedUint32(current_encoded_ptr, value_len);
  // 3.Data value.
  char* current_src = reinterpret_cast<char*>(src->writableData());
  current_encoded_ptr =
      CopyBytesTo(current_encoded_ptr, current_src, value_len);
  // 4.Hash.
  xxhash.first =
      XXH64(dst + sizeof(xxhash), sizeof(value_len) + value_len, xxh_seed_);
  PutFixedHash128(dst, &xxhash);

  return current_encoded_ptr;
}

std::unique_ptr<::folly::IOBuf> BufferEncoder::SerializeOplog(
    const std::deque<WriteBuffer::BufferDataType>& buf_q,
    Index::UpdateEntryCallback update_entry_cb, uint64_t batch_begin_offset,
    uint32_t oplog_size) {
  DCHECK_GT(buf_q.size(), 0);

  Index::SSDColoredPtr colored_ptr = 0;
  XXH128_T xxhash{};
  uint32_t key_len = 0;
  uint32_t value_len = 0;

  char* encoded_log_buf = static_cast<char*>(memalign(align_size_, oplog_size));
  DCHECK(encoded_log_buf);
  std::unique_ptr<::folly::IOBuf> iobuf_oplog(
      ::folly::IOBuf::takeOwnership(encoded_log_buf, oplog_size));
  for (const auto& data_pair : buf_q) {
    key_len = data_pair.first.size();
    value_len = data_pair.second->length();
    DCHECK_GT(key_len, 0);
    DCHECK_GT(value_len, 0);

    // Construct colored_ptr to update `Index`.
    // Refer to `Index` for colored pointer layout detail.
    colored_ptr = 0ul;
    colored_ptr = MaskColoredPtrRecordState(colored_ptr, Index::kSoftDel);
    colored_ptr = MaskColoredPtrLBA(colored_ptr, batch_begin_offset);
    uint64_t record_size =
        static_cast<uint64_t>(data_fixed_part_size_ + value_len);
    uint64_t record_units = AlignedTo(record_size, align_size_) / align_size_;
    colored_ptr = MaskColoredPtrSize(colored_ptr, record_units);
    update_entry_cb(data_pair.first, colored_ptr);

    // Construct oplogs.
    // TODO(fangliming) : use real 128 hash.
    char* xxh_ptr = encoded_log_buf;
    encoded_log_buf += sizeof(xxhash);
    encoded_log_buf = PutFixedUint32(encoded_log_buf, key_len);
    encoded_log_buf =
        CopyBytesTo(encoded_log_buf, data_pair.first.data(), key_len);
    encoded_log_buf = PutFixedUint64(encoded_log_buf, colored_ptr);
    xxhash.first =
        XXH64(xxh_ptr + sizeof(xxhash),
              sizeof(key_len) + key_len + sizeof(colored_ptr), xxh_seed_);
    PutFixedHash128(xxh_ptr, &xxhash);

    // Update data offset.
    batch_begin_offset += (data_fixed_part_size_ + value_len);
  }
  return iobuf_oplog;
}

const char* BufferEncoder::DeserializeData(const char* src, uint32_t* length,
                                           char* value,
                                           bool* is_corrupted) const {
  DCHECK(src);
  DCHECK(length);

  // TODO(fangliming) : use real 128 hash.
  XXH128_T expected_xxhash{};
  XXH128_T actual_xxhash{};
  const char* tmp = src;
  tmp = GetFixedHash128(tmp, &actual_xxhash);
  tmp = GetFixedUint32(tmp, length);
  if (value) {
    CopyBytesFrom(tmp, value, *length);
  }
  if (is_corrupted) {
    expected_xxhash.first = XXH64(src + sizeof(actual_xxhash),
                                  sizeof(*length) + *length, xxh_seed_);
    *is_corrupted = (expected_xxhash.first != actual_xxhash.first);
  }
  tmp += *length;

  return tmp;
}

const char* BufferEncoder::DeserializeData(const char* src, uint32_t* length,
                                           std::string* value,
                                           bool* is_corrupted) const {
  DCHECK(src);
  DCHECK(length);

  // TODO(fangliming) : use real 128 hash.
  XXH128_T expected_xxhash{};
  XXH128_T actual_xxhash{};
  const char* tmp = src;
  tmp = GetFixedHash128(tmp, &actual_xxhash);
  tmp = GetFixedUint32(tmp, length);
  if (value) {
    CopyBytesFrom(tmp, value, *length);
  }
  if (is_corrupted) {
    expected_xxhash.first = XXH64(src + sizeof(actual_xxhash),
                                  sizeof(*length) + *length, xxh_seed_);
    *is_corrupted = (expected_xxhash.first != actual_xxhash.first);
  }
  tmp += *length;

  return tmp;
}

const char* BufferEncoder::DeserializeOplog(const char* src,
                                            uint32_t* key_length,
                                            std::string* oplog,
                                            uint64_t* offset,
                                            bool* is_corrupted) const {
  DCHECK(src);
  DCHECK(key_length);
  DCHECK(offset);

  XXH128_T expected_xxhash{};
  XXH128_T actual_xxhash{};
  const char* tmp = src;

  // TODO(fangliming) : use real 128 hash.
  tmp = GetFixedHash128(tmp, &actual_xxhash);
  tmp = GetFixedUint32(tmp, key_length);
  if (oplog) {
    CopyBytesFrom(tmp, oplog, *key_length);
  }
  tmp += *key_length;
  tmp = GetFixedUint64(tmp, offset);
  if (is_corrupted) {
    expected_xxhash.first =
        XXH64(src + sizeof(actual_xxhash),
              sizeof(*key_length) + *key_length + sizeof(*offset), xxh_seed_);
    *is_corrupted = (expected_xxhash.first != actual_xxhash.first);
  }

  return tmp;
}

}  // namespace mtcache
