#include "storage/zoned_store/util.h"

#include "common/logging.h"
#include "storage/zoned_store/index.h"
#include "storage/zoned_store/zoned_store.h"

#include <cstdint>
#include <cstring>

namespace mtcache {
// TODO(fangliming) : add big endian byte order support.
char* PutFixedUint8(char* buf, uint8_t num) {
  memcpy(buf, &num, sizeof(num));
  return buf + sizeof(num);
}

char* PutFixedUint32(char* buf, uint32_t num) {
  memcpy(buf, &num, sizeof(num));
  return buf + sizeof(num);
}

char* PutFixedUint64(char* buf, uint64_t num) {
  memcpy(buf, &num, sizeof(num));
  return buf + sizeof(num);
}

char* PutFixedHash64(char* buf, XXH64_hash_t* num) {
  DCHECK(buf);
  std::memcpy(buf, num, 8);
  return buf + 8;
}

const char* GetFixedHash64(const char* buf, XXH64_hash_t* num) {
  DCHECK(buf);
  DCHECK(num);
  std::memcpy(num, buf, 8);
  return buf + 8;
}

char* PutFixedHash128(char* buf, void* hash_val) {
  DCHECK(buf);
  DCHECK(hash_val);
  std::memcpy(buf, hash_val, 16);
  return buf + 16;
}

const char* GetFixedHash128(const char* buf, void* hash_val) {
  DCHECK(buf);
  DCHECK(hash_val);
  std::memcpy(hash_val, buf, 16);
  return buf + 16;
}

const char* GetFixedUint64(const char* buf, uint64_t* num) {
  DCHECK(num);
  memcpy(num, buf, 8);
  return buf + 8;
}

const char* GetFixedUint32(const char* buf, uint32_t* num) {
  DCHECK(num);
  memcpy(num, buf, 4);
  return buf + 4;
}

const char* GetFixedUint8(const char* buf, uint8_t* num) {
  DCHECK(num);
  memcpy(num, buf, 1);
  return buf + 1;
}

int AlignedTo(uint32_t size, int align_size) {
  int rem = size % align_size;
  if (rem == 0) {
    return size;
  } else {
    return size + (align_size - rem);
  }
}

char* CopyBytesTo(char* dst, const char* src, int len) {
  std::memcpy(dst, src, len);
  return dst + len;
}

const char* CopyBytesFrom(const char* src, std::string* dst, int len) {
  DCHECK(src);
  DCHECK(dst);
  DCHECK_GT(len, 0);
  dst->assign(src, len);
  return src + len;
}

const char* CopyBytesFrom(const char* src, char* dst, int len) {
  DCHECK(dst);
  DCHECK_GT(len, 0);
  std::memcpy(dst, src, len);
  return src + len;
}

std::pair<uint32_t, uint64_t> DecodeColoredPtr(
    Index::SSDColoredPtr colored_ptr) {
  uint32_t size = (colored_ptr >> 7) & 0xfff;
  colored_ptr >>= 19;
  return std::make_pair(size, colored_ptr);
}

uint64_t MaskColoredPtrLBA(Index::SSDColoredPtr old_colored_ptr, uint64_t lba) {
  // High 19bits must be zero.
  DCHECK_EQ(lba >> 45, 0);
  return (old_colored_ptr |
          ((lba << 19) & StorageEngineZonedStore::kSSDLBAFlags));
}

uint64_t MaskColoredPtrSize(Index::SSDColoredPtr old_colored_ptr,
                            uint32_t size) {
  DCHECK_LE(size, 0xFFF);
  return (old_colored_ptr |
          ((size << 7) & StorageEngineZonedStore::kSSDRecordSizeFlags));
}

uint64_t MaskColoredPtrRecordState(Index::SSDColoredPtr old_colored_ptr,
                                   Index::RecordStateType st) {
  DCHECK_LE(st, Index::kMaxCode);
  return (old_colored_ptr | st);
};
};  // namespace mtcache
