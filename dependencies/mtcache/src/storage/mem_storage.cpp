#include "mem_storage.h"

#include "common/logging.h"

#include <folly/Likely.h>
#include <folly/hash/Checksum.h>

namespace mtcache {

// The size of key length (a uint32 is used to represent key length)
static constexpr uint32_t kKeyLen = sizeof(uint32_t);

// The size of value length (a uint32 is used to represent value length)
static constexpr uint32_t kValueLen = sizeof(uint32_t);

// The memory layout of data in allocator is shown in the comment of function
// 'Put'. We can get the key from the memory along side the value.
noodle::Result<void, CacheError> LogBasedAllocatorGCEventListenerBase::OnGCCopy(
    const char* old_ptr, const char* new_ptr) {
  uint32_t val_size;
  uint32_t key_size;

  memcpy(&val_size, old_ptr, kValueLen);
  memcpy(&key_size, old_ptr + kValueLen, kKeyLen);
  const char* key_start = old_ptr + kValueLen + kKeyLen + val_size;
  std::string key(key_start, key_size);

  // Create new RawBuffer with new_ptr from GC thread.
  const char* old_data = old_ptr + kValueLen + kKeyLen;
  const char* new_data = new_ptr + kValueLen + kKeyLen;
  auto new_buffer = std::make_shared<RawBuffer>(const_cast<char*>(new_data),
                                                val_size, engine_, is_pmem_);
  new_buffer->SetKey(key);
  auto res = cb_->Update(key, old_data,
                         std::static_pointer_cast<CacheBuffer>(new_buffer));
  if (UNLIKELY(!res.IsOk())) {
    // For now only kCacheBufferNotFound or kCacheReplaceMismatch should be
    // throw when error occurs.
    DCHECK(res.GetError() == &Errors::kCacheBufferNotFound ||
           res.GetError() == &Errors::kCacheReplaceMismatch);

    if (res.GetError() == &Errors::kCacheBufferNotFound) {
      // The cache buffer can not be found. Maybe this CacheBuffer is deleted
      // from the CacheInstance's index when the old CacheBuffer is evicted by
      // replacement policy during GC.
      LOG_EVERY_SECOND(WARNING)
          << "Got " << google::COUNTER
          << "th CacheBufferNotFound warning in GCCopy, key=[" << key
          << "]. Deleting the copied data, size=" << (val_size + key_size);
    } else if (res.GetError() == &Errors::kCacheReplaceMismatch) {
      // The data pointer in cache buffer is not equal to old data pointer.
      // Maybe the cache has beed updated by someone else. So we should
      // cancel our update operation.
      LOG(WARNING) << "Old data pointer in CacheBuffer mismatch for key=["
                   << key << "] after GCCopy. Deleting the copied data...";
    }

    // Because the LogBasedAllocator will revoke the space for new_ptr
    // when OnGCCopy returns an error, we can not free the space here.
    // As a result we must reset new_buffer here to prevent it from calling
    // StorageEngine::Delete at destruction.
    new_buffer->Reset();
  }

  // When cache buffer is updated successfully, the old cache buffer's
  // ref count will finally decreases to zero when all readers exit. Then
  // it will call StoageEngine::Delete to free the space used by old data.
  // So we do not need to free the space of old_ptr here.
  return res;
}

uint32_t MemStorage::ComputeCRC(const std::string& key, const char* val,
                                size_t val_sz) {
  // Compute crc of two uint32 together.
  uint32_t lens[2];
  lens[0] = static_cast<uint32_t>(val_sz);
  lens[1] = static_cast<uint32_t>(key.size());
  uint32_t crc = folly::crc32c(reinterpret_cast<const uint8_t*>(lens),
                               2 * sizeof(uint32_t));
  // Compute crc of value with the startingChecksum from above.
  crc = folly::crc32c(reinterpret_cast<const uint8_t*>(val), val_sz, crc);
  // Compute crc of key with the startingChecksum from above.
  crc = folly::crc32c(reinterpret_cast<const uint8_t*>(key.data()), key.size(),
                      crc);
  return crc;
}

noodle::Result<char*, CacheError> MemStorage::DoPut(CacheAllocator* allocator,
                                                    const std::string& key,
                                                    const char* val,
                                                    size_t val_sz,
                                                    uint32_t crc) {
  size_t alloc_size = kValueLen + kKeyLen + val_sz + key.size();
  auto ptr_res = allocator->Allocate(alloc_size);
  if (UNLIKELY(!ptr_res.IsOk())) {
    // reduce the log frequency for `AllocatorOutOfSpace`
    if (ptr_res.GetError() == &Errors::kAllocatorOutOfSpace) {
      LOG_EVERY_SECOND(ERROR)
          << "Got " << google::COUNTER
          << "th AllocatorOutOfSpace failure to alloc memory from allocator";
    } else {
      LOG(ERROR) << "Fail to alloc memory from allocator: "
                 << ptr_res.GetError()->GetMessage();
    }
    return ptr_res.GetError();
  }
  char* ptr = ptr_res.Get();
  // write value length at the beginning of the memory space
  uint32_t sz = static_cast<uint32_t>(val_sz);
  memcpy(ptr, &sz, kValueLen);
  ptr += kValueLen;
  // write key length after value length
  sz = static_cast<uint32_t>(key.size());
  memcpy(ptr, &sz, kKeyLen);
  ptr += kKeyLen;
  // write value after key length
  memcpy(ptr, val, val_sz);
  ptr += val_sz;
  // write key after value
  memcpy(ptr, key.data(), key.size());

  // Since Seal() uses a std::memory_order_release to prevent stores being
  // reordered after Seal(), we do not need to put a memory barrier here.
  auto seal_res = allocator->Seal(ptr_res.Get(), alloc_size, crc);
  if (UNLIKELY(!seal_res.IsOk())) {
    LOG(ERROR) << "Fail to seal cache record at "
               << reinterpret_cast<const void*>(ptr_res.Get());
    return seal_res.GetError();
  }
  return ptr_res.Get() + kValueLen + kKeyLen;
}

noodle::Result<char*, CacheError> MemStorage::DoPut(CacheAllocator* allocator,
                                                    const std::string& key,
                                                    const char* val,
                                                    size_t val_sz) {
  return DoPut(allocator, key, val, val_sz, 0);
}

noodle::Result<void, CacheError> MemStorage::DoDelete(CacheAllocator* alloc,
                                                      char* data, size_t size) {
  DCHECK(nullptr != data);
  uint32_t val_len;
  uint32_t key_len;
  memcpy(&val_len, data - kKeyLen - kValueLen, kValueLen);
  DCHECK(val_len == size);
  memcpy(&key_len, data - kKeyLen, kKeyLen);
  char* start_ptr = data - kKeyLen - kValueLen;
  size_t mem_size = kValueLen + kKeyLen + val_len + key_len;
  auto res = alloc->Free(start_ptr, mem_size);
  if (UNLIKELY(!res.IsOk())) {
    LOG(ERROR) << "Fail to free memory to allocator: "
               << res.GetError()->GetMessage() << ", size=" << size
               << ", address=" << reinterpret_cast<void*>(data);
    return res.GetError();
  }
  return {};
}

std::string_view MemStorage::GetKeyFromData(const char* data_ptr) {
  uint32_t val_len;
  uint32_t key_len;
  memcpy(&val_len, data_ptr, kValueLen);
  memcpy(&key_len, data_ptr + kValueLen, kKeyLen);
  return std::string_view(data_ptr + kValueLen + kKeyLen + val_len, key_len);
}

std::shared_ptr<RawBuffer> MemStorage::CreateCacheBufferFromData(
    const char* data_ptr, StorageEngine* engine, bool async) {
  uint32_t val_len;
  memcpy(&val_len, data_ptr, kValueLen);
  return std::make_shared<RawBuffer>(data_ptr + kValueLen + kKeyLen, val_len,
                                     engine, async);
}

}  // namespace mtcache
