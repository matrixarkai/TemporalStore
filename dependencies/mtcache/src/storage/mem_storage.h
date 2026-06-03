#pragma once

#include "allocator/allocator.h"
#include "buffer/raw_buffer.h"
#include "storage_engine.h"

namespace mtcache {

// GCEventListener when log-based allocator is used.
class LogBasedAllocatorGCEventListenerBase
    : public LogBasedAllocatorGCEventListener {
 public:
  LogBasedAllocatorGCEventListenerBase(StorageEngine* engine,
                                       StorageEngine::GCCopyCallback* cb,
                                       bool is_pmem)
      : engine_(engine), cb_(cb), is_pmem_(is_pmem) {}

  ~LogBasedAllocatorGCEventListenerBase() override = default;

  noodle::Result<void, CacheError> OnGCCopy(const char* old_ptr,
                                            const char* new_ptr) override;

 private:
  StorageEngine* engine_;
  StorageEngine::GCCopyCallback* cb_;
  bool is_pmem_;
};

// MemStorage defines the common functions used by DramStorageEngine
// and PMemStorageEngine when log-based allocator or pool-based allocator  is
// used.
class MemStorage {
 public:
  static uint32_t ComputeCRC(const std::string& key, const char* val,
                             size_t val_sz);

  // Put the `data` (whose size is `size` and key is `key`) to `engine_`.
  // The key will be put alongside the value.
  //
  // The memory layout for the cache record when putting a value(with key)
  // is shown below. There are 8 bytes space before the value, 4B standing for
  // the value size and 4B standing for key size. Then comes the space for value
  // and the space for key.
  //
  //    value size(4B)
  //      |
  //      |  key size(4B)
  //      |    |                    key
  //      |    |     value          |
  //      |    |     |              |
  //      v    v     v              v
  //    +-------------------------------------+
  //    |    |    |              |            |
  //    +-------------------------------------+
  //
  // with crc, for pmem only
  static noodle::Result<char*, CacheError> DoPut(CacheAllocator* allocator,
                                                 const std::string& key,
                                                 const char* val, size_t val_sz,
                                                 uint32_t crc);

  // without crc, for dram only
  static noodle::Result<char*, CacheError> DoPut(CacheAllocator* allocator,
                                                 const std::string& key,
                                                 const char* val,
                                                 size_t val_sz);

  static noodle::Result<void, CacheError> DoDelete(CacheAllocator* alloc,
                                                   char* data, size_t size);

  // Get the string_view of key from data_ptr. The input `data_ptr` must be
  // a valid char array matching the memory layout in `DoPut`.
  static std::string_view GetKeyFromData(const char* data_ptr);

  // Create a shared_ptr of RawBuffer from the input `data_ptr`, which must
  // match the memory layout descripted in `DoPut`. The param `async` will
  // decide whether to call Delete of AsyncDelete to free memory when RawBuffer
  // is destroyed.
  static std::shared_ptr<RawBuffer> CreateCacheBufferFromData(
      const char* data_ptr, StorageEngine* engine, bool async);

  MemStorage() = delete;
};

}  // namespace mtcache
