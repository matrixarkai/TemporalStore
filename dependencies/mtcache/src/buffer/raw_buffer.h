#pragma once

#include "cache_buffer.h"
#include "storage/storage_engine.h"

namespace mtcache {

// RawBuffer is a sub-class of CacheBuffer that uses a raw pointer to locate
// the cache record. RawBuffer owns the data buffer passed in and will
// free the buffer at destruction time.
class RawBuffer : public CacheBuffer {
 public:
  // Construct RawBuffer with buffer pointer. RawBuffer does not copy
  // the data buffer, instead it just takes the ownership of the data buffer.
  // So the caller should not free the data buffer after passing the data
  // buffer pointer to RawBuffer. If `async` is true, the destructor will call
  // StorageEngine::AsyncDelete to free the data it holds. Otherwise it will
  // call StorageEngine::Delete. See the comments for `AsyncDelete` at
  // cache/storage/storage_engine.h for details.
  RawBuffer(const char* buffer, size_t size, StorageEngine* engine, bool async)
      : raw_buffer_(buffer),
        size_(size),
        storage_engine_(engine),
        async_(async) {
    CHECK(buffer != nullptr);
  }

  // De-allocate raw_buffer_ when destroying CacheBuffer.
  virtual ~RawBuffer() {
    if (UNLIKELY(storage_engine_ == nullptr)) {
      delete[] raw_buffer_;
      return;
    }
    if (async_) {
      storage_engine_->AsyncDelete(this);
    } else {
      storage_engine_->Delete(this);
    }
  }

  // Disable copy constructor and assignment to avoid double free of
  // raw_buffer_ when destroying this class.
  RawBuffer(const RawBuffer& other) = delete;
  RawBuffer& operator=(const RawBuffer& other) = delete;

  // Move constructor and move assignment should de-allocate the raw_buffer_
  // in old RawBuffer. Otherwise the old RawBuffer will free old raw_buffer_.
  RawBuffer(RawBuffer&& other) noexcept {
    std::swap(size_, other.size_);
    std::swap(raw_buffer_, other.raw_buffer_);
    std::swap(storage_engine_, other.storage_engine_);
    std::swap(async_, other.async_);
    std::swap(key_, other.key_);
  }

  RawBuffer& operator=(RawBuffer&& other) {
    if (LIKELY(storage_engine_ != nullptr)) {
      if (async_) {
        storage_engine_->AsyncDelete(this);
      } else {
        storage_engine_->Delete(this);
      }
    } else {
      delete[] raw_buffer_;
    }
    raw_buffer_ = other.raw_buffer_;
    size_ = other.size_;
    async_ = other.async_;
    key_ = std::move(other.key_);
    storage_engine_ = other.storage_engine_;
    other.raw_buffer_ = nullptr;
    other.storage_engine_ = nullptr;
    return *this;
  }

  // Return the data pointer to the data record.
  const char* Data() const override { return raw_buffer_; }

  size_t Size() const override { return size_; }

  // Clear the content of this RawBuffer. Note that this method will not call
  // StorageEngine::Delete to free the internal buffer.
  void Reset() {
    raw_buffer_ = nullptr;
    size_ = 0;
    storage_engine_ = nullptr;
    key_.clear();
  }

 private:
  // The address of the data record.
  const char* raw_buffer_{nullptr};
  // The size of the data record.
  size_t size_{0};

  // The cache storage engine this cache buffer belongs to. When this cache
  // buffer is destroyed, it should call the storage_engine_'s Delete method
  // to free the buffer space.
  StorageEngine* storage_engine_{nullptr};

  // Whether to delete this RawBuffer synchronously or asynchronously. Usually
  // this field should be true only for PMEM cache buffer.
  bool async_{false};
};

using RawBufferPtr = RawBuffer*;

}  // namespace mtcache
