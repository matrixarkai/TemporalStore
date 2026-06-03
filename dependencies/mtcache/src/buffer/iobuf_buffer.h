#pragma once

#include "cache_buffer.h"

#include <folly/io/IOBuf.h>

namespace mtcache {

// IOBufBuffer is a sub-class of CacheBuffer that stores a folly::IOBuf as
// its value. IOBufBuffer owns the internal IOBuf and releases it
// when the IOBufBuffer is destroyed.
class IOBufBuffer : public CacheBuffer {
 public:
  explicit IOBufBuffer(folly::IOBuf value) : value_(std::move(value)) {}

  virtual ~IOBufBuffer() = default;

  // Disable copy constructor and assignment (to match other buffers).
  IOBufBuffer(const IOBufBuffer& other) = delete;
  IOBufBuffer& operator=(const IOBufBuffer& other) = delete;

  IOBufBuffer(IOBufBuffer&& other) = default;
  IOBufBuffer& operator=(IOBufBuffer&& other) = default;

  // Return the address to the data record.
  const char* Data() const override {
    return reinterpret_cast<const char*>(value_.data());
  }

  // Return the size of data record in bytes.
  size_t Size() const override { return value_.length(); }

 private:
  folly::IOBuf value_;
};

}  // namespace mtcache
