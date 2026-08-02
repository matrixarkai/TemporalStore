#pragma once

#include "cache_buffer.h"
#include "common/logging.h"

#include <string>

namespace mtcache {

// StringViewBuffer is a sub-class of CacheBuffer that stores only the size of
// the ssd cache record. It does not hold the value of the ssd record because
// CacheInstance should load the data from ssd into a `StringBuffer` each
// time accessing it. The `size_` field in StringViewBuffer is used to provide
// the hint for replacment policy to do the eviction.
class StringViewBuffer : public CacheBuffer {
 public:
  explicit StringViewBuffer(size_t size) : size_(size) {}

  virtual ~StringViewBuffer() = default;

  // Disable copy constructor and assignment (to match other buffers).
  StringViewBuffer(const StringViewBuffer& other) = delete;
  StringViewBuffer& operator=(const StringViewBuffer& other) = delete;

  // Default move constructor and move assignment.
  StringViewBuffer(StringViewBuffer&& other) = default;
  StringViewBuffer& operator=(StringViewBuffer&& other) = default;

  // Return the address to the data record.
  const char* Data() const override {
    LOG(FATAL) << "StringViewBuffer does not hold the data. It only holds "
                  "the size of data.";
    return nullptr;
  }

  // Return the size of data record in bytes.
  size_t Size() const override { return size_; }

 private:
  // The size of data record.
  size_t size_;
};

using StringViewBufferPtr = StringViewBuffer*;

}  // namespace mtcache
