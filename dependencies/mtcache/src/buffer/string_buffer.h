#pragma once

#include "cache_buffer.h"

#include <string>

namespace mtcache {

// StringBuffer is a sub-class of CacheBuffer that stores a std::string as
// its value. StringBuffer owns the internal string and releases the string
// when the StringBuffer is destroyed.
class StringBuffer : public CacheBuffer {
 public:
  // Construct StringBuffer with a string value.
  explicit StringBuffer(std::string value) : value_(std::move(value)) {}

  virtual ~StringBuffer() = default;

  // Disable copy constructor and assignment (to match other buffers).
  StringBuffer(const StringBuffer& other) = delete;
  StringBuffer& operator=(const StringBuffer& other) = delete;

  // Default move constructor and move assignment will move std::string.
  StringBuffer(StringBuffer&& other) = default;
  StringBuffer& operator=(StringBuffer&& other) = default;

  // Return the address to the data record.
  const char* Data() const override { return value_.data(); }

  // Return the size of data record in bytes.
  size_t Size() const override { return value_.size(); }

 private:
  // The value of data record.
  std::string value_;
};

using StringBufferPtr = StringBuffer*;

}  // namespace mtcache
