#pragma once

#include <cstddef>
#include <memory>
#include <string>

namespace mtcache {

// CacheBuffer is a buffer of cache data returned by a cache storage engine.
// Different storage engines should use different implementations of
// CacheBuffer. For example, PMEM storage engine should use RefObjBuffer
// to avoid data copy and track the reference count of a cache record, while
// SSD storage engine will use a StringBuffer.
class CacheBuffer {
 public:
  // The destructor of CacheBuffer must be virtual because the derived class
  // will release the data in different ways.
  virtual ~CacheBuffer() {}

  // Returns a const pointer to the data in the buffer.
  // Note that the ownership of the data belongs to CacheBuffer. Callers should
  // not use the data pointer when CacheBuffer is destroyed.
  virtual const char* Data() const = 0;

  // Return the size of the data in the buffer.
  virtual size_t Size() const = 0;

  // Set the key of the cache buffer.
  void SetKey(const std::string& key) { key_ = key; }

  void SetKey(std::string&& key) { key_ = std::move(key); }

  // Return key of the cache buffer.
  const std::string& Key() const { return key_; }

 protected:
  // key of the cached buffer.
  std::string key_;
};

using CacheBufferPtr = CacheBuffer*;
using CacheBufferSharedPtr = std::shared_ptr<CacheBuffer>;

}  // namespace mtcache
