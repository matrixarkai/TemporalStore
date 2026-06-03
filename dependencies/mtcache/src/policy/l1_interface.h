#pragma once

#include "buffer/cache_buffer.h"

#include <string>

namespace mtcache {

// L1 cache is a concept relative to L2 cache. The cache faster than external
// device(SSD) is L1 cache.
//
// In the implementation, L1 cache comprised of DRAM cache and PMEM cache. And
// L2 cache is SSD cache.
class L1CacheInterface {
 public:
  L1CacheInterface() = default;
  virtual ~L1CacheInterface() = default;

  // GetBypassReplacementPolicy looks up a cache buffer with the specified key.
  // This method should ONLY be used by the L2ARC(SSD) instance to pull
  // interested buffers while bypassing the replacement policy of DRAM and PMEM
  // cache instances.
  virtual CacheBufferSharedPtr GetBypassReplacementPolicy(
      const std::string& key) = 0;
};

}  // namespace mtcache
