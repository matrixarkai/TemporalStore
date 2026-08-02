#include "mtcache.h"

#include "unified_cache.h"

namespace mtcache {

std::unique_ptr<Cache<std::string, folly::IOBuf>> MTCacheBuilder::BuildCache(
    const CacheOptions& opts) {
  auto* unified_cache = new UnifiedCache(opts);
  return std::unique_ptr<Cache<std::string, folly::IOBuf>>(unified_cache);
}

std::unique_ptr<ZeroCopyCache<std::string, folly::IOBuf>>
MTCacheBuilder::BuildZeroCopyCache(const CacheOptions& opts) {
  auto* unified_cache = new UnifiedCache(opts);
  return std::unique_ptr<ZeroCopyCache<std::string, folly::IOBuf>>(
      unified_cache);
}

}  // namespace mtcache
