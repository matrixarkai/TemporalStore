#include "l1_imp.h"

namespace mtcache {

CacheBufferSharedPtr L1CacheImplement::GetBypassReplacementPolicy(
    const std::string& key) {
  // Increase l2_pulls_counter if it exists.
  if (l2_pulls_counter_) {
    l2_pulls_counter_->Increase();
  }
  CacheBufferSharedPtr buffer = std::shared_ptr<CacheBuffer>(nullptr);
  buffer = dram_instance_->GetBypassReplacementPolicy(key);
  // As the caller does NOT know the value size of the cache buffer, we try to
  // look it up in the PMEM instance if it is not found in the DRAM instance,
  // regardless of the data placement type between DRAM and PMEM.
  if (!buffer && pmem_instance_) {
    buffer = pmem_instance_->GetBypassReplacementPolicy(key);
  }
  return buffer;
}

}  // namespace mtcache
