#pragma once

#include "cache_instance.h"
#include "l1_interface.h"

#include <string>

namespace mtcache {

class CacheInstance;

// A simple implementation of L1CacheInterface, combines DRAM cache and PMEM
// cache.
class L1CacheImplement : public L1CacheInterface {
 public:
  // L1CacheImplement constructor
  // dram_instance is the L1 cache instance with a DRAM storage engine
  // pmem_instance is the L1 cache instance with a PMEM storage engine
  // Note that L1CacheImplement does not have ownership of the dram/pmem
  // instances.
  // l2_registry is the metric registry for L2 cache instance (i.e. SSD
  // instance) from unified cache.
  L1CacheImplement(CacheInstance* dram_instance, CacheInstance* pmem_instance,
                   std::shared_ptr<noodle::MetricRegistry> l2_registry)
      : dram_instance_(dram_instance),
        pmem_instance_(pmem_instance),
        l2_registry_(l2_registry) {
    CHECK(dram_instance_);
    if (l2_registry_) {
      l2_pulls_counter_ = l2_registry_->MustRegister<noodle::AtomicCounter>(
          noodle::MetricId("pulls"), std::make_shared<noodle::AtomicCounter>());
    }
    // pmem cache instance may not exist.
  }
  ~L1CacheImplement() = default;

  CacheBufferSharedPtr GetBypassReplacementPolicy(
      const std::string& key) override;

 private:
  CacheInstance* dram_instance_;
  CacheInstance* pmem_instance_;

  // l2_registry_ is the metric registry where the l2cache registers metrics
  std::shared_ptr<noodle::MetricRegistry> l2_registry_;
  // l2_pulls_counter_ is the counter metric to track how many times l2cache
  // pulls data from l2cache
  std::shared_ptr<noodle::AtomicCounter> l2_pulls_counter_;
};

}  // namespace mtcache
