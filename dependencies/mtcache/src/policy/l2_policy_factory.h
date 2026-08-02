#pragma once

#include "cache_instance.h"
#include "l1_interface.h"
#include "l2_policy.h"

#include <memory>

namespace mtcache {

class L2CachePolicyFactory {
 public:
  static std::unique_ptr<L2CachePolicy> CreateL2CachePolicy(
      L1CacheInterface* l1_cache, CacheInstance* l2_cache,
      std::shared_ptr<noodle::MetricRegistry> l2_registry);
};

}  // namespace mtcache
