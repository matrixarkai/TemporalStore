#include "l2_policy_factory.h"

#include <gflags/gflags.h>

DECLARE_int32(l2_cache_access_interval_ms);
DECLARE_int32(l2_cache_tail_interval_ms);
DECLARE_int32(l2_cache_write_interval_ms);
DECLARE_int32(l2_cache_access_buffer_capacity);
DECLARE_int32(l2_cache_tail_batch_size);
DECLARE_int32(l2_cache_write_buffer_capacity);
DECLARE_int32(max_arc_cache_item_number);

namespace mtcache {

std::unique_ptr<L2CachePolicy> L2CachePolicyFactory::CreateL2CachePolicy(
    L1CacheInterface* l1_cache, CacheInstance* l2_cache,
    std::shared_ptr<noodle::MetricRegistry> l2_registry) {
  auto arc_policy =
      std::make_shared<ReplacementArc>(FLAGS_max_arc_cache_item_number);
  return std::make_unique<L2CachePolicy>(
      l1_cache, l2_cache, arc_policy, FLAGS_l2_cache_access_interval_ms,
      FLAGS_l2_cache_tail_interval_ms, FLAGS_l2_cache_write_interval_ms,
      FLAGS_l2_cache_access_buffer_capacity, FLAGS_l2_cache_tail_batch_size,
      FLAGS_l2_cache_write_buffer_capacity, l2_registry);
}

}  // namespace mtcache
