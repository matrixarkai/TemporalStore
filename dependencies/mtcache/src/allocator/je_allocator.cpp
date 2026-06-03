#include "allocator/je_allocator.h"

#include "allocator/alloc_utils.h"
#include "common/logging.h"

#include <folly/Likely.h>

namespace mtcache {

JeAllocator::JeAllocator(size_t capacity_bytes,
                         std::shared_ptr<noodle::MetricRegistry> registry)
    : capacity_bytes_(capacity_bytes) {
  CHECK_GE(capacity_bytes, 0) << "JeAllocator capacity must be larger than 0!";
  RegisterMetrics(registry);
}

noodle::Result<char*, CacheError> JeAllocator::Allocate(size_t len) {
  if (UNLIKELY(len == 0)) {
    alloc_zero_counter_->Increase();
    return &Errors::kAllocatorRequestIsZero;
  }

  auto freed_bytes = freed_bytes_counter_->GetValue();
  auto allocated_bytes = allocated_bytes_counter_->GetValue();
  if (allocated_bytes - freed_bytes + len > capacity_bytes_) {
    no_space_counter_->Increase();
    return &Errors::kAllocatorOutOfSpace;
  }
  void* res = malloc(len);

  if (UNLIKELY(res == nullptr)) {
    // reduce the log frequency for `AllocatorOutOfSpace`
    LOG_EVERY_SECOND(ERROR) << "Got " << google::COUNTER
                            << "th AllocatorOutOfSpace failure to allocate "
                            << "memory via jemalloc, len=" << len;
    no_space_counter_->Increase();
    return &Errors::kAllocatorOutOfSpace;
  }
  allocated_bytes_counter_->Increase(len);
  occupied_bytes_counter_->SetValue(allocated_bytes - freed_bytes + len);
  return reinterpret_cast<char*>(res);
}

noodle::Result<void, CacheError> JeAllocator::Free(char* ptr, size_t len) {
  // TODO(dbc) We can not check DoubleFree or InvalidAddress when using
  // jemalloc.
  free(ptr);
  freed_bytes_counter_->Increase(len);
  return {};
}

bool JeAllocator::Contains(const char* ptr) const {
  LOG(FATAL) << "jemalloc does not support Contains!";
}

noodle::Result<void, CacheError> JeAllocator::Seal(char* ptr, size_t len,
                                                   uint32_t crc32) {
  // Unlike LogAllcator, JeAllocator does not need to seal anything.
  return {};
}

noodle::Result<void, CacheError> JeAllocator::Seal(char* ptr) { return {}; }

noodle::Result<AllocatorStats, CacheError> JeAllocator::GetStats() const {
  auto freed = freed_bytes_counter_->GetValue();
  auto allocated = allocated_bytes_counter_->GetValue();
  AllocatorStats stats = {
      .num_allocated_bytes = static_cast<size_t>(allocated),
      .num_freed_bytes = static_cast<size_t>(freed),
      .num_occupied_bytes = static_cast<size_t>(allocated - freed)};
  return stats;
}

noodle::Result<size_t, CacheError> JeAllocator::Capacity() const {
  return capacity_bytes_;
}

AllocatorStats JeAllocator::TEST_GetAllocMetrics() const {
  AllocatorStats stats;
  stats.num_allocated_bytes = allocated_bytes_counter_->GetValue();
  stats.num_freed_bytes = freed_bytes_counter_->GetValue();
  stats.num_occupied_bytes = occupied_bytes_counter_->GetValue();
  return stats;
}

void JeAllocator::RegisterMetrics(
    std::shared_ptr<noodle::MetricRegistry> registry) {
  CHECK(registry != nullptr)
      << "Metric registry for JeAllocator can not be null";
  // Register basic stats metrics.
  allocated_bytes_counter_ = registry->MustRegister<noodle::AtomicCounter>(
      noodle::MetricId("allocator_allocated_bytes"),
      std::make_shared<noodle::AtomicCounter>());
  freed_bytes_counter_ = registry->MustRegister<noodle::AtomicCounter>(
      noodle::MetricId("allocator_freed_bytes"),
      std::make_shared<noodle::AtomicCounter>());
  occupied_bytes_counter_ = registry->MustRegister<noodle::AtomicCounter>(
      noodle::MetricId("allocator_occupied_bytes"),
      std::make_shared<noodle::AtomicCounter>());

  // Register error metrics.
  const std::string error_id = "allocator_error";
  const std::string err_type_tag = "err_type";
  no_space_counter_ = registry->MustRegister<noodle::AtomicCounter>(
      noodle::MetricId(error_id, {{err_type_tag, "out_of_space"}}),
      std::make_shared<noodle::AtomicCounter>());
  alloc_zero_counter_ = registry->MustRegister<noodle::AtomicCounter>(
      noodle::MetricId(error_id, {{err_type_tag, "request_is_zero"}}),
      std::make_shared<noodle::AtomicCounter>());
}

}  // namespace mtcache
