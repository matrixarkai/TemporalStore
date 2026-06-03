#include "simple_storage.h"

#include "buffer/raw_buffer.h"
#include "common/logging.h"

namespace mtcache {

noodle::Result<CacheBufferSharedPtr, CacheError> StorageEngineSimple::Put(
    const std::string& key, folly::IOBuf value) {
  CHECK(initialized_);
  // The new method used by allocator may throw a std::bad_alloc exception
  // if there is insufficient memory. But we can assume it will not happen
  // in StorageEngineSimple.
  auto ptr_result = allocator_->Allocate(value.length());
  CHECK(ptr_result.IsOk());
  memcpy(ptr_result.Get(), value.data(), value.length());
  auto res = std::make_shared<RawBuffer>(ptr_result.Get(), value.length(), this,
                                         false);
  res->SetKey(key);
  return std::static_pointer_cast<CacheBuffer>(res);
}

noodle::Result<CacheBufferSharedPtr, CacheError> StorageEngineSimple::Get(
    const std::string& key) {
  return &Errors::kNotImplemented;
}

noodle::Result<void, CacheError> StorageEngineSimple::Delete(
    CacheBufferPtr buffer) {
  if (!initialized_) {
    return &Errors::kStorageEngineUninitialized;
  }
  auto raw_buffer = dynamic_cast<RawBufferPtr>(buffer);

  if (nullptr == raw_buffer) {
    return &Errors::kStorageInvalidCacheBuffer;
  }

  // This method should only be called by the destructor of CacheBuffer to avoid
  // invalid memory access after the buffer is freed.
  allocator_->Free(const_cast<char*>(raw_buffer->Data()), raw_buffer->Size());
  DLOG(INFO) << "Delete a CacheBuffer!";
  this->TEST_IncreaseDeleteCompletedCount();
  return {};
}

noodle::Result<void, CacheError> StorageEngineSimple::Reset() {
  // Allocated memory will be freed automatically when the related Cache
  // buffers are destructed. So the reset method is a no-op
  return {};
}

noodle::Result<void, CacheError> StorageEngineSimple::RecoverData(
    RecoverDataCallback* callback) {
  return &Errors::kNotImplemented;
}

}  // namespace mtcache
