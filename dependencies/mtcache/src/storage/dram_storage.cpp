#include "dram_storage.h"

#include "allocator/alloc_utils.h"
#include "allocator/je_allocator.h"
#include "common/logging.h"
#include "mem_storage.h"

#include <gflags/gflags.h>

// Space reserved for GC in DRAM log-based allocator
DECLARE_uint64(cache_dram_gc_reserved);
// Max number of threads for DRAM cache
DECLARE_int32(cache_dram_max_thread_num);
// Force to trigger GC
DECLARE_bool(cache_force_gc);
DECLARE_uint64(pool_based_allocator_obj_len);
DECLARE_string(dram_allocator_type);

namespace mtcache {

StorageEngineDram::StorageEngineDram(
    uint64_t capacity, GCCopyCallback* gc_cb,
    std::shared_ptr<noodle::MetricRegistry> registry) {
  allocator_type_ = ParseAllocatorType(FLAGS_dram_allocator_type);
  if (allocator_type_ == AllocatorType::kLogBasedAllocator) {
    CHECK(gc_cb != nullptr);
    // LogBaseAllocator should have at least 2 chunks because the AsyncWriter
    // needs at least one extra chunk.
    if (capacity <= kLogChunkSize) {
      capacity += kLogChunkSize;
    }
    listener_ = std::make_unique<LogBasedAllocatorGCEventListenerBase>(
        this, gc_cb, false);
    allocator_ = std::make_unique<LogBasedMemoryAllocatorDram>(
        listener_.get(), capacity, FLAGS_cache_dram_gc_reserved,
        FLAGS_cache_dram_max_thread_num, registry);
    // create a cache gc instance and bind the allocator
    gc_instance_ = std::make_unique<StorageGCController>(
        reinterpret_cast<LogBasedMemoryAllocator*>(allocator_.get()),
        FLAGS_cache_force_gc, registry);
  } else if (allocator_type_ == AllocatorType::kPoolBasedAllocator) {
    allocator_ = std::make_unique<PoolBasedMemoryAllocatorDram>(
        capacity, FLAGS_cache_dram_max_thread_num,
        FLAGS_pool_based_allocator_obj_len);
  } else {
    // JeAllocator
    allocator_ = std::make_unique<JeAllocator>(capacity, registry);
  }
  const auto& executor = CacheExecutor::GetCommonExecutor();
  async_writer_ =
      std::make_unique<AsyncWriter>(allocator_.get(), executor, executor);
}

bool StorageEngineDram::Start() {
  DCHECK(!initialized_);
  if (allocator_type_ == AllocatorType::kLogBasedAllocator) {
    DCHECK(gc_instance_ != nullptr);
    // start a gc instance in the DRAM storage engine
    gc_instance_->Start();
  }
  initialized_ = true;
  return true;
}

bool StorageEngineDram::Stop() {
  DCHECK(initialized_);
  initialized_ = false;
  if (allocator_type_ == AllocatorType::kLogBasedAllocator) {
    gc_instance_->Stop();
  }
  async_writer_->Stop();
  LOG(INFO) << "Stop DRAMMStorageEngine success.";
  return true;
}

noodle::Result<CacheBufferSharedPtr, CacheError> StorageEngineDram::Get(
    const std::string& key) {
  // Get operation of a CacheBuffer is only supported by SSD storage engine
  // and the CacheBuffer must be a StringBuffer.
  // DRAM storage engine uses RawBuffer, which hold the
  // value data inside the buffer and do not need to call Get() function.
  LOG(WARNING) << "'Get' method in StorageEngineDram is not supported.";
  return &Errors::kStorageUnsupported;
}

noodle::Result<CacheBufferSharedPtr, CacheError> StorageEngineDram::Put(
    const std::string& key, folly::IOBuf value) {
  DCHECK(initialized_);
  auto ptr_res = MemStorage::DoPut(allocator_.get(), key,
                                   reinterpret_cast<const char*>(value.data()),
                                   value.length());
  if (UNLIKELY(!ptr_res.IsOk())) {
    // reduce the output frequency for `AllocatorOutOfSpace`
    if (ptr_res.GetError() != &Errors::kAllocatorOutOfSpace) {
      LOG(ERROR) << "Fail to write data in StorageEngineDram::Put, key=" << key
                 << ", error msg: " << ptr_res.GetError()->GetMessage();
    } else {
      LOG_EVERY_SECOND(ERROR)
          << "Got " << google::COUNTER
          << "th AllocatorOutOfSpace error in StorageEngineDram::Put, key="
          << key;
    }
    return ptr_res.GetError();
  }

  auto buf =
      std::make_shared<RawBuffer>(ptr_res.Get(), value.length(), this, false);
  buf->SetKey(key);
  return std::static_pointer_cast<CacheBuffer>(buf);
}

folly::SemiFuture<noodle::Result<CacheBufferSharedPtr, CacheError>>
StorageEngineDram::AsyncPut(CacheBufferSharedPtr buffer, AsyncPutCb cb) {
  DCHECK(initialized_);

  auto write_func = [buffer](CacheAllocator* alloc) {
    return MemStorage::DoPut(alloc, buffer->Key(), buffer->Data(),
                             buffer->Size());
  };

  auto cb_func = [ buffer, put_cb = std::move(cb),
                   engine = this ](noodle::Result<char*, CacheError> put_res)
                     ->noodle::Result<CacheBufferSharedPtr, CacheError> {
    if (!put_res.IsOk()) {
      // reduce the output frequency for `AllocatorOutOfSpace`
      if (put_res.GetError() != &Errors::kAllocatorOutOfSpace) {
        LOG(ERROR) << "Fail to AysncPut data in StorageEngineDram, key="
                   << buffer->Key()
                   << ", error msg: " << put_res.GetError()->GetMessage();
      } else {
        LOG_EVERY_SECOND(ERROR)
            << "Got " << google::COUNTER
            << "th AllocatorOutOfSpace error to AsyncPut "
            << "data in StorageEngineDram, key=" << buffer->Key();
      }
      put_cb(put_res.GetError());
      return put_res.GetError();
    }
    // DramStorage does no support AsyncDelete, mark it as false
    auto new_buffer = std::make_shared<RawBuffer>(put_res.Get(), buffer->Size(),
                                                  engine, false);
    new_buffer->SetKey(buffer->Key());
    put_cb(std::static_pointer_cast<CacheBuffer>(new_buffer));
    return std::static_pointer_cast<CacheBuffer>(new_buffer);
  };

  return async_writer_->AsyncWrite(
      AsyncWriteTask(std::move(write_func), std::move(cb_func)));
}

noodle::Result<void, CacheError> StorageEngineDram::Delete(
    CacheBufferPtr buffer) {
  if (UNLIKELY(!initialized_)) {
    // The storage engine has stopped, so the requests are just ignored.
    return &Errors::kStorageEngineUninitialized;
  }
  VLOG(3) << "Deleting DRAM CacheBuffer, ptr=" << buffer
          << ", key=" << buffer->Key();
  return MemStorage::DoDelete(
      allocator_.get(), const_cast<char*>(buffer->Data()), buffer->Size());
}

noodle::Result<void, CacheError> StorageEngineDram::Reset() {
  // DramStorageEngine needs to do nothing on reset.
  // TODO(dongbenchao) If allocator has a 'Reset' function, we should call
  // allocator_->Reset() here.
  return {};
}

noodle::Result<void, CacheError> StorageEngineDram::RecoverData(
    RecoverDataCallback* callback) {
  LOG(ERROR) << "StorageEngineDram does not support recovering data";
  return &Errors::kStorageUnsupported;
}

}  // namespace mtcache
