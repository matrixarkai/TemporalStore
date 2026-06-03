#include "pmem_storage.h"

#include "allocator/alloc_utils.h"
#include "buffer/raw_buffer.h"
#include "common/logging.h"
#include "mem_storage.h"

#include <string>

DECLARE_bool(cache_pmem_enable_async_write);
DECLARE_string(pmem_allocator_type);
DECLARE_int32(num_threads_recover_pmem);

namespace mtcache {

using CacheBufResult = noodle::Result<CacheBufferSharedPtr, CacheError>;

// The kEstimateItemSize is used to estimate the total number of records (
// num_records = total_pmem_file_size / kEstimateItemSize),
// which is used to init the capacity of the ConcurrentHashMap when recovering
// PMEM records.
static constexpr size_t kEstimateItemSize = 1024 * 1024;

StorageEnginePMem::StorageEnginePMem(
    uint64_t capacity, const std::vector<std::string>& pmem_paths,
    StorageEngine::GCCopyCallback* gc_cb,
    std::shared_ptr<noodle::MetricRegistry> registry) {
  CHECK(!pmem_paths.empty() || !pmem_paths[0].empty())
      << "PMem path can not be empty!";
  allocator_type_ = ParseAllocatorType(FLAGS_pmem_allocator_type);
  metric_registry_ = std::move(registry);
  if (allocator_type_ == AllocatorType::kLogBasedAllocator) {
    CHECK(gc_cb != nullptr);
    listener_ = std::make_unique<LogBasedAllocatorGCEventListenerBase>(
        this, gc_cb, true);
    const auto& common_executor = CacheExecutor::GetCommonExecutor();
    const auto& pmem_executors = CacheExecutor::GetPmemExecutors();
    dispatcher_ = std::make_unique<PMemDispatcher>(
        allocator_type_, capacity, pmem_paths, common_executor, pmem_executors,
        listener_.get(), metric_registry_);
  } else if (allocator_type_ == AllocatorType::kPoolBasedAllocator) {
    LOG(FATAL)
        << "PMEM storage engine does not support pool-based allocator for "
           "now because pool-based allocator does not support numa-aware "
           "feature";
  } else {
    // JeAllocator
    LOG(FATAL) << "PMEM does not support JeAllocator";
  }
}

bool StorageEnginePMem::Start() {
  DCHECK(!initialized_);
  RegisterStorageEngineMetrics();
  dispatcher_->Start();
  initialized_ = true;
  return true;
}

bool StorageEnginePMem::Stop() {
  DCHECK(initialized_);
  initialized_ = false;
  dispatcher_->Stop();
  LOG(INFO) << "Stop StorageEnginePMem success.";
  return true;
}

void StorageEnginePMem::TEST_JoinPmemWriteExecutor() {
  dispatcher_->TEST_JoinPmemWriteExecutor();
}

CacheBufResult StorageEnginePMem::Get(const std::string& key) {
  // Get operation of a CacheBuffer is only supported by SSD storage engine
  // and the CacheBuffer must be a StringBuffer.
  // PMEM storage engine uses RawBuffer, which hold the
  // value data inside the buffer and do not need to call Get() function.
  LOG(WARNING) << "'Get' method in StorageEnginePMem is not supported.";
  return &Errors::kStorageUnsupported;
}

[[deprecated("Use StorageEnginePMem::AsyncPut instead.")]] CacheBufResult
StorageEnginePMem::Put(const std::string& key, folly::IOBuf value) {
  DCHECK(initialized_);
  // We add a temporary feature to disable async pmem write to debug the GC
  // issue.
  // TODO(david.gong) This WARNING log can be uncommented after the GC issue is
  // resolved.
  // LOG(WARNING) << "`Put` in PMEM storage engine is deprecated, please use "
  //                "`AsyncPut` instead.";
  uint32_t crc = MemStorage::ComputeCRC(
      key, reinterpret_cast<const char*>(value.data()), value.length());
  CacheAllocator* alloc = dispatcher_->GetAllocator(nullptr);
  DCHECK(alloc != nullptr);
  auto put_res =
      MemStorage::DoPut(alloc, key, reinterpret_cast<const char*>(value.data()),
                        value.length(), crc);
  if (UNLIKELY(!put_res.IsOk())) {
    // reduce the output frequency for `AllocatorOutOfSpace`
    if (put_res.GetError() != &Errors::kAllocatorOutOfSpace) {
      LOG(ERROR) << "Fail to write data in StorageEnginePMem::Put, key=" << key
                 << ", error msg: " << put_res.GetError()->GetMessage();
    } else {
      LOG_EVERY_SECOND(ERROR)
          << "Got " << google::COUNTER
          << "th AllocatorOutOfSpace error in StorageEnginePMem::Put, key="
          << key;
    }
    return put_res.GetError();
  }
  // We still use a sync buffer in this sync Put.
  auto buf =
      std::make_shared<RawBuffer>(put_res.Get(), value.length(), this, false);
  buf->SetKey(key);
  return std::static_pointer_cast<CacheBuffer>(buf);
}

CacheBufResult StorageEnginePMem::TEST_PutToNuma(
    const std::string& key, std::unique_ptr<folly::IOBuf> value,
    int32_t numa_id) {
  DCHECK(initialized_);
  uint32_t crc = MemStorage::ComputeCRC(
      key, reinterpret_cast<const char*>(value->data()), value->length());
  CacheAllocator* alloc = dispatcher_->TEST_GetAllocator(numa_id);
  DCHECK(alloc != nullptr);
  auto put_res = MemStorage::DoPut(alloc, key,
                                   reinterpret_cast<const char*>(value->data()),
                                   value->length(), crc);
  if (UNLIKELY(!put_res.IsOk())) {
    LOG(ERROR) << "Fail to write data in StorageEnginePMem::Put, key=" << key
               << ", error msg: " << put_res.GetError()->GetMessage();
    return put_res.GetError();
  }
  auto buf =
      std::make_shared<RawBuffer>(put_res.Get(), value->length(), this, false);
  buf->SetKey(key);
  return std::static_pointer_cast<CacheBuffer>(buf);
}

folly::SemiFuture<CacheBufResult> StorageEnginePMem::AsyncPut(
    CacheBufferSharedPtr buffer, AsyncPutCb cb) {
  DCHECK(initialized_);
  // Compute crc in current thread rather than then the `writer_thread` in
  // dispatcher because the `writer_thread` should focus on writing data.
  uint32_t crc =
      MemStorage::ComputeCRC(buffer->Key(), buffer->Data(), buffer->Size());
  auto write_func = [crc, buffer](CacheAllocator* alloc) {
    return MemStorage::DoPut(alloc, buffer->Key(), buffer->Data(),
                             buffer->Size(), crc);
  };

  auto cb_func = [ buffer, put_cb = std::move(cb),
                   engine = this ](noodle::Result<char*, CacheError> put_res)
                     ->CacheBufResult {
    if (!put_res.IsOk()) {
      // reduce the output frequency for `AllocatorOutOfSpace`
      if (put_res.GetError() != &Errors::kAllocatorOutOfSpace) {
        LOG(ERROR) << "Fail to AysncPut data in StorageEnginePMem, key="
                   << buffer->Key()
                   << ", error msg: " << put_res.GetError()->GetMessage();
      } else {
        LOG_EVERY_SECOND(ERROR)
            << "Got " << google::COUNTER
            << "th AllocatorOutOfSpace error to AsyncPut "
            << "data in StorageEnginePMem, key=" << buffer->Key();
      }
      put_cb(put_res.GetError());
      return put_res.GetError();
    }
    auto new_buffer = std::make_shared<RawBuffer>(put_res.Get(), buffer->Size(),
                                                  engine, true);
    new_buffer->SetKey(buffer->Key());
    put_cb(std::static_pointer_cast<CacheBuffer>(new_buffer));
    return std::static_pointer_cast<CacheBuffer>(new_buffer);
  };

  return dispatcher_->PushTask(
      AsyncWriteTask(std::move(write_func), std::move(cb_func)));
}

folly::SemiFuture<CacheBufResult> StorageEnginePMem::AsyncDelete(
    CacheBufferPtr buffer) {
  if (UNLIKELY(!initialized_)) {
    // The storage engine has stopped, so the requests are just ignored.
    return folly::makeFuture<CacheBufResult>(
        noodle::Result<CacheBufferSharedPtr, CacheError>(
            &Errors::kStorageEngineUninitialized));
  }
  VLOG(3) << "AsyncDeleting PMEM CacheBuffer, ptr=" << buffer
          << ", key=" << buffer->Key();
  char* addr = const_cast<char*>(buffer->Data());
  auto del_func = [ addr, sz = buffer->Size() ](CacheAllocator * alloc)
                      ->noodle::Result<char*, CacheError> {
    auto res = MemStorage::DoDelete(alloc, addr, sz);
    if (UNLIKELY(!res.IsOk())) {
      return res.GetError();
    }
    return addr;
  };

  auto cb_func =
      [addr](noodle::Result<char*, CacheError> del_res) -> CacheBufResult {
    if (UNLIKELY(!del_res.IsOk())) {
      LOG(ERROR) << "Fail to AsyncDelete data in PMEM engine, addr="
                 << reinterpret_cast<void*>(addr)
                 << ", error msg: " << del_res.GetError()->GetMessage();
      return del_res.GetError();
    }
    return CacheBufferSharedPtr(nullptr);
  };
  return dispatcher_->PushTask(
      AsyncWriteTask(std::move(del_func), std::move(cb_func), addr));
}

[[deprecated("Use StorageEnginePMem::AsyncDelete instead.")]] noodle::Result<
    void, CacheError>
StorageEnginePMem::Delete(CacheBufferPtr buffer) {
  if (UNLIKELY(!initialized_)) {
    // The storage engine has stopped, so the requests are just ignored.
    return &Errors::kStorageEngineUninitialized;
  }
  // TODO(dbc) This WARNING log can be uncommented after the mem leak issue is
  // resolved.
  // LOG(WARNING) << "`Delete` in PMEM storage engine is deprecated, please use
  // "
  //                 "`AsyncDelete` instead.";
  VLOG(3) << "Deleting PMEM CacheBuffer, ptr=" << buffer
          << ", key=" << buffer->Key();
  auto fut = AsyncDelete(buffer);
  auto res = std::move(fut).get();
  if (res.IsOk()) {
    return {};
  } else {
    LOG(ERROR) << "Fail to Delete data in PMEM engine, addr="
               << reinterpret_cast<const void*>(buffer->Data())
               << ", error msg: " << res.GetError()->GetMessage();
    return res.GetError();
  }
}

noodle::Result<void, CacheError> StorageEnginePMem::Reset() {
  // TODO(dongbenchao) If allocator has a 'Reset' function, we should call
  // allocator_->Reset() here.
  return {};
}

struct PmemRecoverStats StorageEnginePMem::TEST_GetRecoverStats() {
  PmemRecoverStats recover_stats;
  recover_stats.total_bytes_ = recover_total_bytes_->GetValue();
  recover_stats.valid_bytes_ = recover_valid_bytes_->GetValue();
  recover_stats.freed_bytes_ = recover_freed_bytes_->GetValue();
  recover_stats.corrupted_bytes_ = recover_corrupted_bytes_->GetValue();
  return recover_stats;
}

noodle::Result<void, CacheError> StorageEnginePMem::RecoverData(
    RecoverDataCallback* callback) {
  DCHECK(allocator_type_ == AllocatorType::kLogBasedAllocator)
      << "Only log-based allocator supports recovering pmem data!";
  DCHECK(callback != nullptr);
  // GCController must be paused before recovering pmem data.
  // Although `SetPauseGC` can not pause existing GC jobs, there is no existing
  // GC jobs before `RecoverData` because `RecoverData` is called inside
  // cache `Start` and cache can not serve requests before finish recovery.
  //
  // TODO(dbc) If we make PMEM cache recovery as a background job where cache
  // can serve requests before finish recovery, we should refine the code here.
  const auto& gc_ctls = dispatcher_->GetGCControllers();
  for (auto& gc_ctl : gc_ctls) {
    gc_ctl->SetPauseGC(true);
  }

  // Estimate total number of cache items according to the capacity.
  // We need to set the size of ConcurrentHashMap in advance to avoid frequent
  // rehash during recovery.
  const auto& allocs = dispatcher_->GetAllocators();
  size_t total_file_size = 0;
  for (size_t i = 0; i < allocs.size(); ++i) {
    auto* log_allocator =
        reinterpret_cast<LogBasedMemoryAllocatorPMem*>(allocs[i].get());
    total_file_size += log_allocator->GetRecoverableFileSize();
  }
  size_t estimate_items = total_file_size / kEstimateItemSize;

  auto recover_listener = std::make_unique<PmemAllocatorRecoverListenerImpl>(
      this, callback, estimate_items);
  auto recover_executor = std::make_unique<CPUNumaThreadPoolExecutor>(
      FLAGS_num_threads_recover_pmem, true,
      std::make_shared<folly::NamedThreadFactory>("RecoverPmemExecutor"));
  auto start_time = std::chrono::steady_clock::now();
  PmemRecoverStats recover_stats;
  for (size_t i = 0; i < allocs.size(); ++i) {
    auto* log_allocator =
        reinterpret_cast<LogBasedMemoryAllocatorPMem*>(allocs[i].get());
    log_allocator->Recover(recover_listener.get(), recover_executor.get(),
                           &recover_stats);
  }
  // Wait all recovery tasks to complete
  recover_executor->join();
  auto scan_time = std::chrono::steady_clock::now();
  auto num_valid_records = recover_listener->FinishRecover();
  std::chrono::duration<double, std::milli> recover_duration =
      std::chrono::steady_clock::now() - start_time;
  std::chrono::duration<double, std::milli> scan_duration =
      scan_time - start_time;

  // Resume GCController after recovering pmem data.
  for (auto& gc_ctl : gc_ctls) {
    gc_ctl->SetPauseGC(false);
  }

  LOG(INFO) << "PMEM recovery stats:\n\tScanTime(ms): " << scan_duration.count()
            << ", TotalTime(ms): " << recover_duration.count()
            << "\n\tTotal scanned bytes: " << recover_stats.total_bytes_
            << "\n\tTotal valid bytes(including duplicated keys): "
            << recover_stats.valid_bytes_
            << "\n\tTotal freed bytes: " << recover_stats.freed_bytes_
            << "\n\tTotal corrupted bytes: " << recover_stats.corrupted_bytes_
            << "\n\tTotal valid num records(non-duplicated): "
            << num_valid_records;

  recover_time_counter_->SetValue(
      static_cast<int64_t>(recover_duration.count()));
  recover_records_counter_->SetValue(num_valid_records);
  recover_total_bytes_->SetValue(recover_stats.total_bytes_);
  recover_valid_bytes_->SetValue(recover_stats.valid_bytes_);
  recover_freed_bytes_->SetValue(recover_stats.freed_bytes_);
  recover_corrupted_bytes_->SetValue(recover_stats.corrupted_bytes_);
  return {};
}

uint64_t PmemAllocatorRecoverListenerImpl::FinishRecover() {
  uint64_t num_valid_records = 0;
  uint64_t num_dup = 0;
  // 1. First, free the duplicated records during scanning.
  folly::Optional<const char*> dup_item = dup_records_.try_dequeue();
  while (dup_item.has_value()) {
    num_dup++;
    // Set async flag to true so that the CacheBuffer will call AsyncDelete
    // to release resource when it is destroyed.
    // We do not need to set the key for buf here since it is going to be
    // deleted.
    auto buf =
        MemStorage::CreateCacheBufferFromData(dup_item.value(), engine_, true);
    buf.reset();
    dup_item = dup_records_.try_dequeue();
  }

  // 2. Invoke the callback `cb_` to fill the index in ReplacementPolicy
  for (auto it = record_map_.begin(); it != record_map_.end(); ++it) {
    if (it->second != nullptr) {
      ++num_valid_records;
      auto buffer = MemStorage::CreateCacheBufferFromData(
          it->second, engine_, FLAGS_cache_pmem_enable_async_write);
      buffer->SetKey(std::string(it->first));
      cb_->OnRecoverData(buffer->Key(), buffer);
    }
  }

  LOG(INFO) << "Recover pmem cache success!\n\tvalid records: "
            << num_valid_records << ", duplicated records: " << num_dup;
  return num_valid_records;
}

bool PmemAllocatorRecoverListenerImpl::OnScanRecord(const char* addr,
                                                    size_t len) {
  std::string_view key = MemStorage::GetKeyFromData(addr);

  // Store the key-data_addr pair in the map. This does not mean the record
  // should be inserted into the final index in CacheInstance. Maybe the
  // recover process will meet the same key in the following scanning, which
  // means this key should be discarded.
  auto insert_res = record_map_.insert(std::make_pair(key, addr));
  if (insert_res.second) {
    // Insert success, which means the key did not exist
    return false;
  }

  // Insert failed because this key has existed. Update it to nullptr and
  // push existed ptr(s) to the dup_records_.
  DCHECK(insert_res.second == false);
  DCHECK(insert_res.first->first == key);
  const char* existed_ptr = insert_res.first->second;

  dup_records_.enqueue(addr);
  const char* desired_ptr = nullptr;
  if (existed_ptr != nullptr) {
    // Set the existed data address to nullptr. Because the existed_ptr will
    // be added to dup_records_ and freed soon, we set it to nullptr here to
    // prevent double-free.
    auto assign_res =
        record_map_.assign_if_equal(key, existed_ptr, desired_ptr);
    if (assign_res.has_value()) {
      // FIXME(dbc) MTCache use folly-2021.03.08, but this version has a bug
      // about `assign_if_equal` of ConcurrentHashMap. It returns the iterator
      // to the old key-value when `assign_if_equal` success. In the newer
      // versions this bug has been fixed and this function will return the
      // iterator to the new key-value.
      // If we update folly version in the future, we should update this
      // line of code to:
      // DCHECK(assign_res.value()->second == nullptr);
      DCHECK(assign_res.value()->second == existed_ptr);
      // Update concurrent hashmap success. Put the old addr from the map
      // into dup_records_ queue.
      dup_records_.enqueue(existed_ptr);
    } else {
      // Another current thread updates the map and dup_records_ queue, this
      // thread does not need to do updates any more.
    }
  }
  return true;
}

void StorageEnginePMem::RegisterStorageEngineMetrics() {
  recover_time_counter_ = metric_registry_->MustRegister<noodle::AtomicGauge>(
      noodle::MetricId("pmem_recover_time_ms"),
      std::make_shared<noodle::AtomicGauge>());
  recover_records_counter_ =
      metric_registry_->MustRegister<noodle::AtomicCounter>(
          noodle::MetricId("num_pmem_recover_records"),
          std::make_shared<noodle::AtomicCounter>());
  recover_total_bytes_ = metric_registry_->MustRegister<noodle::AtomicGauge>(
      noodle::MetricId("num_pmem_total_bytes"),
      std::make_shared<noodle::AtomicGauge>());
  recover_valid_bytes_ = metric_registry_->MustRegister<noodle::AtomicGauge>(
      noodle::MetricId("num_pmem_valid_bytes"),
      std::make_shared<noodle::AtomicGauge>());
  recover_freed_bytes_ = metric_registry_->MustRegister<noodle::AtomicGauge>(
      noodle::MetricId("num_pmem_freed_bytes"),
      std::make_shared<noodle::AtomicGauge>());
  recover_corrupted_bytes_ =
      metric_registry_->MustRegister<noodle::AtomicGauge>(
          noodle::MetricId("num_pmem_corrupted_bytes"),
          std::make_shared<noodle::AtomicGauge>());
}

}  // namespace mtcache
