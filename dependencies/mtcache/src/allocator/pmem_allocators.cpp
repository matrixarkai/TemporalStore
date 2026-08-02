#include "allocator/pmem_allocators.h"

#include "allocator/alloc_utils.h"
#include "common/logging.h"

#include <folly/futures/Future.h>
#include <folly/hash/Checksum.h>

#include <filesystem>

namespace mtcache {

void PmemRecoverStats::AddChunkStats(const ChunkRecoverStats& stats) {
  valid_bytes_.fetch_add(stats.valid_bytes, std::memory_order_relaxed);
  freed_bytes_.fetch_add(stats.freed_bytes, std::memory_order_relaxed);
  corrupted_bytes_.fetch_add(stats.corrupted_bytes, std::memory_order_relaxed);
}

PmemRecoverStats::PmemRecoverStats(const PmemRecoverStats& a) {
  total_bytes_.store(a.total_bytes_.load(std::memory_order_relaxed),
                     std::memory_order_relaxed);
  valid_bytes_.store(a.valid_bytes_.load(std::memory_order_relaxed),
                     std::memory_order_relaxed);
  freed_bytes_.store(a.freed_bytes_.load(std::memory_order_relaxed),
                     std::memory_order_relaxed);
  corrupted_bytes_.store(a.corrupted_bytes_.load(std::memory_order_relaxed),
                         std::memory_order_relaxed);
}

// The file name of a chunk file on PMEM is like:
//   /xxx/00000000000000000001.pmem_chunk
static std::string BuildPmemFilePath(const std::string& dir, ChunkID chunk_id) {
  return fmt::format("{}/{:020d}.pmem_chunk", dir, chunk_id);
}

static std::string BuildPmemFileName(ChunkID chunk_id) {
  return fmt::format("{:020d}.pmem_chunk", chunk_id);
}

LogBasedMemoryAllocatorPMem::LogBasedMemoryAllocatorPMem(
    std::string data_path, FlushPolicy flush_policy,
    size_t mini_batch_flush_bytes,
    LogBasedAllocatorGCEventListener* gc_event_listener, size_t capacity_bytes,
    size_t gc_reserved_bytes, size_t max_thread_num,
    std::shared_ptr<noodle::MetricRegistry> registry, int numa_id)
    : LogBasedMemoryAllocatorBase(
          [data_path](void* addr, ChunkID chunk_id, size_t object_len,
                      size_t alignment) {
            return PMemAllocateObject_v2(
                addr, BuildPmemFilePath(data_path, chunk_id), object_len);
          },
          [](void* addr, size_t len) -> noodle::Result<void, CacheError> {
            // Can not free VM because log-based allocator reserved large range
            // of VM in advance and it will release it at destruction. If we
            // free it here, it will cause holes with the VM and make the
            // releasing process fail.
            LOG(FATAL) << "Can not free object in log-based allocator";
            return &Errors::kNotImplemented;
          },
          gc_event_listener, capacity_bytes, gc_reserved_bytes, max_thread_num,
          registry, numa_id),
      flush_policy_(flush_policy),
      mini_batch_flush_bytes_(mini_batch_flush_bytes) {
  // If the path for pmem does not exists, we must create the directoy.
  if (!std::filesystem::exists(data_path)) {
    std::error_code err_code;
    bool res = std::filesystem::create_directories(data_path, err_code);
    CHECK(res) << "Fail to create dirctory for pmem cache allocator, path=["
               << data_path
               << "], error msg is: " << google::StrError(err_code.value());
  }
  data_path_ = std::move(data_path);
  // Validate chunk files under data_path_ and remove invalid files.
  recoverable_filenames_ = FilterValidChunkFiles();
}

noodle::Result<char*, CacheError> LogBasedMemoryAllocatorPMem::Allocate(
    size_t len) {
  // need kChecksumLen extra bytes to write checksum
  return LogBasedMemoryAllocatorBase::Allocate(len + kChecksumLen);
}

noodle::Result<void, CacheError> LogBasedMemoryAllocatorPMem::Free(char* ptr,
                                                                   size_t len) {
  return LogBasedMemoryAllocatorBase::Free(ptr, len + kChecksumLen);
}

std::vector<std::string> LogBasedMemoryAllocatorPMem::FilterValidChunkFiles() {
  std::vector<std::string> valid_files;
  std::vector<std::string> invalid_files;

  ChunkID max_chunk_id =
      (capacity_and_gc_bytes_ + kLogChunkSize - 1) / kLogChunkSize;
  std::string max_chunk_str = BuildPmemFileName(max_chunk_id);

  LOG(INFO) << "Scaning all PMEM files under [" << data_path_
            << "], expected_file_size=" << kLogChunkSize
            << ", max_chunk_id=" << max_chunk_id;
  // Step 1: Get the PMEM filenames under `data_path_`. Only files
  // with kLogChunkSize are counted. Otherwise it will cause fatal error in
  // LogBasedAllocator.
  // We check filesize here because kLogChunkSize may be configured as a gflags
  // argument in the future.
  auto size_valid_files =
      GetPmemFileName(data_path_, kLogChunkSize, &invalid_files);
  valid_files.reserve(size_valid_files.size());

  // Step 2: Filter the filenames from step 1. Only files with chunk id less
  // than max_chunk_id are valid.
  for (size_t i = 0; i < size_valid_files.size(); ++i) {
    if (size_valid_files[i] < max_chunk_str) {
      valid_files.push_back(std::move(size_valid_files[i]));
    } else {
      invalid_files.push_back(std::move(size_valid_files[i]));
    }
  }

  LOG(INFO) << "Found " << valid_files.size()
            << " valid chunk files with expected size&&name and "
            << invalid_files.size()
            << " invalid files with unexpected_size||name. You can set vlog"
               " level to 4 to print filenames of all valid/invalid files";
  if (VLOG_IS_ON(4)) {
    std::stringstream valid_fname_ss;
    for (const auto& n : valid_files) {
      valid_fname_ss << n << "\n";
    }
    VLOG(4) << "Valid chunk files:\n" << valid_fname_ss.str();
    std::stringstream invalid_fname_ss;
    for (const auto& n : invalid_files) {
      invalid_fname_ss << n << "\n";
    }
    VLOG(4) << "Invalid chunk files:\n" << invalid_fname_ss.str();
  }

  if (!invalid_files.empty()) {
    LOG(INFO) << "Deleting the chunk files with unexpected size/name...";
    // Delete the invalid pmem files to free pmem space.
    for (const auto& invalid_name : invalid_files) {
      std::string fpath = data_path_ + "/" + invalid_name;
      if (!DeletePmemFile(fpath)) {
        // If the invalid files can not be deleted, it will impact the following
        // AllcoateChunk function. In this case users must clean these files
        // manually.
        LOG(FATAL) << "Fail to delete invalid pmem file, path=" << fpath;
      }
    }
  }
  return valid_files;
}

std::vector<ChunkID> LogBasedMemoryAllocatorPMem::RecoverChunkId(
    const std::vector<std::string>& fnames) {
  std::vector<ChunkID> chunk_ids;
  for (const auto& fname : fnames) {
    // Get chunk_id from file_name
    size_t npos = fname.find_last_of('.');
    // The fname is in the format of "00000000001.pmem_chunk".
    // Set the stoull's base to 10 explicitly to avoid potential octal base.
    chunk_ids.emplace_back(std::stoull(fname.substr(0, npos), nullptr, 10));
  }
  // Recycle unused ChunkID
  std::sort(chunk_ids.begin(), chunk_ids.end());
  ChunkID pre_id = 0;
  for (size_t i = 0; i < chunk_ids.size(); ++i) {
    while (pre_id < chunk_ids[i]) {
      recycled_chunk_id_.push_front(pre_id++);
    }
    pre_id = chunk_ids[i] + 1;
  }
  // next_chunk_id_ must be the highest id, so we set it to the largest value
  // in chunk_ids plus 1.
  next_chunk_id_.store(chunk_ids.back() + 1, std::memory_order_relaxed);
  return chunk_ids;
}

ChunkRecoverStats LogBasedMemoryAllocatorPMem::ScanChunk(
    GeneralChunk* chunk, PmemAllocatorRecoverListener* listener) {
  ChunkRecoverStats stats;
  char* begin = chunk->data;
  char* end = begin + kLogChunkSize;
  uint32_t mem_obj_len = 0;
  for (; begin + kHeaderLen < end; begin += (kHeaderLen + mem_obj_len)) {
    memcpy(&mem_obj_len, begin, sizeof(mem_obj_len));
    if (mem_obj_len == kChunkStopMark) {
      // 1. Meet EOF of the whole chunk and finish scanning this chunk.
      VLOG(2) << "Recover Pmem Cache: Meet EOF at " << static_cast<void*>(begin)
              << ", ChunkId=" << chunk->meta.id;

      int64_t sz = static_cast<int64_t>(end - begin);
      chunk->meta.num_freed_bytes.fetch_add(sz, std::memory_order_relaxed);
      stats.freed_bytes += sz;
      break;
    }
    if (mem_obj_len & kTombstoneMask) {
      // 2. This record has been freed. Skip this record and continue to next
      //    record in this chunk.
      VLOG(3) << "Recover Pmem Cache: Meet freed record at "
              << static_cast<void*>(begin) << ", ChunkId=" << chunk->meta.id;
      // remove tombstone bit to get the real physical length
      mem_obj_len &= kRecordLenMask;
      chunk->meta.num_freed_bytes.fetch_add(kHeaderLen + mem_obj_len,
                                            std::memory_order_relaxed);
      stats.freed_bytes += (kHeaderLen + mem_obj_len);
      continue;
    }
    if (begin + kHeaderLen + mem_obj_len > end || mem_obj_len <= kChecksumLen) {
      // 3. The value of mem_obj_len is corrupted/invalid and we do not know
      //    the position of this&next records. Just finish scanning this chunk.
      VLOG(1) << "Recover Pmem Cache: illegal record length, length="
              << mem_obj_len << ", addr=" << static_cast<void*>(begin)
              << ", ChunkId=" << chunk->meta.id;
      // Add kChunkStopMark here to seal the chunk.
      const uint32_t stop_mark = kChunkStopMark;
      memcpy(begin, &stop_mark, sizeof(stop_mark));

      int64_t sz = static_cast<int64_t>(end - begin);
      chunk->meta.num_freed_bytes.fetch_add(sz, std::memory_order_relaxed);
      stats.corrupted_bytes += sz;
      break;
    }

    // 4. Valid mem_obj_len, check crc of this record.
    uint32_t crc_expect;
    size_t payload_len = mem_obj_len - sizeof(crc_expect);
    memcpy(&crc_expect, begin + kHeaderLen + payload_len, kChecksumLen);
    uint32_t crc_actual = folly::crc32c(
        reinterpret_cast<uint8_t*>(begin + kHeaderLen), payload_len);
    if (crc_expect != crc_actual) {
      // 4.1 This record's checksum doesn't match. Mark the corrupted record
      //     as a freed record.
      uint32_t mask_obj_len = mem_obj_len | kTombstoneMask;
      memcpy(begin, &mask_obj_len, sizeof(mask_obj_len));
      chunk->meta.num_freed_bytes.fetch_add(kHeaderLen + mem_obj_len,
                                            std::memory_order_relaxed);
      stats.corrupted_bytes += (kHeaderLen + mem_obj_len);
      VLOG(1) << "Recover Pmem Cache: found corrupted record with unexpected "
                 "CRC, addr="
              << static_cast<void*>(begin) << ", ChunkID=" << chunk->meta.id;
      // Though this record is corrupted, we can continue scanning this chunk.
      continue;
    } else {
      // 4.2 Correct checksum. Call callback of recover listener.
      //     Note that this record may not be reinserted into cache index
      //     finally because there may be duplicated keys among recovered
      //     records. In this case the duplicated records are all invalid
      //     records and will be freed at the last step of PMEM recovery.
      listener->OnScanRecord(begin + kHeaderLen, payload_len);
      RefChunk(chunk);
      stats.valid_bytes += (kHeaderLen + mem_obj_len);
      VLOG(3) << "Recover pmem cache: found one valid record at "
              << static_cast<void*>(begin + kHeaderLen)
              << ", len=" << payload_len << ", ChunkID=" << chunk->meta.id;
    }
  }  // end of for-each record in this chunk

  // If none of records in this chunk can be recovered, set this chunk to freed
  // state and insert it into the free_chunk_list.
  if (chunk->meta.ref_cnt.load(std::memory_order_relaxed) == 0) {
    chunk->meta.num_allocated_bytes.store(0, std::memory_order_relaxed);
    chunk->meta.num_freed_bytes.store(0, std::memory_order_relaxed);
    {  // add to free list
      std::lock_guard<std::mutex> guard(alloc_mtx_);
      free_chunks_.emplace_back(chunk);
    }

    // update global stats at last
    DCHECK(num_occupied_bytes_ >= kLogChunkSize);
    num_occupied_bytes_.fetch_sub(kLogChunkSize, std::memory_order_relaxed);
  } else {
    // This chunk is not empty, we set this chunk's state to SealedState.
    chunk->meta.num_allocated_bytes.store(kLogChunkSize,
                                          std::memory_order_relaxed);
  }
  return stats;
}

void LogBasedMemoryAllocatorPMem::Recover(
    PmemAllocatorRecoverListener* listener, CPUNumaThreadPoolExecutor* executor,
    PmemRecoverStats* recover_stats) {
  DCHECK(listener != nullptr);
  DCHECK(executor != nullptr);

  // FIXME(dbc) In fact we should scan all the chunk files even the chunk file
  // exceeds the max chunk id because these chunks may contain records with
  // the same key as the records in the valid chunks. In that case those
  // valid records should also be invalidated. But since MTCache do not have
  // two different records with the same key (records are never updated),
  // we can simply delete these invalid chunks files before recovery.
  std::vector<ChunkID> chunk_ids = RecoverChunkId(recoverable_filenames_);
  recover_stats->total_bytes_.fetch_add(
      kLogChunkSize * recoverable_filenames_.size(), std::memory_order_relaxed);

  for (size_t i = 0; i < chunk_ids.size(); ++i) {
    std::string full_path = BuildPmemFilePath(data_path_, chunk_ids[i]);
    void* addr = ChunkId2Addr(chunk_ids[i]);
    auto open_res = PMemMapFile(addr, full_path, kLogChunkSize);
    if (!open_res.IsOk()) {
      // This chunk can not be opened or mapped. Maybe current user does not
      // have the permission to read/write this file.
      // Try to delete this file. If it can not be deleted, the cache must
      // fail to start.
      LOG(ERROR) << "PMEM file can not be opened&maped, path=" << full_path
                 << ", trying to delete this file.";
      if (unlink(full_path.c_str()) != 0) {
        LOG(FATAL) << "Can not delete PMEM file, path=" << full_path
                   << ", errno=" << errno;
      }
      // recycle this ChunkID
      recycled_chunk_id_.push_front(chunk_ids[i]);
      continue;
    }
    CHECK(open_res.Get() == addr);
    GeneralChunk* chunk =
        new GeneralChunk(chunk_ids[i], reinterpret_cast<char*>(addr));
    num_occupied_bytes_.fetch_add(kLogChunkSize, std::memory_order_relaxed);
    VLOG(4) << "Recovering " << i << "th chunk, ChunkID=" << chunk_ids[i];
    auto& p = chunks_[chunk_ids[i]];
    DCHECK(p == nullptr);
    p.store(chunk, std::memory_order_release);
    folly::via(executor, [this, chunk, listener, recover_stats]() {
      auto chunk_stats = ScanChunk(chunk, listener);
      recover_stats->AddChunkStats(chunk_stats);
    });
  }
}

void LogBasedMemoryAllocatorPMem::OnSeal(GeneralChunk* chunk, char* payload_ptr,
                                         size_t payload_len, uint32_t crc32) {
  if (payload_len == 0) {
    // If the caller do not provide the value lenth and crc32, we must compute
    // crc32 here. The will lead to reading from PMEM and may be slower than
    // compute crc32 in DRAM.
    CHECK(crc32 == 0) << "payload_len and crc32 should both be zero";
    char* head_ptr = payload_ptr - kHeaderLen;
    uint32_t val_with_crc32_len;
    memcpy(&val_with_crc32_len, head_ptr, sizeof(kHeaderLen));
    payload_len = val_with_crc32_len - kChecksumLen;
    crc32 = folly::crc32c(reinterpret_cast<uint8_t*>(payload_ptr), payload_len);
  }
  // write checksum after the data field
  memcpy(payload_ptr + payload_len, &crc32, kChecksumLen);

  // flush data if necessary
  if (flush_policy_ == FlushPolicy::kInstantFlush) {
    // There is extra kHeaderLen (4B) space before data field to store the
    // length of data field. See LogBasedMemoryAllocatorBase.
    PMemPersist(payload_ptr - kHeaderLen,
                kHeaderLen + payload_len + sizeof(crc32));
  } else if (flush_policy_ == FlushPolicy::kMiniBatchFlush) {
    size_t alloc_bytes =
        chunk->meta.num_allocated_bytes.load(std::memory_order_relaxed);
    // only flush when `unsealed_cnt` equals to 1, which means all memory
    // objects in the chunk are sealed(immutable) and flush-able
    unsigned int unsealed_cnt =
        chunk->unsealed_cnt.load(std::memory_order_acquire);
    if (unsealed_cnt == 1) {
      size_t flushed_bytes = chunk->flushed_bytes;
      assert(flushed_bytes % mini_batch_flush_bytes_ == 0);
      if (alloc_bytes - flushed_bytes >= mini_batch_flush_bytes_) {
        PMemPersist(chunk->data + flushed_bytes, mini_batch_flush_bytes_);
        chunk->flushed_bytes += mini_batch_flush_bytes_;
      }
    } else {
      LOG(ERROR) << "`chunk->unsealed_cnt` should always be 1 under current "
                    "storage engine implementation. Ignore the message if this "
                    "runs in UT. Now it's "
                 << unsealed_cnt;
    }
  }
}

void LogBasedMemoryAllocatorPMem::OnSealChunk(GeneralChunk* chunk) {
  if (flush_policy_ == FlushPolicy::kMiniBatchFlush) {
    // flush the rest
    size_t alloc_bytes =
        chunk->meta.num_allocated_bytes.load(std::memory_order_relaxed);
    size_t flushed_bytes = chunk->flushed_bytes;
    assert(flushed_bytes % mini_batch_flush_bytes_ == 0 &&
           flushed_bytes <= alloc_bytes);
    PMemPersist(chunk->data + flushed_bytes, alloc_bytes - flushed_bytes);
    chunk->flushed_bytes = 0;
  }
}

size_t LogBasedMemoryAllocatorPMem::GetRecoverableFileSize() const {
  return recoverable_filenames_.size() * kLogChunkSize;
}

PoolBasedMemoryAllocatorPMem::PoolBasedMemoryAllocatorPMem(
    std::string data_path, FlushPolicy flush_policy, size_t capacity_bytes,
    size_t max_thread_num, size_t obj_len)
    : PoolBasedMemoryAllocatorBase(
          [data_path](ChunkID chunk_id, size_t object_len, size_t alignment) {
            return PMemAllocateObject(
                fmt::format("{}/{:020d}.pmem_chunk", data_path, chunk_id),
                object_len, alignment);
          },
          PMemFreeObject, capacity_bytes, max_thread_num, obj_len),
      flush_policy_(flush_policy) {
  // If the path for pmem does not exists, we must create the directoy.
  if (!std::filesystem::exists(data_path)) {
    bool res = std::filesystem::create_directories(data_path);
    CHECK(res) << "Fail to create dirctory for pmem path";
  }
  CHECK(flush_policy != FlushPolicy::kMiniBatchFlush)
      << "Pool-based allocator does not support kMiniBatchFlush flush policy!";
}

noodle::Result<char*, CacheError> PoolBasedMemoryAllocatorPMem::Allocate(
    size_t len) {
  // need kChecksumLen extra bytes to write checksum
  return PoolBasedMemoryAllocatorBase::Allocate(len + kChecksumLen);
}

noodle::Result<void, CacheError> PoolBasedMemoryAllocatorPMem::Free(
    char* ptr, size_t len) {
  return PoolBasedMemoryAllocatorBase::Free(ptr, len + kChecksumLen);
}

void PoolBasedMemoryAllocatorPMem::OnSeal(char* payload_ptr, size_t payload_len,
                                          uint32_t crc32) {
  if (payload_len == 0) {
    // If the caller do not provide the value lenth and crc32, we must compute
    // crc32 here. The will lead to reading from PMEM and may be slower than
    // compute crc32 in DRAM.
    CHECK(crc32 == 0) << "payload_len and crc32 should both be zero";
    char* head_ptr = payload_ptr - kHeaderLen;
    uint32_t val_with_crc32_len;
    memcpy(&val_with_crc32_len, head_ptr, sizeof(kHeaderLen));
    payload_len = val_with_crc32_len - kChecksumLen;
    crc32 = folly::crc32c(reinterpret_cast<uint8_t*>(payload_ptr), payload_len);
  }
  // write checksum after the data field
  memcpy(payload_ptr + payload_len, &crc32, kChecksumLen);

  // flush data if necessary
  if (flush_policy_ == FlushPolicy::kInstantFlush) {
    PMemPersist(payload_ptr - kHeaderLen,
                kHeaderLen + payload_len + sizeof(crc32));
  }
}

}  // namespace mtcache
