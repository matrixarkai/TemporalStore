#pragma once

#include "allocator/log_allocator.h"
#include "allocator/pool_allocator.h"
#include "common/thread_pool/cpu_numa_thread_pool_executor.h"

namespace mtcache {

// Users' data is writtern to CPU's cache firstly. At that point, there is no
// guarantee when the data is flushed to PMem. We can either choose flush
// policy:
enum class FlushPolicy {
  // Do nothing, just let CPU decide when to flush its cache to PMem
  kNoFlush,
  // Flush data instantly in every `Seal` call
  kInstantFlush,
  // Flush data in mini-batch style. Only at least `mini_batch_flush_bytes`
  // data have been writtern, then the allocator will flush once
  // kMiniBatchFlush is only supported by the log-based allocator
  kMiniBatchFlush,
};

// This structure is similar to PmemRecoverStats except that this class does
// not have `total_bytes`.
struct ChunkRecoverStats {
  int64_t valid_bytes{0};
  int64_t freed_bytes{0};
  int64_t corrupted_bytes{0};
};

struct PmemRecoverStats {
  // Total bytes of valid pmem files. Note that the files that can not be
  // opened/mapped successfully or with incorrect size are not counted.
  std::atomic<int64_t> total_bytes_{0};
  // Total bytes of valid cache records, including kHeaderLen and CRC length.
  // Valid records are that with correct CRC.
  std::atomic<int64_t> valid_bytes_{0};
  // Total bytes of freed records, including the space after kChunkStopMark.
  std::atomic<int64_t> freed_bytes_{0};
  // Total bytes of corrupted records (i.e. CRC not match or header length
  // not valid).
  std::atomic<int64_t> corrupted_bytes_{0};

  // Add the numbers from `ChunkRecoverStats` into this PmemRecoverStats.
  void AddChunkStats(const ChunkRecoverStats& stats);

  PmemRecoverStats() = default;

  // This copy constructor is needed because some UTs need to get recover stats
  // via function `TEST_GetRecoverStats` and the atomic counters in
  // PmemRecoverStats can not be copied by default.
  PmemRecoverStats(const PmemRecoverStats& a);
};

class PmemAllocatorRecoverListener {
 public:
  virtual ~PmemAllocatorRecoverListener() = default;

  // Return true if the input record was met before (duplicated).
  // Return false if the input record is met for the first time.
  virtual bool OnScanRecord(const char* addr, size_t len) = 0;
};

class LogBasedMemoryAllocatorPMem : public LogBasedMemoryAllocatorBase {
 public:
  LogBasedMemoryAllocatorPMem(
      std::string data_path, FlushPolicy flush_policy,
      size_t mini_batch_flush_bytes,
      LogBasedAllocatorGCEventListener* gc_event_listener,
      size_t capacity_bytes, size_t gc_reserved_bytes, size_t max_thread_num,
      std::shared_ptr<noodle::MetricRegistry> registry, int numa_id);

  ~LogBasedMemoryAllocatorPMem() override = default;

  noodle::Result<char*, CacheError> Allocate(size_t len) override;

  noodle::Result<void, CacheError> Free(char* ptr, size_t len) override;

  // Iterate over all sealed memory objects with only best-effort guarantee and
  // invoke `listener->OnScanRecord()` on each of memory objects.
  // After shutdown or system crash, the upper layer may want to rebuild its
  // data structure(e.g. hashtable index) based on memory objects that had been
  // persistent by the allocator. `Recover` method iterates all memory objects
  // and `listener->OnScanRecord()` is called for every valid object.
  // Only memory objects of which checksum matches are recognized as valid ones.
  // Since there might be hardware error or partial write error, there is no
  // 100% guarantee all sealed memory objects will be identified as valid ones.
  //
  // NOTE: this function push many recover tasks into `executor` and do the
  // recovery asynchronously. The caller should join the threads in `executor`
  // to wait recovery tasks to complete.
  void Recover(PmemAllocatorRecoverListener* listener,
               CPUNumaThreadPoolExecutor* executor,
               PmemRecoverStats* recover_stats);

  // Get the total recoverable file size under the data directory of this
  // allocator.
  size_t GetRecoverableFileSize() const;

 protected:
  void OnSeal(GeneralChunk* chunk, char* payload_ptr, size_t payload_len,
              uint32_t crc32) override;

  void OnSealChunk(GeneralChunk* chunk) override;

 private:
  // Obtain ChunkIDs from the input file names with the format descripted in
  // function `BuildPmemFilePath` (/xxx/00000000000000000001.pmem_chunk).
  // If the ChunkIDs are discontinuous, the missing ChunkIDs will be inserted
  // into the `recycled_chunk_id_` list for furture usage. This function also
  // set the next_chunk_id_ to the maxinum of input ChunkID plus one.
  std::vector<ChunkID> RecoverChunkId(const std::vector<std::string>& fnames);

  // Get file names under `data_path_` with valid file size/name and delete the
  // invalid files.
  //
  // Valid files means:
  //  1. the file size is equal to kLogChunkSize
  //  2. the chunk id in chunk file name does not exceed max_chunk_id
  //     (max_chunk_id = capacity_and_gc_bytes_ / kLogChunkSize)
  std::vector<std::string> FilterValidChunkFiles();

  // Scan a chunk during recovery to:
  //   1. record recoverable records in `listener`
  //   2. free corrupted records
  //   3. restore the meta counters in ChunkMeta such as num_freed_bytes,
  //      num_allocated_bytes
  //
  // Return the recover stats of the scanned chunk.
  ChunkRecoverStats ScanChunk(GeneralChunk* chunk,
                              PmemAllocatorRecoverListener* listener);

 private:
  static constexpr uint32_t kChecksumLen = sizeof(uint32_t);  // We use CRC32C

  const FlushPolicy flush_policy_;
  const size_t mini_batch_flush_bytes_;
  std::string data_path_;

  // The valid filenames under `data_path_` at startup time.
  // The filenames are obtained when this PMEM log-based allocator scans all
  // files under `data_path_` and remove the invalid files when start. See
  // `FilterValidChunkFiles()` for details.
  //
  // The subsequent files created after start time will not be added to
  // recoverable_filenames_.
  std::vector<std::string> recoverable_filenames_;
};

class PoolBasedMemoryAllocatorPMem : public PoolBasedMemoryAllocatorBase {
 public:
  PoolBasedMemoryAllocatorPMem(std::string data_path, FlushPolicy flush_policy,
                               size_t capacity_bytes, size_t max_thread_num,
                               size_t obj_len);

  ~PoolBasedMemoryAllocatorPMem() override = default;

  noodle::Result<char*, CacheError> Allocate(size_t len) override;

  noodle::Result<void, CacheError> Free(char* ptr, size_t len) override;

 private:
  void OnSeal(char* payload_ptr, size_t payload_len, uint32_t crc32) override;

 private:
  static constexpr uint32_t kChecksumLen = 4;  // We use CRC32C

  const FlushPolicy flush_policy_;
};

}  // namespace mtcache
