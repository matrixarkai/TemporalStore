#pragma once

#include "storage/zoned_store/zoned_store.h"
#include "utils.h"

#include <absl/time/clock.h>

#include <cstddef>
#include <memory>
#include <numeric>

namespace mtcache {

class ZonedStoreBench {
 public:
  ZonedStoreBench(const std::string& db_path) {
    zonedstore_.reset(new StorageEngineZonedStore(
        db_path, 32UL << 30 /* storage capacity */, 0 /* zone mode */,
        10 /* user buf*/, 2 /* gc buf */, (64UL << 20) /* buf size*/,
        0.8 /* flush threshold*/, false /* using existing db */));
    zonedstore_->Start();
  }

  ~ZonedStoreBench() = default;

 public:
  void ReadWriteBench(uint32_t workers, uint32_t write_sz_gb,
                      uint32_t read_ratio);

  void ReadOnlyBench(uint32_t workers);

  // TODO(guokuankuan): Export bench result to target file.
  void export_result() {}

 private:
  // Trace the execute time of target func.
  template <typename Func>
  uint64_t timer(const Func& func) {
    auto t1 = absl::GetCurrentTimeNanos();
    func();
    auto t2 = absl::GetCurrentTimeNanos();
    return t2 - t1;
  }

 private:
  // Export filename.
  std::string export_file_ = "cache_bench_result.txt";

  std::shared_ptr<StorageEngineZonedStore> zonedstore_;
};
}  // namespace mtcache
