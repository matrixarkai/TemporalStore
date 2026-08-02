#include "zonedstore_bench.h"

#include "common/logging.h"
#include "debug_utils.h"
#include "simple_lru_cache.h"

#include <gflags/gflags.h>

#include <cstdio>
#include <iostream>
#include <string>
#include <thread>

// We want to use `assertion` in our benchmarking tool anyway.
#undef NDEBUG
#include <cassert>

#define MIN_VSIZE 50
#define MAX_VSIZE 100

namespace mtcache {

void ZonedStoreBench::ReadOnlyBench(uint32_t workers) {}

void ZonedStoreBench::ReadWriteBench(uint32_t workers, uint32_t write_sz_gb,
                                     uint32_t read_ratio) {
  std::cout << "ReadWriteBench Started!" << std::endl;

  uint64_t total_bytes = (uint64_t)write_sz_gb << 30;

  std::atomic<uint64_t> written_bytes{0};
  std::atomic<uint64_t> inserted_key_cnt{0};

  std::vector<std::thread> threads;
  for (int i = 0; i < workers; ++i) {
    threads.emplace_back([&, i] {
      std::cout << "Worker " << i << " Started!" << std::endl;
      while (written_bytes < total_bytes) {
        int rand = fast_rand16() % 100;
        if (rand < read_ratio) {
          // Read
          std::string key =
              std::to_string(fast_rand64() % inserted_key_cnt.load());
          auto rst = zonedstore_->Get(key);
          if (!rst.IsOk()) {
            std::cout << "Random Read Failed: " << rst.GetError() << std::endl;
          }
          auto size = rst.Get()->Size();
          if (size == 0) {
            std::cout << "" << std::endl;
            exit(0);
          }
        } else {
          // Write
          std::string key = std::to_string(inserted_key_cnt++);
          int vsize = (fast_rand16() % (MAX_VSIZE - MIN_VSIZE + 1)) + MIN_VSIZE;
          vsize = vsize << 10;
          char* data;
          rand_string(&data, vsize, false);

          auto rst =
              zonedstore_->Put(key, *(folly::IOBuf::copyBuffer(data, vsize)));
          if (!rst.IsOk()) {
            std::cout << "Insert Failed!" << std::endl;
            exit(0);
          }

          written_bytes += vsize;
          if (inserted_key_cnt % 100 == 0) {
            std::cout << "Inserted Keys: " << inserted_key_cnt.load()
                      << ", Written Size(MB): " << (written_bytes >> 20)
                      << std::endl;
          }
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  std::cout << "ReadWriteBench Finished!" << std::endl;
}
}  // namespace mtcache

// All flags here.
DEFINE_uint32(workers, 1, "Total number of concurrent workers");
DEFINE_uint64(write_sz_gb, 1, "Total size of written data in GB");
DEFINE_uint32(read_ratio, 10, "The proportion of reads in all operations");
DEFINE_string(ssd_path, "/tmp", "Path of the ssd mounted directory.");

DEFINE_bool(readonly, false, "");

// Main
int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  mtcache::ZonedStoreBench bench(FLAGS_ssd_path);

  if (!FLAGS_readonly) {
    bench.ReadWriteBench(FLAGS_workers, FLAGS_write_sz_gb, FLAGS_read_ratio);
  } else {
    bench.ReadOnlyBench(FLAGS_workers);
  }

  return 0;
}
