#include "allocator/dram_allocators.h"
#include "allocator/pmem_allocators.h"
#include "test/log_allocator_gc_listener_mock.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <map>
#include <mutex>
#include <random>
#include <thread>

namespace mtcache {

static constexpr size_t kCapacity = (8 * kLogChunkSize);
static constexpr size_t kGCReserve = (2 * kLogChunkSize);
static constexpr uint32_t kMaxThreadNum = 100;
static const std::string kTestDir = "/tmp/mtcache_allocator_test";

class LogBasedMemoryAllocatorPMemInstantFlushPolicy
    : public LogBasedMemoryAllocatorPMem {
 public:
  using LogBasedMemoryAllocatorPMem::LogBasedMemoryAllocatorPMem;
};

class LogBasedMemoryAllocatorPMemMiniBatchFlushPolicy
    : public LogBasedMemoryAllocatorPMem {
 public:
  using LogBasedMemoryAllocatorPMem::LogBasedMemoryAllocatorPMem;
};

static std::unique_ptr<LogBasedMemoryAllocatorDram> CreateAllocator(
    LogBasedMemoryAllocatorDram*, LogBasedAllocatorGCEventListener* listener,
    std::shared_ptr<noodle::MetricRegistry> registry) {
  CHECK(registry != nullptr);
  return std::make_unique<LogBasedMemoryAllocatorDram>(
      listener, kCapacity, kGCReserve, kMaxThreadNum, registry);
}

static std::unique_ptr<LogBasedMemoryAllocatorPMem> CreateAllocator(
    LogBasedMemoryAllocatorPMem*, LogBasedAllocatorGCEventListener* listener,
    std::shared_ptr<noodle::MetricRegistry> registry) {
  CHECK(registry != nullptr);
  return std::make_unique<LogBasedMemoryAllocatorPMem>(
      kTestDir, FlushPolicy::kNoFlush, 0, listener, kCapacity, kGCReserve,
      kMaxThreadNum, registry, -1);
}

static std::unique_ptr<LogBasedMemoryAllocatorPMemInstantFlushPolicy>
CreateAllocator(LogBasedMemoryAllocatorPMemInstantFlushPolicy*,
                LogBasedAllocatorGCEventListener* listener,
                std::shared_ptr<noodle::MetricRegistry> registry) {
  CHECK(registry != nullptr);
  return std::make_unique<LogBasedMemoryAllocatorPMemInstantFlushPolicy>(
      kTestDir, FlushPolicy::kInstantFlush, 0, listener, kCapacity, kGCReserve,
      kMaxThreadNum, registry, -1);
}

static std::unique_ptr<LogBasedMemoryAllocatorPMemMiniBatchFlushPolicy>
CreateAllocator(LogBasedMemoryAllocatorPMemMiniBatchFlushPolicy*,
                LogBasedAllocatorGCEventListener* listener,
                std::shared_ptr<noodle::MetricRegistry> registry) {
  CHECK(registry != nullptr);
  return std::make_unique<LogBasedMemoryAllocatorPMemMiniBatchFlushPolicy>(
      kTestDir, FlushPolicy::kMiniBatchFlush, 4096, listener, kCapacity,
      kGCReserve, kMaxThreadNum, registry, -1);
}

template <typename AllocatorType>
class LogBasedMemoryAllocatorTest : public testing::Test {
 public:
  LogBasedMemoryAllocatorTest() {
    registry_ = noodle::GetMetricRegistry("ti.mtcache.allocator_test");
    allocator_ = CreateAllocator((AllocatorType*){}, &listener_, registry_);
    listener_.alloc_ = allocator_.get();
  }

  ~LogBasedMemoryAllocatorTest() override {
    noodle::GetGlobalMetricRegistry()->Deregister("ti.mtcache.allocator_test");
    std::filesystem::remove_all(kTestDir);
  }

  std::string Get(const std::string& key) {
    return listener_.GetInternalMap(key);
  }

  void Set(const std::string& key, const std::string& value) {
    auto alloc_res = allocator_->Allocate(value.size() + 1);
    ASSERT_TRUE(alloc_res.IsOk()) << alloc_res.GetError()->GetMessage();
    char* new_ptr = alloc_res.Get();
    memcpy(new_ptr, value.c_str(), value.size());
    new_ptr[value.size()] = '\0';

    auto seal_res = allocator_->Seal(new_ptr);
    ASSERT_TRUE(seal_res.IsOk());

    const char* old_ptr = listener_.SetInternalMapAndReturnOldPtr(key, new_ptr);
    if (old_ptr != nullptr) {
      auto free_res = allocator_->Free(const_cast<char*>(old_ptr), 0);
      ASSERT_TRUE(free_res.IsOk());
    }
  }

  void Del(const std::string& key) {
    const char* ptr = listener_.DelInternalMapAndReturnOldPtr(key);
    if (ptr != nullptr) {
      auto free_res = allocator_->Free(const_cast<char*>(ptr), 0);
      ASSERT_TRUE(free_res.IsOk());
    }
  }

  size_t GetKVNum() { return listener_.key2ptr_map_.size(); }

  size_t GetGCLeftChunksSize() {
    return allocator_->TEST_GetGCLeftChunksSize();
  }

  size_t GetNumInitedTLSCtx() { return allocator_->TEST_GetNumInitedTLSCtx(); }

  static bool IsPMemImpl() {
    return std::is_same_v<AllocatorType, LogBasedMemoryAllocatorPMem> ||
           std::is_base_of_v<LogBasedMemoryAllocatorPMem, AllocatorType>;
  }

  LogBasedAllocatorGCEventListenerMock listener_;
  std::unique_ptr<AllocatorType> allocator_;
  std::shared_ptr<noodle::MetricRegistry> registry_;
};

typedef testing::Types<LogBasedMemoryAllocatorDram,
                       LogBasedMemoryAllocatorPMem /* FlushPolicy::kNoFlush */,
                       LogBasedMemoryAllocatorPMemInstantFlushPolicy,
                       LogBasedMemoryAllocatorPMemMiniBatchFlushPolicy>
    AllocatorImplTypes;
TYPED_TEST_SUITE(LogBasedMemoryAllocatorTest, AllocatorImplTypes);

TYPED_TEST(LogBasedMemoryAllocatorTest, AllocateAndFree) {
  auto alloc_res = this->allocator_->Allocate(3);
  ASSERT_TRUE(alloc_res.IsOk());
  char* ptr = alloc_res.Get();
  ptr[0] = 'A';
  ptr[1] = 'B';
  ptr[2] = '\0';
  auto free_res = this->allocator_->Free(ptr, 3);
  ASSERT_TRUE(free_res.IsOk());
}

TYPED_TEST(LogBasedMemoryAllocatorTest, AllocateAndSealAndFreeInterleaving) {
  std::vector<char*> ptr_vec;
  std::vector<const ChunkMeta*> meta_vec;
  std::function<bool(const ChunkMeta* meta)> iter_func =
      [&](const ChunkMeta* meta) {
        meta_vec.emplace_back(meta);
        return true;
      };

  int32_t mbs = kLogChunkSize / (1 << 20);
  for (int32_t i = 0; i < mbs - 1; ++i) {
    auto alloc_res = this->allocator_->Allocate(1 << 20);
    ASSERT_TRUE(alloc_res.IsOk());
    char* ptr = alloc_res.Get();
    ptr_vec.emplace_back(ptr);
  }
  {  // chunk is not full
    auto iter_meta_res =
        this->allocator_->IterateRecyclableChunkMeta(iter_func);
    ASSERT_TRUE(iter_meta_res.IsOk());
    ASSERT_TRUE(meta_vec.empty());
  }
  {
    auto alloc_res = this->allocator_->Allocate(1 << 20);
    ASSERT_TRUE(alloc_res.IsOk());
    char* ptr = alloc_res.Get();
    ptr_vec.emplace_back(ptr);
  }
  {  // chunk is full but not sealed
    auto iter_meta_res =
        this->allocator_->IterateRecyclableChunkMeta(iter_func);
    ASSERT_TRUE(iter_meta_res.IsOk());
    ASSERT_TRUE(meta_vec.empty());
  }
  for (char* ptr : ptr_vec) {
    auto seal_res = this->allocator_->Seal(ptr);
    ASSERT_TRUE(seal_res.IsOk());
  }
  {  // chunk is full and sealed
    auto iter_meta_res =
        this->allocator_->IterateRecyclableChunkMeta(iter_func);
    ASSERT_TRUE(iter_meta_res.IsOk());
    ASSERT_EQ(meta_vec.size(), 1);

    auto* meta = meta_vec.front();
    ASSERT_EQ(meta->id, 0);
    ASSERT_EQ(meta->num_allocated_bytes, kLogChunkSize);
    ASSERT_EQ(meta->num_freed_bytes,
              kLogChunkSize - ((4 + (1 << 20)) * (mbs - 1)) -
                  (this->IsPMemImpl() ? 4 * (mbs - 1) : 0));
    ASSERT_EQ(meta->ref_cnt, (mbs - 1));

    auto get_stats_res = this->allocator_->GetStats();
    ASSERT_TRUE(get_stats_res.IsOk());
    auto stats = get_stats_res.Get();
    ASSERT_EQ(stats.num_allocated_bytes,
              kLogChunkSize + (this->IsPMemImpl() ? 4 * mbs : 0));
    ASSERT_EQ(stats.num_freed_bytes, 0);
    ASSERT_EQ(stats.num_occupied_bytes, 2 * kLogChunkSize);
  }
  for (size_t i = 0; i < (mbs - 1); ++i) {
    auto free_res = this->allocator_->Free(ptr_vec[i], 0);
    ASSERT_TRUE(free_res.IsOk());
  }
  {  // chunk is freed
    meta_vec.clear();
    auto iter_meta_res =
        this->allocator_->IterateRecyclableChunkMeta(iter_func);
    ASSERT_TRUE(iter_meta_res.IsOk());
    ASSERT_TRUE(meta_vec.empty());

    auto get_stats_res = this->allocator_->GetStats();
    ASSERT_TRUE(get_stats_res.IsOk());
    auto stats = get_stats_res.Get();
    ASSERT_EQ(stats.num_allocated_bytes,
              kLogChunkSize + (this->IsPMemImpl() ? 4 * mbs : 0));
    ASSERT_EQ(stats.num_freed_bytes,
              (mbs - 1) * (1 << 20) + (this->IsPMemImpl() ? 4 * (mbs - 1) : 0));
    ASSERT_EQ(stats.num_occupied_bytes, kLogChunkSize);
  }
  {
    auto retrieve_res =
        this->allocator_->RetrieveChunkMeta(1, [&](const ChunkMeta* meta) {
          ASSERT_EQ(meta->id, 1);
          ASSERT_EQ(meta->num_allocated_bytes,
                    4 + (1 << 20) + (this->IsPMemImpl() ? 4 : 0));
          ASSERT_EQ(meta->num_freed_bytes, 0);
          ASSERT_EQ(meta->ref_cnt, 2);
        });
    ASSERT_TRUE(retrieve_res.IsOk());
  }
  {
    auto free_res = this->allocator_->Free(ptr_vec[mbs - 1], 0);
    ASSERT_TRUE(free_res.IsOk());
  }
  {  // chunk is owned by a writer
    auto retrieve_res =
        this->allocator_->RetrieveChunkMeta(1, [&](const ChunkMeta* meta) {
          ASSERT_EQ(meta->id, 1);
          ASSERT_EQ(meta->num_allocated_bytes,
                    4 + (1 << 20) + (this->IsPMemImpl() ? 4 : 0));
          ASSERT_EQ(meta->num_freed_bytes,
                    4 + (1 << 20) + (this->IsPMemImpl() ? 4 : 0));
          ASSERT_EQ(meta->ref_cnt, 1);
        });
    ASSERT_TRUE(retrieve_res.IsOk());
  }
}

TYPED_TEST(LogBasedMemoryAllocatorTest, Capacity) {
  auto cap_res = this->allocator_->Capacity();
  ASSERT_TRUE(cap_res.IsOk());
  ASSERT_EQ(cap_res.Get(), kCapacity);
}

// TYPED_TEST(LogBasedMemoryAllocatorTest, Recover) {
//  auto recover_res = this->allocator_->Recover(nullptr);
//  ASSERT_EQ(recover_res.GetError(), &Errors::kNotImplemented);
//}

TYPED_TEST(LogBasedMemoryAllocatorTest, IterateMultipleChunkMeta) {
  uint32_t mbs = kLogChunkSize / (1 << 20);
  uint32_t mba = mbs - 1;
  {  // base
    auto alloc_res = this->allocator_->Allocate(mba * (1 << 20));
    ASSERT_TRUE(alloc_res.IsOk());
    auto seal_res = this->allocator_->Seal(alloc_res.Get());
    ASSERT_TRUE(seal_res.IsOk());
    auto free_res = this->allocator_->Free(alloc_res.Get(), 0);
    ASSERT_TRUE(free_res.IsOk());
    alloc_res = this->allocator_->Allocate(7);
    ASSERT_TRUE(alloc_res.IsOk());
    seal_res = this->allocator_->Seal(alloc_res.Get());
    ASSERT_TRUE(seal_res.IsOk());
  }
  {  // more num_freed_bytes
    auto alloc_res = this->allocator_->Allocate(mba * (1 << 20));
    ASSERT_TRUE(alloc_res.IsOk());
    auto seal_res = this->allocator_->Seal(alloc_res.Get());
    ASSERT_TRUE(seal_res.IsOk());
    auto free_res = this->allocator_->Free(alloc_res.Get(), 0);
    ASSERT_TRUE(free_res.IsOk());
    alloc_res = this->allocator_->Allocate(6);
    ASSERT_TRUE(alloc_res.IsOk());
    seal_res = this->allocator_->Seal(alloc_res.Get());
    ASSERT_TRUE(seal_res.IsOk());
  }
  {  // more num_freed_bytes + more ref_cnt
    auto alloc_res = this->allocator_->Allocate(mba * (1 << 20));
    ASSERT_TRUE(alloc_res.IsOk());
    auto seal_res = this->allocator_->Seal(alloc_res.Get());
    ASSERT_TRUE(seal_res.IsOk());
    auto free_res = this->allocator_->Free(alloc_res.Get(), 0);
    ASSERT_TRUE(free_res.IsOk());
    for (size_t i = 0; i < 2; ++i) {
      alloc_res = this->allocator_->Allocate(1);
      ASSERT_TRUE(alloc_res.IsOk());
      seal_res = this->allocator_->Seal(alloc_res.Get());
      ASSERT_TRUE(seal_res.IsOk());
    }
  }
  {
    auto alloc_res = this->allocator_->Allocate(mba * (1 << 20) + 1);
    ASSERT_TRUE(alloc_res.IsOk());

    size_t nth = 0;
    auto iter_meta_res = this->allocator_->IterateRecyclableChunkMeta(
        [&](const ChunkMeta* meta) {
          EXPECT_LT(nth, 3);

          if (this->IsPMemImpl()) {
            switch (nth) {
              case 0:
                EXPECT_EQ(meta->id, 1);
                EXPECT_EQ(meta->num_allocated_bytes, mbs * (1 << 20));
                EXPECT_EQ(meta->num_freed_bytes, mbs * (1 << 20) - 10 - 4);
                EXPECT_EQ(meta->ref_cnt, 1);
                break;

              case 1:
                EXPECT_EQ(meta->id, 0);
                EXPECT_EQ(meta->num_allocated_bytes, mbs * (1 << 20));
                EXPECT_EQ(meta->num_freed_bytes, mbs * (1 << 20) - 11 - 4);
                EXPECT_EQ(meta->ref_cnt, 1);
                break;

              case 2:
                EXPECT_EQ(meta->id, 2);
                EXPECT_EQ(meta->num_allocated_bytes, mbs * (1 << 20));
                EXPECT_EQ(meta->num_freed_bytes, mbs * (1 << 20) - 10 - 4 * 2);
                EXPECT_EQ(meta->ref_cnt, 2);
                break;
            }
          } else {
            switch (nth) {
              case 0:
                EXPECT_EQ(meta->id, 1);
                EXPECT_EQ(meta->num_allocated_bytes, mbs * (1 << 20));
                EXPECT_EQ(meta->num_freed_bytes, mbs * (1 << 20) - 10);
                EXPECT_EQ(meta->ref_cnt, 1);
                break;

              case 1:
                EXPECT_EQ(meta->id, 2);
                EXPECT_EQ(meta->num_allocated_bytes, mbs * (1 << 20));
                EXPECT_EQ(meta->num_freed_bytes, mbs * (1 << 20) - 10);
                EXPECT_EQ(meta->ref_cnt, 2);
                break;

              case 2:
                EXPECT_EQ(meta->id, 0);
                EXPECT_EQ(meta->num_allocated_bytes, mbs * (1 << 20));
                EXPECT_EQ(meta->num_freed_bytes, mbs * (1 << 20) - 11);
                EXPECT_EQ(meta->ref_cnt, 1);
                break;
            }
          }

          ++nth;
          return true;
        });
    ASSERT_TRUE(iter_meta_res.IsOk());
  }
}

TYPED_TEST(LogBasedMemoryAllocatorTest, OutOfSpaceError) {
  uint32_t half_chunk_sz = kLogChunkSize / 2;
  uint32_t num_chunk = kCapacity / kLogChunkSize;
  for (size_t i = 0; i < num_chunk + 1; ++i) {
    auto alloc_res = this->allocator_->Allocate(half_chunk_sz);
    if (!alloc_res.IsOk()) {
      auto error_counter =
          noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
              noodle::MetricId(
                  "ti.mtcache.allocator_test.failed_allocator_counter",
                  {{"allocator_error_id", "out_of_space"}}));
      EXPECT_EQ(error_counter->GetValue(), 1);
      ASSERT_EQ(alloc_res.GetError(), &Errors::kAllocatorOutOfSpace);
      return;
    }
  }
  // should not reach here
  ASSERT_TRUE(false);
}

TYPED_TEST(LogBasedMemoryAllocatorTest, DoubleFreeError) {
  auto alloc_res = this->allocator_->Allocate(3);
  char* ptr = alloc_res.Get();
  ptr[0] = 'A';
  ptr[1] = 'B';
  ptr[2] = '\0';
  auto free_res = this->allocator_->Free(ptr, 3);
  ASSERT_TRUE(free_res.IsOk());
  free_res = this->allocator_->Free(ptr, 3);
  if (!free_res.IsOk()) {
    auto error_counter =
        noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
            noodle::MetricId(
                "ti.mtcache.allocator_test.failed_allocator_counter",
                {{"allocator_error_id", "double_free"}}));
    EXPECT_EQ(error_counter->GetValue(), 1);
    ASSERT_EQ(free_res.GetError(), &Errors::kAllocatorDoubleFree);
    return;
  }
  // should not reach here
  ASSERT_TRUE(false);
}

TYPED_TEST(LogBasedMemoryAllocatorTest, ChunkReuse) {
  uint32_t mbs = kLogChunkSize / (1 << 20);
  uint32_t mba = mbs - 1;
  for (size_t i = 0; i < 2; ++i) {
    auto alloc_res = this->allocator_->Allocate(mba * (1 << 20));
    ASSERT_TRUE(alloc_res.IsOk());
    auto seal_res = this->allocator_->Seal(alloc_res.Get());
    ASSERT_TRUE(seal_res.IsOk());
    auto free_res = this->allocator_->Free(alloc_res.Get(), 0);
    ASSERT_TRUE(free_res.IsOk());
  }
  {
    auto get_stats_res = this->allocator_->GetStats();
    ASSERT_TRUE(get_stats_res.IsOk());
    auto stats = get_stats_res.Get();
    ASSERT_EQ(stats.num_allocated_bytes,
              2 * mba * (1 << 20) + (this->IsPMemImpl() ? 4 * 2 : 0));
    ASSERT_EQ(stats.num_freed_bytes,
              2 * mba * (1 << 20) + (this->IsPMemImpl() ? 4 * 2 : 0));
    ASSERT_EQ(stats.num_occupied_bytes, mbs * (1 << 20));
  }
}

TYPED_TEST(LogBasedMemoryAllocatorTest, GC) {
  ASSERT_EQ(kLogChunkSize % 4, 0);
  uint32_t batches = kLogChunkSize / 4;
  std::string v(batches, '1');
  this->Set("A", v);
  for (size_t i = 0; i < 10; ++i) {
    v[0] = 'a' + i;
    this->Set("B", v);
    if (i == 4) {
      this->Set("C", v);
    }
  }

  std::vector<ChunkID> id_vec;
  auto iter_meta_res =
      this->allocator_->IterateRecyclableChunkMeta([&](const ChunkMeta* meta) {
        EXPECT_EQ(meta->num_allocated_bytes, kLogChunkSize);
        EXPECT_EQ(meta->num_freed_bytes, kLogChunkSize - 4 - batches - 1 -
                                             (this->IsPMemImpl() ? 4 : 0));
        EXPECT_EQ(meta->ref_cnt, 1);
        if (meta->num_freed_bytes > kLogChunkSize * 0.2) {
          id_vec.emplace_back(meta->id);
        }
        return true;
      });
  ASSERT_TRUE(iter_meta_res.IsOk());
  ASSERT_EQ(id_vec.size(), 2);

  {
    auto get_stats_res = this->allocator_->GetStats();
    ASSERT_TRUE(get_stats_res.IsOk());
    auto stats = get_stats_res.Get();
    ASSERT_EQ(stats.num_allocated_bytes,
              12 * (batches + 1) + (this->IsPMemImpl() ? 4 * 12 : 0));
    ASSERT_EQ(stats.num_freed_bytes,
              9 * (batches + 1) + (this->IsPMemImpl() ? 4 * 9 : 0));
    ASSERT_EQ(stats.num_occupied_bytes, 3 * kLogChunkSize);
  }
  auto gc_res = this->allocator_->GC(id_vec.data(), id_vec.size());
  ASSERT_TRUE(gc_res.IsOk());
  {
    auto get_stats_res = this->allocator_->GetStats();
    ASSERT_TRUE(get_stats_res.IsOk());
    auto stats = get_stats_res.Get();
    ASSERT_EQ(stats.num_allocated_bytes,
              12 * (batches + 1) + (this->IsPMemImpl() ? 4 * 12 : 0));
    ASSERT_EQ(stats.num_freed_bytes,
              9 * (batches + 1) + (this->IsPMemImpl() ? 4 * 9 : 0));
    ASSERT_EQ(stats.num_occupied_bytes, 2 * kLogChunkSize);
  }

  {
    size_t nth = 0;
    auto iter_meta_res = this->allocator_->IterateRecyclableChunkMeta(
        [&](const ChunkMeta* meta) {
          EXPECT_LT(nth, 1);

          EXPECT_EQ(meta->id, 3);
          EXPECT_EQ(meta->num_allocated_bytes, kLogChunkSize);
          EXPECT_EQ(meta->num_freed_bytes,
                    kLogChunkSize - 2 * (4 + batches + 1) -
                        (this->IsPMemImpl() ? 4 * 2 : 0));
          EXPECT_EQ(meta->ref_cnt, 2);

          ++nth;
          return true;
        });
    ASSERT_TRUE(iter_meta_res.IsOk());
  }

  v[0] = '1';
  ASSERT_EQ(this->Get("A"), v);
  v[0] = 'a' + 9;
  ASSERT_EQ(this->Get("B"), v);
  v[0] = 'a' + 4;
  ASSERT_EQ(this->Get("C"), v);
}

TYPED_TEST(LogBasedMemoryAllocatorTest, ChaosMultithreading) {
  std::atomic<bool> done{false};

  std::vector<std::thread> gc_jobs;
  for (size_t i = 0; i < 2; ++i) {
    gc_jobs.emplace_back([&, i]() {
      uint32_t gc_num = 0;
      while (!done) {
        std::vector<ChunkID> id_vec;
        auto iter_meta_res = this->allocator_->IterateRecyclableChunkMeta(
            [&](const ChunkMeta* meta) {
              if (meta->num_freed_bytes > kLogChunkSize * 0.2) {
                id_vec.emplace_back(meta->id);
              }
              return true;
            });
        ASSERT_TRUE(iter_meta_res.IsOk());

        auto gc_res = this->allocator_->GC(id_vec.data(), id_vec.size());
        ASSERT_TRUE(gc_res.IsOk());
        gc_num += id_vec.size();
      }
      LOG(INFO) << "GC chunk num for gc_job_" << i << " is " << gc_num;
    });
  }

  std::atomic<size_t> kv_num{0};
  std::atomic<size_t> alloc_mem_bytes{0};
  std::vector<std::thread> mod_jobs;
  constexpr uint32_t kValueSize = 50 * 1024;
  for (size_t i = 0; i < 3; ++i) {
    mod_jobs.emplace_back([&, i]() {
      // NOTING(lyj): increase kTestTimes to higher value(e.g. 1M) if you are
      // doing a more serious test.
      // kTestTimes * kValueSize * mod_jobs_num must be less than kCapacity
      constexpr auto kTestTimes = 1024;
      const auto prefix = std::to_string(i) + '_';
      std::string insert_v(kValueSize, 'a');

      std::random_device rd;
      std::default_random_engine engine(rd());
      std::uniform_int_distribution<uint64_t> dist;
      std::map<std::string, std::string> std_map;

      // Random Set
      for (size_t i = 0; i < kTestTimes; ++i) {
        auto k = prefix + std::to_string(dist(engine));
        if (dist(engine) & 1) {         // add
        } else if (!std_map.empty()) {  // update
          auto it = std_map.lower_bound(k);
          if (it == std_map.end()) {
            --it;
          }
          k = it->first;
        }
        insert_v[0] = '0' + (i % 10);
        this->Set(k, insert_v);
        // no need to use 'insert_v' as it is too large
        std_map[k] = std::to_string(i % 10);
      }
      std::cout << "Random Set done." << std::endl;
      LOG(INFO) << "k-v number after Set for mod_job_" << i
                << " is: " << std_map.size();

      // Random Del
      for (size_t i = 0; i < kTestTimes; ++i) {
        auto k = prefix + std::to_string(dist(engine));
        if (dist(engine) % 3 == 0 && !std_map.empty()) {  // del
          auto it = std_map.lower_bound(k);
          if (it == std_map.end()) {
            --it;
          }
          k = it->first;
        }
        // else {}  // blind del
        this->Del(k);
        std_map.erase(k);
      }
      std::cout << "Random Del done." << std::endl;
      LOG(INFO) << "k-v number after Del for mod_job_" << i
                << " is: " << std_map.size();

      // Random Get
      for (size_t i = 0; i < kTestTimes; ++i) {
        auto k = prefix + std::to_string(dist(engine));
        if ((dist(engine) & 1) && !std_map.empty()) {  // found
          auto it = std_map.lower_bound(k);
          if (it == std_map.end()) {
            --it;
          }
          k = it->first;
          std::string v = this->Get(k);
          ASSERT_EQ(v[0], (std_map[k])[0]);
        } else {  // not found
          std::string v = this->Get(k);
          ASSERT_TRUE(v.empty());
        }
      }
      std::cout << "Random Get done." << std::endl;

      kv_num += std_map.size();
      alloc_mem_bytes += (std_map.size() * (insert_v.size() + 1));
    });
  }

  for (auto& job : mod_jobs) {
    job.join();
  }
  done = true;
  for (auto& job : gc_jobs) {
    job.join();
  }

  {
    std::vector<ChunkID> id_vec;
    auto iter_meta_res = this->allocator_->IterateRecyclableChunkMeta(
        [&](const ChunkMeta* meta) {
          LOG(INFO) << "meta_free: " << meta->num_freed_bytes
                    << ", meta_ptr: " << meta;
          if (meta->num_freed_bytes > kLogChunkSize * 0.2) {
            id_vec.emplace_back(meta->id);
          }
          return true;
        });
    ASSERT_TRUE(iter_meta_res.IsOk());

    auto gc_res = this->allocator_->GC(id_vec.data(), id_vec.size());
    ASSERT_TRUE(gc_res.IsOk());
    LOG(INFO) << "GC chunk num after gc_job stop is " << id_vec.size();
  }
  {
    ASSERT_EQ(kv_num, this->GetKVNum());
    auto get_stats_res = this->allocator_->GetStats();
    ASSERT_TRUE(get_stats_res.IsOk());
    auto stats = get_stats_res.Get();
    LOG(INFO) << "alloc_bytes: " << stats.num_allocated_bytes
              << ", freed_bytes: " << stats.num_freed_bytes;
    ASSERT_EQ(stats.num_allocated_bytes - stats.num_freed_bytes,
              alloc_mem_bytes + (this->IsPMemImpl() ? 4 * kv_num : 0));
    size_t ref_cnt = 0;
    for (size_t i = 0; i < (kCapacity + kGCReserve) / kLogChunkSize; ++i) {
      this->allocator_->RetrieveChunkMeta(
          i, [&](const ChunkMeta* meta) { ref_cnt += meta->ref_cnt; });
    }
    LOG(INFO) << "KVNum: " << this->GetKVNum();
    LOG(INFO) << "GCLeftChunksSize: " << this->GetGCLeftChunksSize();
    LOG(INFO) << "NumInitedTLSCtx: " << this->GetNumInitedTLSCtx();
    ASSERT_LE(ref_cnt, this->GetKVNum() + this->GetGCLeftChunksSize() +
                           this->GetNumInitedTLSCtx());
  }
}

}  // namespace mtcache
