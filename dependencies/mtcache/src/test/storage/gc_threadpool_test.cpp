#include "allocator/dram_allocators.h"
#include "storage/gc_controller.h"
#include "test/log_allocator_gc_listener_mock.h"

#include <gflags/gflags.h>

DECLARE_int32(num_gc_workers);
DECLARE_int32(fragmentation_ratio_max);

namespace mtcache {

class MockLogBasedAllocGC {
 public:
  MockLogBasedAllocGC()
      : allocator_(&listener_, 1073741824, 128 * 1048576, 100,
                   noodle::GetMetricRegistry("ti.mtcache.gctest")) {
    // 1GB, 128 MB
    listener_.alloc_ = &allocator_;
  }

  ~MockLogBasedAllocGC() {}

  std::string Get(const std::string& key) {
    return listener_.GetInternalMap(key);
  }

  void Set(const std::string& key, const std::string& value) {
    auto alloc_res = allocator_.Allocate(value.size() + 1);
    ASSERT_TRUE(alloc_res.IsOk());
    char* new_ptr = alloc_res.Get();
    memcpy(new_ptr, value.c_str(), value.size());
    new_ptr[value.size()] = '\0';

    auto seal_res = allocator_.Seal(new_ptr);
    ASSERT_TRUE(seal_res.IsOk());

    const char* old_ptr = listener_.SetInternalMapAndReturnOldPtr(key, new_ptr);
    if (old_ptr != nullptr) {
      auto free_res = allocator_.Free(const_cast<char*>(old_ptr), 0);
      ASSERT_TRUE(free_res.IsOk());
    }
  }

  void Del(const std::string& key) {
    const char* ptr = listener_.DelInternalMapAndReturnOldPtr(key);
    if (ptr != nullptr) {
      auto free_res = allocator_.Free(const_cast<char*>(ptr), 0);
      ASSERT_TRUE(free_res.IsOk());
    }
  }

  size_t GetKVNum() { return listener_.key2ptr_map_.size(); }

  LogBasedAllocatorGCEventListenerMock listener_;
  LogBasedMemoryAllocatorDram allocator_;
};

class TestGC : public ::testing::Test {
 public:
  void SetUp() override {}
  void TearDown() override { CacheExecutor::DestroyAllExecutors(); }
  MockLogBasedAllocGC mock_instance_;
};

TEST_F(TestGC, SingleGCWorker) {
  FLAGS_num_gc_workers = 1;
  ASSERT_EQ(kLogChunkSize % 4, 0);
  uint32_t batches = kLogChunkSize / 4;
  std::string v(batches, '1');
  mock_instance_.Set("A", v);
  for (size_t i = 0; i < 10; ++i) {
    v[0] = 'a' + i;
    mock_instance_.Set("B", v);
    if (i == 4) {
      mock_instance_.Set("C", v);
    }
  }
  std::vector<ChunkID> id_vec;
  auto iter_meta_res = mock_instance_.allocator_.IterateRecyclableChunkMeta(
      [&](const ChunkMeta* meta) {
        EXPECT_EQ(meta->num_allocated_bytes, kLogChunkSize);
        EXPECT_EQ(meta->num_freed_bytes, kLogChunkSize - 4 - batches - 1);
        EXPECT_EQ(meta->ref_cnt, 1);
        if (meta->num_freed_bytes > kLogChunkSize * 0.2) {
          id_vec.emplace_back(meta->id);
        }
        return true;
      });
  ASSERT_TRUE(iter_meta_res.IsOk());
  ASSERT_EQ(id_vec.size(), 2);

  auto get_stats_res = mock_instance_.allocator_.GetStats();
  ASSERT_TRUE(get_stats_res.IsOk());
  auto stats = get_stats_res.Get();
  ASSERT_EQ(stats.num_allocated_bytes, 12 * (batches + 1));
  ASSERT_EQ(stats.num_freed_bytes, 9 * (batches + 1));
  ASSERT_EQ(stats.num_occupied_bytes, 3 * kLogChunkSize);

  auto registry = noodle::GetMetricRegistry("ti.mtcache.gctest");
  // starrt a gc instance
  StorageGCController gc_instance_(&mock_instance_.allocator_, false, registry);
  gc_instance_.Start();
  // ensure no more gc tasks
  while (gc_instance_.TEST_GetNumGcCompleteChunks() < 2) {
    // wait until 2 chunks are gc-ed.
  }
  gc_instance_.WaitAllTaskComplete();
  // There is one gc task that will be executed by one thread
  EXPECT_EQ(gc_instance_.TEST_GetNumGcCompleteTasks(), 1);
  EXPECT_EQ(gc_instance_.TEST_GetNumGcCompleteChunks(), 2);
  gc_instance_.Stop();

  get_stats_res = mock_instance_.allocator_.GetStats();
  ASSERT_TRUE(get_stats_res.IsOk());
  stats = get_stats_res.Get();
  ASSERT_EQ(stats.num_allocated_bytes, 12 * (batches + 1));
  ASSERT_EQ(stats.num_freed_bytes, 9 * (batches + 1));
  ASSERT_EQ(stats.num_occupied_bytes, 2 * kLogChunkSize);
  noodle::GetGlobalMetricRegistry()->Deregister("ti.mtcache.gctest");
}

TEST_F(TestGC, MultiGCWorkers) {
  FLAGS_num_gc_workers = 2;
  ASSERT_EQ(kLogChunkSize % 4, 0);
  uint32_t batches = kLogChunkSize / 4 * 3;
  // If half of the chunk is marked as garbage, gc it
  FLAGS_fragmentation_ratio_max = 50;

  std::string data(batches, 'a');
  mock_instance_.Set("A", data);
  for (size_t i = 0; i < 10; ++i) {
    data[0] = 'a' + i;
    mock_instance_.Set("B", data);
    // Fragment the chunk
    std::string key = std::to_string(i);
    mock_instance_.Set(key, "B");
    if (i == 4) {
      mock_instance_.Set("C", data);
    }
  }
  // Now, we used 12 chunks.

  auto get_stats_res = mock_instance_.allocator_.GetStats();
  ASSERT_TRUE(get_stats_res.IsOk());
  auto stats = get_stats_res.Get();
  ASSERT_EQ(stats.num_allocated_bytes,
            10 * (batches + 1 + 2) + 2 * (batches + 1));
  ASSERT_EQ(stats.num_freed_bytes, 9 * (batches + 1));
  ASSERT_EQ(stats.num_occupied_bytes, 12 * kLogChunkSize);  // 12 chunks

  std::vector<ChunkID> id_vec;
  auto iter_meta_res = mock_instance_.allocator_.IterateRecyclableChunkMeta(
      [&](const ChunkMeta* meta) {
        if (meta->num_freed_bytes >
            kLogChunkSize * FLAGS_fragmentation_ratio_max / 100) {
          id_vec.emplace_back(meta->id);
          return true;
        } else {
          return false;
        }
      });
  ASSERT_TRUE(iter_meta_res.IsOk());
  // 9 chunks need gc
  ASSERT_EQ(id_vec.size(), 9);

  auto registry = noodle::GetMetricRegistry("ti.mtcache.gctest");

  // starrt a gc instance
  StorageGCController gc_instance_(&mock_instance_.allocator_, false, registry);
  gc_instance_.Start();

  // ensure no more gc tasks
  while (gc_instance_.TEST_GetNumGcCompleteChunks() < 9) {
    // wait until 9 chunks are gc-ed.
  }
  gc_instance_.WaitAllTaskComplete();
  // There is 2 gc task that will be executed by 2 thread
  EXPECT_EQ(gc_instance_.TEST_GetNumGcCompleteTasks(), 2);
  EXPECT_EQ(gc_instance_.TEST_GetNumGcCompleteChunks(), 9);

  gc_instance_.Stop();

  auto get_stats_res2 = mock_instance_.allocator_.GetStats();
  ASSERT_TRUE(get_stats_res2.IsOk());
  auto stats2 = get_stats_res2.Get();
  ASSERT_EQ(stats2.num_allocated_bytes,
            10 * (batches + 1 + 2) + 2 * (batches + 1));
  ASSERT_EQ(stats2.num_freed_bytes, 9 * (batches + 1));
  // GC jobs have been submitted, but their memory space are not guaranteed to
  // be released. So we need to consider GCLeftChunksSize().
  ASSERT_EQ(stats2.num_occupied_bytes,
            (3 + mock_instance_.allocator_.TEST_GetGCLeftChunksSize()) *
                kLogChunkSize);
  noodle::GetGlobalMetricRegistry()->Deregister("ti.mtcache.gctest");
}

}  // namespace mtcache

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
