#include "storage/pmem_dispatcher.h"

#include "common/numa_utils.h"

#include <gtest/gtest.h>

#include <filesystem>

DECLARE_int32(used_num_numa_nodes);
DECLARE_uint64(cache_pmem_gc_reserved);

namespace mtcache {

class LogAllocatorGCListenerMock : public LogBasedAllocatorGCEventListener {
 public:
  noodle::Result<void, CacheError> OnGCCopy(const char* old_ptr,
                                            const char* new_ptr) override {
    CHECK(old_ptr != nullptr);
    CHECK(new_ptr != nullptr);
    CHECK(old_ptr != new_ptr);
    return {};
  }

  // Dispatcher* dispatch_{nullptr};
};

class PMemDispatcherTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    registry_ = noodle::GetMetricRegistry("ti.mtcache.dispatchtest");

    NumaInfo::Init();
  }

  static void TearDownTestCase() {
    noodle::GetGlobalMetricRegistry()->Deregister("ti.mtcache.dispatchtest");
    CacheExecutor::DestroyAllExecutors();
  }

  void SetUp() override {
    n_numa_ = NumaInfo::GetMaxNumNumaNodes();
    FLAGS_used_num_numa_nodes = n_numa_;
    for (int i = 0; i < n_numa_; ++i) {
      // On CI env, we may have NUMA nodes that do not own any cores.
      // In this case, we only use the first `i` valid NUMA nodes for
      // the tests.
      if (NumaInfo::GetCpuCoresOfNumaNode(i).empty()) {
        FLAGS_used_num_numa_nodes = i;
        break;
      }
      char path[64] = "/tmp/mtcache_storage_dispacherX_test_XXXXXX";
      path[27] = '0' + i;
      if (mkdtemp(path) == nullptr) {
        LOG(FATAL) << "create tmp_dir failed, errno=" << errno;
      }
      pmem_paths_.push_back(std::string(path));
    }

    FLAGS_cache_pmem_gc_reserved = 6ULL * 1024 * 1024;
  }

  void TearDown() override {
    for (const auto& path : pmem_paths_) {
      std::filesystem::remove_all(path);
    }
  }

  static std::shared_ptr<noodle::MetricRegistry> registry_;

  int n_numa_;
  std::vector<std::string> pmem_paths_;
  uint64_t pmem_capacity_ = 100ULL * 1024 * 1024;
};

std::shared_ptr<noodle::MetricRegistry> PMemDispatcherTest::registry_;

TEST_F(PMemDispatcherTest, Basic) {
  auto listener = std::make_unique<LogAllocatorGCListenerMock>();
  const auto& common_executor = CacheExecutor::GetCommonExecutor();
  const auto& pmem_executors = CacheExecutor::GetPmemExecutors();
  auto dispatcher = std::make_unique<PMemDispatcher>(
      AllocatorType::kLogBasedAllocator, pmem_capacity_, pmem_paths_,
      common_executor, pmem_executors, listener.get(),
      PMemDispatcherTest::registry_);

  ASSERT_TRUE(dispatcher->Start());

  EXPECT_NE(dispatcher->GetAllocator(nullptr), nullptr);

  const std::string_view test_str = "12345678";
  char* w_ptr = nullptr;

  auto wf =
      [&test_str](CacheAllocator* alloc) -> noodle::Result<char*, CacheError> {
    EXPECT_NE(alloc, nullptr);
    auto ptr_res = alloc->Allocate(test_str.size());
    EXPECT_TRUE(ptr_res.IsOk());
    char* ptr = ptr_res.Get();
    EXPECT_NE(ptr, nullptr);
    memcpy(ptr, test_str.data(), test_str.size());
    auto seal_res = alloc->Seal(ptr, test_str.size(), 1U);
    EXPECT_TRUE(seal_res.IsOk());
    return ptr;
  };

  auto wcb = [&w_ptr](noodle::Result<char*, CacheError> wres)
      -> noodle::Result<CacheBufferSharedPtr, CacheError> {
        EXPECT_TRUE(wres.IsOk());
        EXPECT_NE(wres.Get(), nullptr);
        EXPECT_EQ(w_ptr, nullptr);
        w_ptr = wres.Get();
        return CacheBufferSharedPtr(nullptr);
      };

  auto fut1 =
      dispatcher->PushTask(AsyncWriteTask(std::move(wf), std::move(wcb)));
  auto pres = std::move(fut1).get();
  ASSERT_TRUE(pres.IsOk());
  EXPECT_EQ(std::move(pres).Get(), nullptr);

  auto del_f = [&w_ptr, &test_str](
      CacheAllocator* alloc) -> noodle::Result<char*, CacheError> {
    EXPECT_NE(w_ptr, nullptr);
    alloc->Free(w_ptr, test_str.size());
    return w_ptr;
  };

  auto dcb = [&w_ptr](noodle::Result<char*, CacheError> dres)
      -> noodle::Result<CacheBufferSharedPtr, CacheError> {
        EXPECT_TRUE(dres.IsOk());
        EXPECT_EQ(dres.Get(), w_ptr);
        return CacheBufferSharedPtr(nullptr);
      };

  auto fut2 = dispatcher->PushTask(
      AsyncWriteTask(std::move(del_f), std::move(dcb), w_ptr));
  auto dres = std::move(fut2).get();
  ASSERT_TRUE(dres.IsOk());

  ASSERT_TRUE(dispatcher->Stop());
}

}  // namespace mtcache
