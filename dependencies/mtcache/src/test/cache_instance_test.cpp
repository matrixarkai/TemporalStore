#include "cache_instance.h"

#include "buffer/raw_buffer.h"
#include "cache_executor.h"
#include "common/logging.h"
#include "common/numa_utils.h"
#include "tools/utils.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <vector>
#include <cstring>

// Allocator type of DRAM and PMEM storage: Log-based or Pool-based
// DRAM storage
DECLARE_string(dram_allocator_type);
// PMEM storage
DECLARE_string(pmem_allocator_type);
// Single object memory size in pool-based allocator
DECLARE_uint64(pool_based_allocator_obj_len);
// The number of NUMA nodes used by the system
DECLARE_int32(used_num_numa_nodes);

DECLARE_bool(cache_enable_ssd_data_recovery);

namespace mtcache {

// [lower,upper]
std::string gen_random(const int lower, const int upper) {
  const int len = (::rand() % (upper - lower + 1)) + lower;
  const char optional_char[] = "0123456789-./&*(abcdefgABCD)";
  std::string s(len, 0);
  s[0] = 'S';
  for (int i = 1; i < len; i++) {
    s[i] = optional_char[::rand() % (sizeof(optional_char) - 1)];
    // s[i] = optional_char[i % (sizeof(optional_char))];
  }
  return s;
}

class CacheInstanceTest : public ::testing::Test {
 protected:
  static void TearDownTestCase() { CacheExecutor::DestroyAllExecutors(); }
  void SetUp() override {
    // Disable NUMA-awareness in UTs.
    FLAGS_used_num_numa_nodes = 1;
    registry_ = noodle::GetMetricRegistry("ti.mtcache.cacheinstance_test");
    CHECK(registry_ != nullptr);
    NumaInfo::Init();
  }

  void TearDown() override {
    std::filesystem::remove_all(pmem_path_);
    std::filesystem::remove_all(pmem_path1_);
    std::filesystem::remove_all(ssd_path_);
    noodle::GetGlobalMetricRegistry()->Deregister(
        "ti.mtcache.cacheinstance_test");
  }

  std::string pmem_path_{"/tmp/mtcache_cache_instance_test_pmem"};
  std::string pmem_path1_{"/tmp/mtcache_cache_instance_test_pmem1"};
  std::string ssd_path_{"/tmp/mtcache_cache_instance_test_ssd"};

  std::shared_ptr<noodle::MetricRegistry> registry_;
};

TEST_F(CacheInstanceTest, ReadAllHitTest) {
  CacheInstance instance(100, ReplacementPolicyType::kFIFO,
                         StorageEngineType::kSimpleStorageEngine, {});
  instance.SetMetricRegistry(registry_);
  auto init_res = instance.Start();
  ASSERT_TRUE(init_res.IsOk());

  uint32_t cnt = 10;
  for (int i = 1; i < cnt; ++i) {
    std::string key = std::to_string(i);
    std::string data = key;
    auto value = folly::IOBuf::copyBuffer(data);
    instance.Put(key, std::move(*value));
  }

  for (int i = 1; i < cnt; ++i) {
    std::string key = std::to_string(i);
    auto rst = instance.Get(key);
    ASSERT_TRUE(rst.IsOk());

    auto buffer = rst.Get();
    EXPECT_EQ(0, memcmp(buffer->Data(), key.c_str(), key.length()));
  }
  auto stop_res = instance.Stop();
  EXPECT_TRUE(stop_res.IsOk());
}

TEST_F(CacheInstanceTest, TestDRAMEngine) {
  CacheInstance instance(30, ReplacementPolicyType::kSLRU,
                         StorageEngineType::kDRAMStorageEngine, {});
  instance.SetMetricRegistry(registry_);
  auto init_res = instance.Start();
  ASSERT_TRUE(init_res.IsOk());
  {
    auto get_res = instance.Get("foo");
    EXPECT_FALSE(get_res.IsOk());
    EXPECT_EQ(&Errors::kCacheBufferNotFound, get_res.GetError());
    std::string foodata = "FooData";
    std::unique_ptr<folly::IOBuf> foovalue = folly::IOBuf::copyBuffer(foodata);

    auto put_res = instance.Put("foo", std::move(*foovalue));
    EXPECT_TRUE(put_res.IsOk());

    get_res = instance.Get("foo");
    EXPECT_TRUE(get_res.IsOk());
    auto foo_buffer = get_res.Get();
    EXPECT_NE(nullptr, foo_buffer);
    EXPECT_EQ(0, memcmp(foo_buffer->Data(), foodata.c_str(), foodata.length()));

    char* data_async = new char[5];
    memset(data_async, '1', 5);
    std::string key_async = "async_key";
    // DramStorage does no support AsyncDelete, mark it as false
    // data_async does not belong to any StorageEngine, markt it as nullptr
    auto buffer_async_old =
        std::make_shared<RawBuffer>(data_async, 5, nullptr, false);
    buffer_async_old->SetKey(key_async);

    auto fut = instance.AsyncPut(
        std::static_pointer_cast<CacheBuffer>(buffer_async_old), "11111");
    auto async_res = std::move(fut).get();
    EXPECT_TRUE(async_res.IsOk());
    auto buf_async = std::move(async_res).Get();
    EXPECT_NE(buf_async, buffer_async_old);

    get_res = instance.Get(key_async);
    EXPECT_TRUE(get_res.IsOk());
    auto buf_new = std::move(get_res).Get();
    EXPECT_NE(buf_new, nullptr);
    // new buffer from AsyncPut
    EXPECT_NE(buf_new, buffer_async_old);
    EXPECT_EQ(buf_new, buf_async);
    EXPECT_EQ(0, memcmp(buf_new->Data(), data_async, 5));

    auto del_res = instance.Delete("foo");
    EXPECT_TRUE(del_res.IsOk());
    get_res = instance.Get("foo");
    EXPECT_FALSE(get_res.IsOk());
    EXPECT_EQ(&Errors::kCacheBufferNotFound, get_res.GetError());
  }
  auto stop_res = instance.Stop();
  EXPECT_TRUE(stop_res.IsOk());
}

TEST_F(CacheInstanceTest, TestPMEMEngine) {
  CacheInstance instance(30, ReplacementPolicyType::kSLRU,
                         StorageEngineType::kPMEMStorageEngine, {pmem_path_});
  instance.SetMetricRegistry(registry_);
  auto init_res = instance.Start();
  ASSERT_TRUE(init_res.IsOk());
  {
    auto get_res = instance.Get("foo");
    EXPECT_FALSE(get_res.IsOk());
    EXPECT_EQ(&Errors::kCacheBufferNotFound, get_res.GetError());
    std::string foodata = "FooData";
    std::unique_ptr<folly::IOBuf> foovalue = folly::IOBuf::copyBuffer(foodata);
    {
      auto put_res = instance.Put("foo", std::move(*foovalue));
      EXPECT_TRUE(put_res.IsOk());
    }

    get_res = instance.Get("foo");
    EXPECT_TRUE(get_res.IsOk());
    auto foo_buffer = std::move(get_res).Get();
    EXPECT_NE(nullptr, foo_buffer);
    EXPECT_EQ(0, memcmp(foo_buffer->Data(), foodata.c_str(), foodata.length()));

    auto fut =
        instance.AsyncPut(foo_buffer, "CacheInstanceTest::TestPMEMEngine");
    auto async_res = std::move(fut).get();
    ASSERT_TRUE(async_res.IsOk());
    auto foo_buf_async = std::move(async_res).Get();

    get_res = instance.Get("foo");
    EXPECT_TRUE(get_res.IsOk());
    auto foo_buf_new = std::move(get_res).Get();
    EXPECT_NE(foo_buf_new, nullptr);
    // new buffer from AsyncPut
    EXPECT_NE(foo_buf_new, foo_buffer);
    EXPECT_EQ(foo_buf_new, foo_buf_async);

    auto del_res = instance.Delete("foo");
    EXPECT_TRUE(del_res.IsOk());
    get_res = instance.Get("foo");
    EXPECT_FALSE(get_res.IsOk());
    EXPECT_EQ(&Errors::kCacheBufferNotFound, get_res.GetError());
  }
  auto stop_res = instance.Stop();
  EXPECT_TRUE(stop_res.IsOk());
}

TEST_F(CacheInstanceTest, TestDRAMPoolEngine) {
  FLAGS_pmem_allocator_type = "Pool";
  FLAGS_pool_based_allocator_obj_len = 64;
  CacheInstance instance(256, ReplacementPolicyType::kFIFO,
                         StorageEngineType::kDRAMStorageEngine, {pmem_path_});
  instance.SetMetricRegistry(registry_);

  auto start_res = instance.Start();
  ASSERT_TRUE(start_res.IsOk());
  {
    auto get_res = instance.Get("foo");
    EXPECT_FALSE(get_res.IsOk());
    EXPECT_EQ(&Errors::kCacheBufferNotFound, get_res.GetError());
    std::string foodata = "FooData";
    std::unique_ptr<folly::IOBuf> foovalue = folly::IOBuf::copyBuffer(foodata);
    auto put_res = instance.Put("foo", std::move(*foovalue));
    EXPECT_TRUE(put_res.IsOk());

    get_res = instance.Get("foo");
    EXPECT_TRUE(get_res.IsOk());
    auto foo_buffer = get_res.Get();
    EXPECT_NE(nullptr, foo_buffer);
    EXPECT_EQ(0, memcmp(foo_buffer->Data(), foodata.c_str(), foodata.length()));

    auto del_res = instance.Delete("foo");
    EXPECT_TRUE(del_res.IsOk());
    get_res = instance.Get("foo");
    EXPECT_FALSE(get_res.IsOk());
    EXPECT_EQ(&Errors::kCacheBufferNotFound, get_res.GetError());
  }
  auto stop_res = instance.Stop();
  EXPECT_TRUE(stop_res.IsOk());
}

TEST_F(CacheInstanceTest, SimpleEngine) {
  CacheInstance instance(30, ReplacementPolicyType::kSLRU,
                         StorageEngineType::kSimpleStorageEngine, {});
  instance.SetMetricRegistry(registry_);
  auto init_res = instance.Start();
  ASSERT_TRUE(init_res.IsOk());

  {
    auto get_res = instance.Get("foo");
    EXPECT_FALSE(get_res.IsOk());
    EXPECT_EQ(&Errors::kCacheBufferNotFound, get_res.GetError());
    std::string foodata = "FooData";
    std::unique_ptr<folly::IOBuf> foovalue = folly::IOBuf::copyBuffer(foodata);
    auto put_res = instance.Put("foo", std::move(*foovalue));
    EXPECT_TRUE(put_res.IsOk());

    get_res = instance.Get("foo");
    EXPECT_TRUE(get_res.IsOk());
    auto foo_buffer = get_res.Get();
    EXPECT_NE(nullptr, foo_buffer);
    EXPECT_EQ(0, memcmp(foo_buffer->Data(), foodata.c_str(), foodata.length()));

    auto del_res = instance.Delete("foo");
    EXPECT_TRUE(del_res.IsOk());
    get_res = instance.Get("foo");
    EXPECT_FALSE(get_res.IsOk());
    EXPECT_EQ(&Errors::kCacheBufferNotFound, get_res.GetError());
  }
  auto stop_res = instance.Stop();
  EXPECT_TRUE(stop_res.IsOk());
}

TEST_F(CacheInstanceTest, GCCopy) {
  CacheInstance instance(100, ReplacementPolicyType::kSLRU,
                         StorageEngineType::kSimpleStorageEngine, {});
  instance.SetMetricRegistry(registry_);
  auto init_res = instance.Start();
  ASSERT_TRUE(init_res.IsOk());
  {
    std::string fookey = "foo";
    std::string foodata = "FooData";
    std::unique_ptr<folly::IOBuf> foovalue = folly::IOBuf::copyBuffer(foodata);
    auto put_res = instance.Put(fookey, std::move(*foovalue));
    EXPECT_TRUE(put_res.IsOk());

    auto get_res = instance.Get(fookey);
    EXPECT_TRUE(get_res.IsOk());
    auto foo_buffer = std::move(get_res).Get();
    EXPECT_NE(nullptr, foo_buffer);

    auto* engine = instance.TEST_GetStorageEngine();
    // We use StorageEngine::Put to emulate the allocator copy process.
    // Note that Put method will call Seal() of the internal allocator, which
    // will increase the ref count of the chunk in allocator. But this will
    // not affect the result as the storage engine will be destroyed soon.
    auto foovalue2 = folly::IOBuf::copyBuffer(foodata);
    auto foovalue2_copy = folly::IOBuf::copyBuffer(foodata);
    auto put_res2 = engine->Put(fookey, std::move(*foovalue2));
    ASSERT_TRUE(put_res2.IsOk());
    auto buffer2 = std::move(put_res2).Get();
    put_res2 = engine->Put(fookey, std::move(*foovalue2_copy));
    ASSERT_TRUE(put_res2.IsOk());
    auto buffer2_copy = std::move(put_res2).Get();

    auto res1 = instance.Update("not_found", foo_buffer->Data(), buffer2_copy);
    EXPECT_FALSE(res1.IsOk());
    EXPECT_EQ(res1.GetError(), &Errors::kCacheBufferNotFound);
    res1 = instance.Update(fookey, buffer2->Data(), buffer2_copy);
    EXPECT_FALSE(res1.IsOk());
    EXPECT_EQ(res1.GetError(), &Errors::kCacheReplaceMismatch);

    res1 = instance.Update(fookey, foo_buffer->Data(), buffer2_copy);
    EXPECT_TRUE(res1.IsOk());

    get_res = instance.Get(fookey);
    EXPECT_TRUE(get_res.IsOk());
    auto foo_buffer_copy = std::move(get_res).Get();
    EXPECT_NE(foo_buffer->Data(), foo_buffer_copy->Data());
  }
  auto stop_res = instance.Stop();
  EXPECT_TRUE(stop_res.IsOk());
}

TEST_F(CacheInstanceTest, EvictionHandler) {
  CacheInstance instance(16, ReplacementPolicyType::kSLRU,
                         StorageEngineType::kSimpleStorageEngine, {});
  instance.SetMetricRegistry(registry_);
  auto init_res = instance.Start();
  ASSERT_TRUE(init_res.IsOk());
  {
    std::vector<CacheBufferSharedPtr> evicted;
    instance.RegisterEvictionHandler(
        [&](CacheBufferSharedPtr buffer) { evicted.push_back(buffer); });
    instance.SetEvictionHandlerStatus(true);

    std::string foodata = "Fo";
    std::unique_ptr<folly::IOBuf> foovalue = folly::IOBuf::copyBuffer(foodata);
    auto put_res = instance.Put("foo", std::move(*foovalue));
    EXPECT_TRUE(put_res.IsOk());

    std::string bardata = "BarData";
    std::unique_ptr<folly::IOBuf> barvalue = folly::IOBuf::copyBuffer(bardata);
    put_res = instance.Put("bar", std::move(*barvalue));
    EXPECT_TRUE(put_res.IsOk());

    std::string furdata = "FurDataBB";
    std::unique_ptr<folly::IOBuf> furvalue = folly::IOBuf::copyBuffer(furdata);
    put_res = instance.Put("fur", std::move(*furvalue));
    EXPECT_TRUE(put_res.IsOk());

    EXPECT_EQ(12, instance.GetUsedSpace());
    ASSERT_EQ(2, evicted.size());
    EXPECT_STREQ("foo", evicted[0]->Key().c_str());
    EXPECT_STREQ("bar", evicted[1]->Key().c_str());
  }
  auto stop_res = instance.Stop();
  EXPECT_TRUE(stop_res.IsOk());
}

TEST_F(CacheInstanceTest, ZonedStoreResetAndRecover) {
  uint64_t db_size = 6UL << 30;
  std::string db_path = ssd_path_ + "/" + gen_random(1, 10);
  CacheInstance* instance = new CacheInstance(db_size,
                                              ReplacementPolicyType::kSLRU,
                                              StorageEngineType::kSSDZonedStoreStorageEngine,
                                              {db_path});
  instance->SetMetricRegistry(registry_);
  auto init_res = instance->Start();
  ASSERT_TRUE(init_res.IsOk());

  std::vector<std::string> keys, values;
  std::unordered_set<std::string> keys_set;
  const int kv_pair_size = 20000;  // 20K items
  const int key_lower = 16;
  const int key_upper = 128;

  // Generate all key-value pairs
  for (int i = 0; i < kv_pair_size; i++) {
    auto key = gen_random(key_lower, key_upper);
    int vsize = (100UL << 10);
    char* value;
    rand_string(&value, vsize, false);
    if (keys_set.count(key) != 0) {
      continue;
    }
    keys.push_back(key);
    keys_set.insert(key);
    values.push_back(std::string(value, vsize));
  }
  LOG(INFO) << kv_pair_size << " key-value generated";

  // Insert new key value pairs (about 2GB data, should occupy at least 1 zone)
  std::vector<std::thread> threads;
  for (int i = 0; i < 8; i++) {
    threads.emplace_back([&, i] {
      for (int idx = i; idx < kv_pair_size; idx += 8) {
        auto put_res = instance->Put(keys[idx],
                                    *(folly::IOBuf::copyBuffer(values[idx])));
        EXPECT_TRUE(put_res.IsOk());
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  LOG(INFO) << "Put KV Succeed, total:" << kv_pair_size;

  // Check inserted data
  int get_cnt = 0;
  for (int i = 0; i < keys.size(); ++i) {
    auto get_res = instance->Get(keys[i]);
    EXPECT_TRUE(get_res.IsOk());
    EXPECT_EQ(0, std::memcmp(values[i].data(),
                             get_res.Get()->Data(),
                             get_res.Get()->Size()));
    get_cnt++;
  }
  LOG(INFO) << "Check " << get_cnt << " out of " << keys.size() << " KV pairs";
  LOG(INFO) << "Check KV Succeed";

  // Close & destory instance
  auto stop_res = instance->Stop();
  EXPECT_TRUE(stop_res.IsOk());
  delete instance;
  instance = nullptr;
  LOG(INFO) << "Destory instance success";

  // Reopen the DB
  FLAGS_cache_enable_ssd_data_recovery = true;
  instance = new CacheInstance(db_size,
                               ReplacementPolicyType::kSLRU,
                               StorageEngineType::kSSDZonedStoreStorageEngine,
                               {db_path});
  // TODO(guokuankuan) Cannot set, why?
  // instance->SetMetricRegistry(registry_);
  init_res = instance->Start();
  ASSERT_TRUE(init_res.IsOk());

  // Check recovered data (partially vallid)
  get_cnt = 0;
  for (int i = 0; i < keys.size(); ++i) {
    auto get_res = instance->Get(keys[i]);
    if (get_res.IsOk() && (std::memcmp(values[i].data(),
                           get_res.Get()->Data(),
                           get_res.Get()->Size()) == 0)) {
      get_cnt++;
    }
  }
  EXPECT_GT(get_cnt, 0);
  LOG(INFO) << "Check " << get_cnt << " out of " << keys.size() << " KV pairs";
  stop_res = instance->Stop();
  EXPECT_TRUE(stop_res.IsOk());
  delete instance;
}

TEST_F(CacheInstanceTest, ResetAndRecover) {
  CacheInstance instance(30, ReplacementPolicyType::kSLRU,
                         StorageEngineType::kSimpleStorageEngine, {});
  instance.SetMetricRegistry(registry_);
  auto init_res = instance.Start();
  ASSERT_TRUE(init_res.IsOk());
  // The Put/Get operations to the cache instance are put into a scope so the
  // returned cache buffers are deleted before the cache instance is stopped.
  {
    // Put and Get "foo", the data in the returned CacheBuffer from both calls
    // should be the same.
    std::string foodata = "Fo";
    std::unique_ptr<folly::IOBuf> foovalue = folly::IOBuf::copyBuffer(foodata);
    auto put_res = instance.Put("foo", std::move(*foovalue));
    EXPECT_TRUE(put_res.IsOk());
    auto get_res = instance.Get("foo");
    EXPECT_TRUE(get_res.IsOk());
    EXPECT_EQ(0, memcmp(put_res.Get()->Data(), get_res.Get()->Data(),
                        get_res.Get()->Size()));

    // Put and Get "bar" into the cache instance and both calls should be
    // successful.
    std::string bardata = "BarData";
    std::unique_ptr<folly::IOBuf> barvalue = folly::IOBuf::copyBuffer(bardata);
    put_res = instance.Put("bar", std::move(*barvalue));
    EXPECT_TRUE(put_res.IsOk());
    get_res = instance.Get("bar");
    EXPECT_TRUE(get_res.IsOk());

    // Both the key and the value of each cache buffer are counted as used cache
    // space, thus the total used space by foo and bar should be 3 ("foo") + 2
    // ("Fo") + 3 ("bar")
    // + 7 ("BarData") = 15.
    EXPECT_EQ(15, instance.GetUsedSpace());

    // Reset the cache instance and then neither foo nor bar can be found, and
    // the used space should become zero.
    EXPECT_TRUE(instance.Reset().IsOk());
    EXPECT_EQ(0, instance.GetUsedSpace());
    get_res = instance.Get("foo");
    EXPECT_FALSE(get_res.IsOk());
    EXPECT_EQ(&Errors::kCacheBufferNotFound, get_res.GetError());
    get_res = instance.Get("bar");
    EXPECT_FALSE(get_res.IsOk());
    EXPECT_EQ(&Errors::kCacheBufferNotFound, get_res.GetError());
  }
  auto stop_res = instance.Stop();
  EXPECT_TRUE(stop_res.IsOk());
}

}  // namespace mtcache

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
