#include "policy/slru.h"

#include "common/logging.h"
#include "storage/simple_storage.h"

#include <gtest/gtest.h>

#include <random>

// We use cache_slru_ut to indicate that we are testing SLRU UT
DECLARE_bool(cache_slru_ut);

namespace mtcache {

TEST(ReplacementSLRU, Put) {
  StorageEngineSimple engine;
  ASSERT_TRUE(engine.Start());
  ReplacementSLRU slru(5);
  slru.Init();
  slru.TEST_ConfigLRUMaintainer(false);

  EXPECT_EQ(5, slru.GetCapacity());
  EXPECT_EQ(0, slru.GetUsedSpace());
  EXPECT_EQ(5, slru.GetFreeSpace());

  // This is a corner case that should not happen in production. But if the
  // value size of the cached buffers is greater than the cache capacity, it
  // will be evicted immediately
  std::string foodata = "FooData";
  std::unique_ptr<folly::IOBuf> foovalue = folly::IOBuf::copyBuffer(foodata);
  auto engine_res = engine.Put("foo", std::move(*foovalue));
  ASSERT_TRUE(engine_res.IsOk());
  auto cache_buffer = std::move(engine_res.Get());
  auto put_res = slru.Put(cache_buffer);
  ASSERT_FALSE(put_res.empty());
  EXPECT_STREQ(cache_buffer->Key().c_str(), put_res[0]->Key().c_str());
  EXPECT_EQ(0, slru.GetUsedSpace());
  EXPECT_EQ(5, slru.GetFreeSpace());

  // Update capacity and try again
  slru.SetCapacity(16);
  EXPECT_EQ(16, slru.GetCapacity());
  put_res = slru.Put(cache_buffer);
  EXPECT_TRUE(put_res.empty());
  EXPECT_EQ(10, slru.GetUsedSpace());
  EXPECT_EQ(6, slru.GetFreeSpace());

  // Evict buffers
  std::string bardata = "Bar";
  std::unique_ptr<folly::IOBuf> barvalue = folly::IOBuf::copyBuffer(bardata);
  engine_res = engine.Put("bar", std::move(*barvalue));
  ASSERT_TRUE(engine_res.IsOk());
  cache_buffer = std::move(engine_res.Get());
  put_res = slru.Put(cache_buffer);
  EXPECT_TRUE(put_res.empty());
  EXPECT_EQ(16, slru.GetUsedSpace());
  EXPECT_EQ(0, slru.GetFreeSpace());

  std::string cdata = "TestData";
  std::unique_ptr<folly::IOBuf> cvalue = folly::IOBuf::copyBuffer(cdata);
  engine_res = engine.Put("c", std::move(*cvalue));
  ASSERT_TRUE(engine_res.IsOk());
  cache_buffer = std::move(engine_res.Get());
  put_res = slru.Put(cache_buffer);
  EXPECT_FALSE(put_res.empty());
  EXPECT_EQ(15, slru.GetUsedSpace());
  EXPECT_EQ(1, slru.GetFreeSpace());
}

TEST(ReplacementSLRU, PutGetDelete) {
  StorageEngineSimple engine;
  ASSERT_TRUE(engine.Start());
  ReplacementSLRU slru(25);
  slru.Init();
  slru.TEST_ConfigLRUMaintainer(false);

  // Test Get method returns buffer installed by Put method
  std::string foodata = "FooData";
  auto foovalue = folly::IOBuf::copyBuffer(foodata);
  auto engine_res = engine.Put("foo", std::move(*foovalue));
  ASSERT_TRUE(engine_res.IsOk());
  auto foo_cache_buffer = std::move(engine_res.Get());
  auto put_res = slru.Put(foo_cache_buffer);
  EXPECT_TRUE(put_res.empty());
  EXPECT_NE(nullptr, slru.Get("foo"));

  // Install another buffer and access foo last
  std::string bardata = "BarData";
  auto barvalue = folly::IOBuf::copyBuffer(bardata);
  engine_res = engine.Put("bar", std::move(*barvalue));
  ASSERT_TRUE(engine_res.IsOk());
  auto bar_cache_buffer = std::move(engine_res.Get());
  EXPECT_EQ(nullptr, slru.Get("bar"));
  put_res = slru.Put(bar_cache_buffer);
  EXPECT_TRUE(put_res.empty());
  EXPECT_NE(nullptr, slru.Get("bar"));
  EXPECT_EQ(20, slru.GetUsedSpace());
  EXPECT_NE(nullptr, slru.Get("foo"));

  // Install another buffer and foo is evicted
  std::string bazdata = "BazBazData";
  auto bazvalue = folly::IOBuf::copyBuffer(bazdata);
  engine_res = engine.Put("baz", std::move(*bazvalue));
  ASSERT_TRUE(engine_res.IsOk());
  auto baz_cache_buffer = std::move(engine_res.Get());
  put_res = slru.Put(baz_cache_buffer);
  ASSERT_FALSE(put_res.empty());
  EXPECT_EQ(1, put_res.size());
  EXPECT_STREQ("foo", put_res[0]->Key().c_str());
  EXPECT_EQ(nullptr, slru.Get("foo"));
  EXPECT_NE(nullptr, slru.Get("baz"));
  EXPECT_NE(nullptr, slru.Delete("baz"));
  EXPECT_EQ(nullptr, slru.Get("baz"));
}

TEST(ReplacementSLRU, AccessAfterReset) {
  StorageEngineSimple engine;
  ASSERT_TRUE(engine.Start());
  ReplacementSLRU slru(20);
  slru.Init();
  slru.TEST_ConfigLRUMaintainer(false);

  std::string foodata = "FooData";
  auto foovalue = folly::IOBuf::copyBuffer(foodata);
  auto engine_res = engine.Put("foo", std::move(*foovalue));
  ASSERT_TRUE(engine_res.IsOk());
  auto foo_cache_buffer = std::move(engine_res.Get());
  auto put_res = slru.Put(foo_cache_buffer);
  EXPECT_TRUE(put_res.empty());
  EXPECT_EQ(10, slru.GetUsedSpace());

  slru.Reset();

  EXPECT_EQ(0, slru.GetUsedSpace());
  EXPECT_EQ(20, slru.GetFreeSpace());
  EXPECT_EQ(nullptr, slru.Get("foo"));
  EXPECT_EQ(nullptr, slru.Delete("foo"));

  std::string bardata = "BarData";
  auto barvalue = folly::IOBuf::copyBuffer(bardata);
  engine_res = engine.Put("bar", std::move(*barvalue));
  ASSERT_TRUE(engine_res.IsOk());
  auto bar_cache_buffer = std::move(engine_res.Get());
  EXPECT_EQ(slru.Put(bar_cache_buffer).size(), 0);
}

// hot:warm:cold = 20:40:40
TEST(ReplacementSLRU, TestMaintainer) {
  FLAGS_cache_slru_ut = true;
  StorageEngineSimple engine;
  ASSERT_TRUE(engine.Start());
  // byte-hot:warm:cold = 8B:16B:16B
  ReplacementSLRU slru(40);
  slru.Init();

  std::string foodata = "FooData";
  auto foovalue = folly::IOBuf::copyBuffer(foodata);
  auto engine_res = engine.Put("foo", std::move(*foovalue));
  ASSERT_TRUE(engine_res.IsOk());
  auto foo_cache_buffer = std::move(engine_res.Get());
  auto put_res = slru.Put(foo_cache_buffer);
  EXPECT_TRUE(put_res.empty());

  auto foo_pos = slru.TEST_CheckLRUPos("foo");
  auto foo_flag = slru.TEST_CheckBufferFlag("foo");
  // "foo" should in hot lru
  EXPECT_EQ(0, foo_pos);
  EXPECT_EQ(BUFFER_INIT, foo_flag);
  EXPECT_NE(nullptr, slru.Get("foo"));
  foo_flag = slru.TEST_CheckBufferFlag("foo");
  // only access "foo" once
  EXPECT_EQ(BUFFER_FETCHED, foo_flag);

  std::string bardata = "BarData";
  auto barvalue = folly::IOBuf::copyBuffer(bardata);
  engine_res = engine.Put("bar", std::move(*barvalue));
  ASSERT_TRUE(engine_res.IsOk());
  auto bar_cache_buffer = std::move(engine_res.Get());
  put_res = slru.Put(bar_cache_buffer);
  EXPECT_TRUE(put_res.empty());
  auto bar_pos = slru.TEST_CheckLRUPos("bar");
  auto bar_flag = slru.TEST_CheckBufferFlag("bar");
  // "bar" should in hot lru
  EXPECT_EQ(0, bar_pos);
  EXPECT_EQ(BUFFER_INIT, bar_flag);

  // wait Maintainer move "foo" to cold lru
  slru.TEST_WaitForLRUMaintainer();
  foo_pos = slru.TEST_CheckLRUPos("foo");
  // "foo" should in hot lru
  EXPECT_EQ(2, foo_pos);
  FLAGS_cache_slru_ut = false;
}

TEST(ReplacementSLRU, ConcurrentStressTest) {
  std::default_random_engine e;
  StorageEngineSimple engine;
  std::uniform_int_distribution<unsigned> u(0, 100);
  ASSERT_TRUE(engine.Start());
  ReplacementSLRU slru(30);
  slru.Init();

  std::string data = "FooData";
  std::string key = "foo";
  auto datavalue = folly::IOBuf::copyBuffer(data);
  auto engine_res = engine.Put(key, std::move(*datavalue));
  ASSERT_TRUE(engine_res.IsOk());
  auto foo_cache_buffer = std::move(engine_res.Get());
  auto put_res = slru.Put(foo_cache_buffer);

  std::atomic<int> iterations{2000};
  std::vector<std::thread> threads;
  unsigned int num_threads = 6;
  threads.reserve(num_threads);

  for (uint32_t t = 0; t < num_threads; t++) {
    threads.push_back(std::thread([&]() {
      while (--iterations >= 0) {
        data[0] = 'a' + (iterations % 10);
        // generate a random key between 0 to 100
        std::string new_key = std::to_string(u(e));
        auto new_value = folly::IOBuf::copyBuffer(data);
        auto new_engine_res = engine.Put(new_key, std::move(*new_value));
        ASSERT_TRUE(new_engine_res.IsOk());
        auto new_cache_buffer = std::move(new_engine_res.Get());
        // put first
        auto new_put_res = slru.Put(new_cache_buffer);

        if (iterations % 2 == 0) {
          // get
          auto new_get_rest = slru.Get(new_key);
        } else {
          // delete
          auto new_delete_res = slru.Delete(new_key);
        }
      }
    }));
  }
  for (auto& t : threads) {
    t.join();
  }
}

// TODO(kai.wu) add more UTs

}  // namespace mtcache

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
