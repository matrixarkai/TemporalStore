#include "policy/fifo.h"

#include "common/logging.h"
#include "storage/simple_storage.h"

#include <gtest/gtest.h>

namespace mtcache {

TEST(ReplacementFIFO, Put) {
  StorageEngineSimple engine;
  ASSERT_TRUE(engine.Start());
  ReplacementFIFO fifo(5);
  fifo.Init();
  EXPECT_EQ(5, fifo.GetCapacity());
  EXPECT_EQ(0, fifo.GetUsedSpace());
  EXPECT_EQ(5, fifo.GetFreeSpace());

  // This is a corner case that should not happen in production. But if the
  // value size of the cached buffer is greater than the cache capacity, it will
  // be evicted immediately
  std::string foodata = "FooData";
  std::unique_ptr<folly::IOBuf> foovalue = folly::IOBuf::copyBuffer(foodata);
  auto engine_res = engine.Put("foo", std::move(*foovalue));
  ASSERT_TRUE(engine_res.IsOk());
  auto cache_buffer = std::move(engine_res.Get());
  auto put_res = fifo.Put(cache_buffer);
  ASSERT_FALSE(put_res.empty());
  EXPECT_STREQ(cache_buffer->Key().c_str(), put_res[0]->Key().c_str());
  EXPECT_EQ(0, fifo.GetUsedSpace());
  EXPECT_EQ(5, fifo.GetFreeSpace());

  // Update capacity and try again
  fifo.SetCapacity(16);
  EXPECT_EQ(16, fifo.GetCapacity());
  put_res = fifo.Put(cache_buffer);
  EXPECT_TRUE(put_res.empty());
  EXPECT_EQ(10, fifo.GetUsedSpace());
  EXPECT_EQ(6, fifo.GetFreeSpace());

  // Evict buffers
  std::string bardata = "Bar";
  std::unique_ptr<folly::IOBuf> barvalue = folly::IOBuf::copyBuffer(bardata);
  engine_res = engine.Put("bar", std::move(*barvalue));
  ASSERT_TRUE(engine_res.IsOk());
  cache_buffer = std::move(engine_res.Get());
  put_res = fifo.Put(cache_buffer);
  EXPECT_TRUE(put_res.empty());
  EXPECT_EQ(16, fifo.GetUsedSpace());
  EXPECT_EQ(0, fifo.GetFreeSpace());

  std::string cdata = "TestData";
  std::unique_ptr<folly::IOBuf> cvalue = folly::IOBuf::copyBuffer(cdata);
  engine_res = engine.Put("c", std::move(*cvalue));
  ASSERT_TRUE(engine_res.IsOk());
  cache_buffer = std::move(engine_res.Get());
  put_res = fifo.Put(cache_buffer);
  EXPECT_FALSE(put_res.empty());
  EXPECT_EQ(15, fifo.GetUsedSpace());
  EXPECT_EQ(1, fifo.GetFreeSpace());
}

TEST(ReplacementFIFO, PutGetDelete) {
  StorageEngineSimple engine;
  ASSERT_TRUE(engine.Start());
  ReplacementFIFO fifo(25);
  fifo.Init();

  // Test Get method returns buffer installed by Put method
  std::string foodata = "FooData";
  auto foovalue = folly::IOBuf::copyBuffer(foodata);
  auto engine_res = engine.Put("foo", std::move(*foovalue));
  ASSERT_TRUE(engine_res.IsOk());
  auto foo_cache_buffer = std::move(engine_res.Get());
  auto put_res = fifo.Put(foo_cache_buffer);
  EXPECT_TRUE(put_res.empty());
  EXPECT_NE(nullptr, fifo.Get("foo"));

  // Install another buffer and access foo last
  std::string bardata = "BarData";
  auto barvalue = folly::IOBuf::copyBuffer(bardata);
  engine_res = engine.Put("bar", std::move(*barvalue));
  ASSERT_TRUE(engine_res.IsOk());
  auto bar_cache_buffer = std::move(engine_res.Get());
  EXPECT_EQ(nullptr, fifo.Get("bar"));
  put_res = fifo.Put(bar_cache_buffer);
  EXPECT_TRUE(put_res.empty());
  EXPECT_NE(nullptr, fifo.Get("bar"));
  EXPECT_EQ(20, fifo.GetUsedSpace());
  EXPECT_NE(nullptr, fifo.Get("foo"));

  // Install another buffer and foo is evicted
  std::string bazdata = "BazBazData";
  auto bazvalue = folly::IOBuf::copyBuffer(bazdata);
  engine_res = engine.Put("baz", std::move(*bazvalue));
  ASSERT_TRUE(engine_res.IsOk());
  auto baz_cache_buffer = std::move(engine_res.Get());
  put_res = fifo.Put(baz_cache_buffer);
  ASSERT_FALSE(put_res.empty());
  EXPECT_EQ(1, put_res.size());
  EXPECT_STREQ("foo", put_res[0]->Key().c_str());
  EXPECT_EQ(nullptr, fifo.Get("foo"));
  EXPECT_NE(nullptr, fifo.Get("baz"));
  EXPECT_NE(nullptr, fifo.Delete("baz"));
  EXPECT_EQ(nullptr, fifo.Get("baz"));
}

TEST(ReplacementFIFO, PutUpdate) {
  StorageEngineSimple engine;
  ASSERT_TRUE(engine.Start());
  ReplacementFIFO fifo(25);
  fifo.Init();

  // Test Get method returns buffers installed by Put method
  std::string foodata1 = "FooData";
  std::string foodata2 = "Foo";
  auto foovalue1 = folly::IOBuf::copyBuffer(foodata1);
  auto foovalue2 = folly::IOBuf::copyBuffer(foodata2);
  auto engine_res1 = engine.Put("foo", std::move(*foovalue1));
  ASSERT_TRUE(engine_res1.IsOk());
  auto engine_res2 = engine.Put("foo", std::move(*foovalue2));
  auto foo_cache_buffer1 = std::move(engine_res1.Get());
  auto foo_cache_buffer2 = std::move(engine_res2.Get());
  // After putting foo buffer1, the used space should be 3 + 7 = 10
  auto put_res1 = fifo.Put(foo_cache_buffer1);
  EXPECT_TRUE(put_res1.empty());
  EXPECT_NE(nullptr, fifo.Get("foo"));
  EXPECT_EQ(10, fifo.GetUsedSpace());

  // After putting (updating) foo with buffer2, the used space should be 3 + 3 =
  // 6
  auto put_res2 = fifo.Put(foo_cache_buffer2);
  EXPECT_TRUE(put_res2.empty());
  EXPECT_NE(nullptr, fifo.Get("foo"));
  EXPECT_EQ(6, fifo.GetUsedSpace());
}

TEST(ReplacementFIFO, AccessAfterReset) {
  StorageEngineSimple engine;
  ASSERT_TRUE(engine.Start());
  ReplacementFIFO fifo(20);
  fifo.Init();

  std::string foodata = "FooData";
  auto foovalue = folly::IOBuf::copyBuffer(foodata);
  auto engine_res = engine.Put("foo", std::move(*foovalue));
  ASSERT_TRUE(engine_res.IsOk());
  auto foo_cache_buffer = std::move(engine_res.Get());
  auto put_res = fifo.Put(foo_cache_buffer);
  EXPECT_TRUE(put_res.empty());
  EXPECT_EQ(10, fifo.GetUsedSpace());

  fifo.Reset();
  EXPECT_EQ(0, fifo.GetUsedSpace());
  EXPECT_EQ(20, fifo.GetFreeSpace());
  EXPECT_EQ(nullptr, fifo.Get("foo"));
  EXPECT_EQ(nullptr, fifo.Delete("foo"));

  std::string bardata = "BarData";
  auto barvalue = folly::IOBuf::copyBuffer(bardata);
  engine_res = engine.Put("bar", std::move(*barvalue));
  ASSERT_TRUE(engine_res.IsOk());
  auto bar_cache_buffer = std::move(engine_res.Get());
  EXPECT_EQ(fifo.Put(bar_cache_buffer).size(), 0);
}

TEST(ReplacementFIFO, TestOldCacheBufferReleasedAfterUpdate) {
  StorageEngineSimple engine;
  ASSERT_TRUE(engine.Start());
  ReplacementFIFO fifo(1024);
  fifo.Init();
  {
    for (int i = 0; i < 21; i++) {
      // Create a CacheBuffer
      std::string key = std::to_string(i);
      std::string foodata = "Foo";
      auto foovalue = folly::IOBuf::copyBuffer(foodata);
      auto engine_res = engine.Put(key, std::move(*foovalue));
      auto foo_cache_buffer = std::move(engine_res.Get());
      // Put the CacheBuffer into fifo replacement policy
      fifo.Put(foo_cache_buffer);
    }
  }
  {
    // Folly hazptr gc threshold is 20
    for (int i = 0; i < 20; i++) {
      std::string key = std::to_string(i);
      std::string foodata2 = "Foo2";
      auto foovalue2 = folly::IOBuf::copyBuffer(foodata2);
      auto engine_res2 = engine.Put(key, std::move(*foovalue2));
      auto foo_cache_buffer2 = std::move(engine_res2.Get());
      auto old_buf = fifo.Get(key);
      EXPECT_NE(nullptr, old_buf);
      // Use a new CacheBuffer to replace the old CacheBuffer in fifo
      // replacement policy and the old CacheBuffer will be destructured
      fifo.UpdateCacheBuffer(key, old_buf->Data(), foo_cache_buffer2);
    }
  }
  folly::hazptr_cleanup();
  LOG(INFO) << "************ All old CacheBuffers  destruction should be done "
               "before this ************";
  // Only 19 data hazptrs will be reclaimed because the first data hazptr is
  // held by iterator of folly concurrent hashmap without reclamation
  ASSERT_GE(engine.TEST_GetNumDeleteCompletedCount(), 19);

  fifo.Reset();
  ASSERT_TRUE(engine.Stop());
  LOG(INFO) << "Stop engine";
}

}  // namespace mtcache

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
