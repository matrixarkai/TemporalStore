
#include "storage/zoned_store/zoned_store.h"

#include "common/cache_error.h"
#include "common/logging.h"

#include <folly/io/IOBuf.h>
#include <gtest/gtest.h>
#include <noodle/base/result.h>
#include <noodle/test_util/sync_point.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace mtcache {

DEFINE_string(store_test_db_dir, "", "DB Path");

const uint64_t SMALL_DEVICE_SZ = 3UL << 30;
const uint64_t LARGE_DEVICE_SZ = 6UL << 30;

class StoreTest : public testing::Test {
 protected:
  std::shared_ptr<StorageEngineZonedStore> store_;

  std::string db_path_;
  std::string directory_path_;

  uint32_t user_buf_ = 1;
  uint32_t gc_buf_ = 1;
  double flush_thread_ = 0.5;
  uint32_t toy_capacity_per_buf_ = (16ul << 10);     // 16KB
  uint32_t formal_capacity_per_buf_ = (16ul << 20);  // 16MB

  void CreateToyZonedStoreObj() {
    store_ = std::make_shared<StorageEngineZonedStore>(
        db_path_, SMALL_DEVICE_SZ, 1 /* large mode */, user_buf_,
        gc_buf_, toy_capacity_per_buf_, flush_thread_,
        false /* using existing db */);
    ASSERT_TRUE(store_->Start());
  }

  void CreateFormalZonedStoreObj() {
    // buffer is 4MB
    store_ = std::make_shared<StorageEngineZonedStore>(
        db_path_, LARGE_DEVICE_SZ, 1 /* large mode */, user_buf_,
        gc_buf_, formal_capacity_per_buf_, flush_thread_,
        false /* using existing db */);
    ASSERT_TRUE(store_->Start());
  }

  // [lower,upper]
  std::string gen_random(const int lower, const int upper) {
    const int len = (::rand() % (upper - lower + 1)) + lower;
    const char optional_char[] = "0123456789-./&*(abcdefgABCD)";
    std::string s(len, 0);
    for (int i = 0; i < len; i++) {
      s[i] = optional_char[::rand() % (sizeof(optional_char) - 1)];
    }
    return s;
  }

  void SetUp() override {
    if (!FLAGS_store_test_db_dir.empty()) {
      db_path_ = FLAGS_store_test_db_dir;
      std::cout << "use user defined dbpath: " << db_path_ << std::endl;
      return;
    }
    char path[64] = "/tmp/zoned_store_XXXXXX";
    if (mkdtemp(path) == nullptr) {
      LOG(FATAL) << "create tmp_dir failed, errno=" << errno;
    }
    directory_path_ = std::string(path);
    db_path_ = std::string(path) + "/dbase";
  }

  void TearDown() override {
    store_->Stop();
    std::filesystem::remove_all(directory_path_);
  }
};

// Test "memory dirty read" case
// time1: put <k1,v1>
// time2: user get 'k1', and only completes the first half.
// time3: inner flush thread flush <k1,v1> from memory to SSD.
// time4:  user get completes the second half.
TEST_F(StoreTest, MemoryItemLifetimeTest) {
  NOODLE_INIT_SYNC_POINT_SINGLETONS()->LoadDependency(
      {{"BufferManager::FlushBuffers_END",
        "StorageEngineZonedStore::Get_MemoryCase2"},
       {"StorageEngineZonedStore::Get_MemoryCase",
        "BufferManager::FlushBuffer_BEGIN"}});
  NOODLE_INIT_SYNC_POINT_SINGLETONS()->EnableProcessing();

  CreateToyZonedStoreObj();
#if DCHECK_IS_ON()
  const std::string expected_key(4096, 'k');
  const std::string expected_value(4096, 'v');
  store_->Put(expected_key, *(folly::IOBuf::copyBuffer(expected_value)));

  auto result = store_->Get(expected_key);

  ASSERT_TRUE(result.IsOk());
  auto result_length = result.Get()->Size();
  ASSERT_EQ(4096, result_length);
  std::string actual_value(result.Get()->Data(), result_length);
  ASSERT_EQ(expected_value, actual_value);
#endif
  NOODLE_INIT_SYNC_POINT_SINGLETONS()->DisableProcessing();
}

// Large batches of data, maintain a in-memory
// map as a reference.
// Single-thread test.
TEST_F(StoreTest, LargePutGetDeleteTest) {
  CreateFormalZonedStoreObj();

  std::unordered_map<std::string, std::string> expected_data;
  const int first_batches = 500;
  const int second_batched = 25;
  const int key_lower = 16;
  const int key_upper = 128;
  const int value_lower = (1UL << 10);
  const int value_mid = (256UL << 10);
  const int value_upper = (16UL << 20) - 8192;

  for (int i = 0; i < first_batches; i++) {
    auto key = gen_random(key_lower, key_upper);
    auto value = gen_random(value_lower, value_mid);
    if (expected_data.count(key) != 0) {
      continue;
    }
    store_->Put(key, *(folly::IOBuf::copyBuffer(value)));
    expected_data[key] = std::move(value);
  }
  LOG(INFO) << "Put Small KV Succeed";
  for (int i = 0; i < second_batched; i++) {
    auto key = gen_random(key_lower, key_upper);
    auto value = gen_random(value_mid, value_upper);
    if (expected_data.count(key) != 0) {
      continue;
    }
    store_->Put(key, *(folly::IOBuf::copyBuffer(value)));
    expected_data[key] = std::move(value);
  }
  LOG(INFO) << "Put Large KV Succeed";
  for (const auto& data_pair : expected_data) {
    auto result = store_->Get(data_pair.first);
    ASSERT_TRUE(result.IsOk());
    auto result_length = result.Get()->Size();
    int expected_length = data_pair.second.size();

    ASSERT_EQ(result_length, expected_length);

    std::string actual_value(result.Get()->Data(), result_length);
    ASSERT_EQ(actual_value, data_pair.second);
  }
  LOG(INFO) << "Get KV Succeed";
}

TEST_F(StoreTest, RestartWithSmallerCapacityTest) {
  store_ = std::make_shared<StorageEngineZonedStore>(
      db_path_, LARGE_DEVICE_SZ, 1 /* large mode */,
      10 /*user buf cnt*/, gc_buf_, formal_capacity_per_buf_, flush_thread_,
      false /* using existing db */);
  ASSERT_TRUE(store_->Start());

  // Fill enough data, larger than the cache capacity
  std::atomic<uint64_t> sum = 0;
  std::vector<std::thread> workers;
  for (int i = 0; i < 10; ++i) {
    workers.emplace_back([&, i]() {
      LOG(INFO) << "thread " << i << " started" << std::endl;
      auto value = gen_random(1 << 20, 1 << 20);
      for (int j = 0; j < 1000; ++j) {
        auto key = "key_" + std::to_string(i) + "_" + std::to_string(j);
        store_->Put(key, *(folly::IOBuf::copyBuffer(value)));
        sum += (1 << 20);
      }
      LOG(INFO) << "thread " << i << " ended" << std::endl;
    });
  }
  for (auto& worker : workers) {
    worker.join();
  }
  store_->Stop();
  LOG(INFO) << "Put data and close DB, data size: " << (sum.load()) << " bytes";

  // Restart with a smaller capacity
  store_.reset(new StorageEngineZonedStore(db_path_, SMALL_DEVICE_SZ, 1, 10, gc_buf_,
                                           formal_capacity_per_buf_,
                                           flush_thread_, true));

  store_->Start();
}

TEST_F(StoreTest, UseLargeBufferSize) {
  store_ = std::make_shared<StorageEngineZonedStore>(
      db_path_, SMALL_DEVICE_SZ, 1, 5, gc_buf_, 1 << 30 /*buf size*/, flush_thread_,
      false);
  store_->Start();
  auto value = gen_random(1 << 20, 1 << 20);
  for (int i = 0; i < 8000; ++i) {
    auto key = "key_" + std::to_string(i);
    store_->Put(key, *(folly::IOBuf::copyBuffer(value)));
  }
}

TEST_F(StoreTest, OutOfSpaceTest) {
  store_ = std::make_shared<StorageEngineZonedStore>(
      db_path_, SMALL_DEVICE_SZ, 1, 5, gc_buf_, 1 << 30 /*buf size*/, flush_thread_,
      false);
  store_->Start();
  auto value = gen_random(1 << 20, 1 << 20);
  // Total 8GB data, write into `SMALL_SIZE`(3GB) database
  for (int i = 0; i < 8000; ++i) {
    auto key = "key_" + std::to_string(i);
    store_->Put(key, *(folly::IOBuf::copyBuffer(value)));
  }
}

}  // namespace mtcache

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
