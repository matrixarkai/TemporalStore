
#include "common/cache_error.h"
#include "storage/zoned_store/zoned_store.h"
#include "tools/utils.h"

#include <folly/io/IOBuf.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <noodle/base/result.h>
#include <noodle/test_util/sync_point.h>

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
DEFINE_string(recovery_test_ssd_path_, "/tmp/zoned_store_recovery_test",
              "ssd path or file path");
DEFINE_uint64(total_pairs_, 20000, "num of total kv pairs(around 100KB)");
DEFINE_uint64(thread_num_, 16, "num of threads putting data");
DEFINE_uint64(cache_capacity_, 6ULL << 30, "ZonedStore capacity");

class ZonedStoreRecoveryTest : public testing::Test {
 protected:
  std::shared_ptr<StorageEngineZonedStore> store_;

  std::string db_path_;
  std::string directory_path_;

  uint32_t user_buf_ = 1;
  uint32_t gc_buf_ = 1;
  double flush_thread_ = 0.5;
  uint32_t toy_capacity_per_buf_ = (16ul << 10);     // 16KB
  uint32_t formal_capacity_per_buf_ = (16ul << 20);  // 16MB

  void CreateZonedStoreObj(bool using_existing_db) {
    // buffer is 4MB
    store_ = std::make_shared<StorageEngineZonedStore>(
        db_path_, FLAGS_cache_capacity_ /* capacity */, 1 /* large mode */,
        user_buf_, gc_buf_, formal_capacity_per_buf_, flush_thread_,
        using_existing_db /* using existing db */);
    ASSERT_TRUE(store_->Start());
  }

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

  void SetUp() override {
    directory_path_ = FLAGS_recovery_test_ssd_path_;
    if (std::filesystem::is_block_file(directory_path_)) {
      db_path_ = directory_path_;
    } else {
      db_path_ = directory_path_ + "/dbase";
    }
  }

  void TearDown() override {
    store_->Stop();
    if (!std::filesystem::is_block_file(directory_path_)) {
      std::filesystem::remove_all(db_path_);
    }
  }
};

// Large batches of data, maintain a in-memory
// map as a reference.
// Single-thread test.
TEST_F(ZonedStoreRecoveryTest, ZonedStore_RecoveryTest) {
  CreateZonedStoreObj(false);

  // std::unordered_map<std::string, std::string> expected_data;
  std::vector<std::string> keys, values;
  std::unordered_set<std::string> keys_set;
  const int kv_pair_size = FLAGS_total_pairs_;
  const int key_lower = 16;
  const int key_upper = 128;
  const int value_lower = (90UL << 10);
  const int value_upper = (110UL << 10);
  {
    for (int i = 0; i < kv_pair_size; i++) {
      auto key = gen_random(key_lower, key_upper);
      int vsize =
          (fast_rand16() % (value_upper - value_lower + 1)) + value_lower;
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

    std::vector<std::thread> threads;
    for (int i = 0; i < FLAGS_thread_num_; i++) {
      threads.emplace_back([&, i] {
        for (int idx = i; idx < kv_pair_size; idx += FLAGS_thread_num_) {
          store_->Put(keys[idx], *(folly::IOBuf::copyBuffer(values[idx])));
        }
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }
    LOG(INFO) << "Put KV Succeed";

    int get_cnt = 0;
    for (int i = 0; i < keys.size(); ++i) {
      auto result = store_->Get(keys[i]);
      /*
      if (result.GetError()) {
        EXPECT_TRUE(result.GetError() == &Errors::kStorageBufferNotFound);
        continue;
      }
      */
      ASSERT_TRUE(result.IsOk());
      auto result_length = result.Get()->Size();
      int expected_length = values[i].size();

      ASSERT_EQ(result_length, expected_length);

      std::string actual_value(result.Get()->Data(), result_length);
      ASSERT_EQ(actual_value, values[i]);
      get_cnt++;
    }
    LOG(INFO) << "Check " << get_cnt << " of " << keys.size() << " KV pairs";
    LOG(INFO) << "Check KV Succeed";

    store_->Stop();
  }

  {
    LOG(INFO) << "Start Recovery";
    auto start = std::chrono::system_clock::now();
    CreateZonedStoreObj(true);
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> time_cost = end - start;
    LOG(INFO) << "CreateZonedStore WITH Recovery Time cost: "
              << time_cost.count() << "s";
    int rec_cnt = 0;
    // for (const auto& data_pair : expected_data) {
    for (int i = 0; i < keys.size(); ++i) {
      // auto result = store_->Get(data_pair.first);
      auto result = store_->Get(keys[i]);
      if (result.GetError()) {
        // ASSERT_TRUE(result.GetError() == &Errors::kStorageBufferNotFound);
        continue;
      }
      ASSERT_TRUE(result.IsOk());
      auto result_length = result.Get()->Size();
      int expected_length = values[i].size();

      ASSERT_EQ(result_length, expected_length);

      std::string actual_value(result.Get()->Data(), result_length);
      ASSERT_EQ(actual_value, values[i]);
      rec_cnt++;
    }
    LOG(INFO) << "Check " << rec_cnt << " of " << keys.size() << " KV pairs";
    LOG(INFO) << "Check KV Succeed after Recovery";
    store_->Stop();
  }

  {
    LOG(INFO) << "Start Without Recovery";
    auto start = std::chrono::system_clock::now();
    CreateZonedStoreObj(false);
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> time_cost = end - start;
    LOG(INFO) << "CreateZonedStore WITHOUT Recovery Time cost: "
              << time_cost.count() << "s";
    for (int i = 0; i < kv_pair_size;
         i += (fast_rand16() % (kv_pair_size / 100))) {
      auto result = store_->Get(keys[i]);
      // if(result.GetError()) {
      //   continue;
      // }
      ASSERT_TRUE(result.GetError());
    }
    LOG(INFO) << "Get Nothing Without Recovery";
  }
}

}  // namespace mtcache

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
