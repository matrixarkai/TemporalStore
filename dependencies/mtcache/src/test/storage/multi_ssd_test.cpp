#include "storage/multi_ssd.h"

#include "buffer/string_buffer.h"
#include "buffer/string_view_buffer.h"
#include "storage/ssd_terarkdb.h"
#include "storage/zoned_store/zoned_store.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <stdlib.h>

DECLARE_bool(cache_ssd_data_recovery_in_background);

namespace mtcache {

#define CACHE_CAPACITY (4UL << 30)
#define DISK_NUM 2

class RecoverDataCallback4Test : public StorageEngine::RecoverDataCallback {
 public:
  RecoverDataCallback4Test() = default;
  ~RecoverDataCallback4Test() = default;

  void OnRecoverData(const std::string& key,
                     CacheBufferSharedPtr buffer) override {
    last_recover_key_ = key;
    recovered_record_cnt_++;
  }

  std::string GetLastRecoverKey() { return last_recover_key_; }
  int64_t GetRecoveredRecordCnt() { return recovered_record_cnt_.load(); }

 private:
  std::string last_recover_key_{""};
  std::atomic<int64_t> recovered_record_cnt_{0};
};

class MultiSSDTest : public ::testing::Test {
 protected:
  std::string make_path() {
    char path[64] = "/tmp/mtcache_storage_ssd_test_XXXXXX";
    if (mkdtemp(path) == nullptr) {
      LOG(FATAL) << "create tmp_dir failed, errno=" << errno;
    }
    return std::string(path);
  }

  void SetUp() override {
    for (int i = 0; i < DISK_NUM; ++i) {
      // auto path = make_path();
      // ZonedStore run on dev or file, While TerarkDB run on path
      auto path = make_path() + "/file_based_dev" + std::to_string(i);
      ssd_paths_.push_back(path);
    }
  }

  void TearDown() override {
    for (auto path : ssd_paths_) {
      auto p = std::filesystem::path(path);
      std::filesystem::remove_all(p.parent_path());
    }
  }

  std::vector<std::string> ssd_paths_;
};

TEST_F(MultiSSDTest, PutGetPeekDeleteReset) {
  auto registry = noodle::GetMetricRegistry("ti.mtcache.multi_ssd_test");
  std::unique_ptr<StorageEngine> engine =
      std::make_unique<StorageEngineMultiSSD>(ssd_paths_, CACHE_CAPACITY,
                                              registry);
  ASSERT_TRUE(engine->Start());

  for (int i = 0; i < 10; ++i) {
    // Put method test
    std::string key = "key" + std::to_string(i);
    std::string data = "StorageEngineMultiSSD";
    std::unique_ptr<folly::IOBuf> value = folly::IOBuf::copyBuffer(data);
    auto put_res = engine->Put(key, std::move(*value));
    EXPECT_TRUE(put_res.IsOk());
    auto buffer = std::move(put_res).Get();
    auto string_view_buffer = dynamic_cast<StringViewBufferPtr>(buffer.get());
    EXPECT_NE(nullptr, string_view_buffer);
    EXPECT_EQ(data.size(), buffer->Size());

    EXPECT_TRUE(engine->Peek(key));
    EXPECT_FALSE(engine->Peek("not_exist"));

    // Get method test
    auto get_res = engine->Get(key);
    EXPECT_TRUE(get_res.IsOk());
    buffer = std::move(get_res).Get();
    auto string_buffer = dynamic_cast<StringBufferPtr>(buffer.get());
    EXPECT_NE(nullptr, string_buffer);
    EXPECT_EQ(buffer->Size(), data.size());
    EXPECT_STREQ(buffer->Data(), data.data());

    buffer.reset();
    EXPECT_TRUE(engine->Delete(key).IsOk());
    // ZonedStore use softdel so we don't need this
    // EXPECT_FALSE(engine->Peek(key));
  }

  EXPECT_TRUE(engine->Stop());
  noodle::GetGlobalMetricRegistry()->Deregister("ti.mtcache.multi_ssd_test");
}

// ZonedStore Recovery Test is enough
TEST_F(MultiSSDTest, DISABLED_Recover) {
  FLAGS_cache_ssd_data_recovery_in_background = false;
  auto registry = noodle::GetMetricRegistry("ti.mtcache.multi_ssd_test");
  std::unique_ptr<StorageEngine> engine =
      std::make_unique<StorageEngineMultiSSD>(ssd_paths_, CACHE_CAPACITY,
                                              registry);
  ASSERT_TRUE(engine->Start());

  for (int i = 0; i < 10; ++i) {
    // Put method test
    std::string key = "key" + std::to_string(i);
    std::string data = "StorageEngineMultiSSD";
    std::unique_ptr<folly::IOBuf> value = folly::IOBuf::copyBuffer(data);
    auto put_res = engine->Put(key, std::move(*value));
    EXPECT_TRUE(put_res.IsOk());
    auto buffer = std::move(put_res).Get();
    auto string_view_buffer = dynamic_cast<StringViewBufferPtr>(buffer.get());
    EXPECT_NE(nullptr, string_view_buffer);
    EXPECT_EQ(data.size(), buffer->Size());
    EXPECT_DEATH(buffer->Data(), "");

    EXPECT_TRUE(engine->Peek(key));
    EXPECT_FALSE(engine->Peek("not_exist"));
  }

  RecoverDataCallback4Test callback;
  auto recover_res = engine->RecoverData(&callback);
  EXPECT_TRUE(recover_res.IsOk());

  // not sure which key in the last device
  //  EXPECT_EQ(callback.GetLastRecoverKey(), "key10");
  EXPECT_EQ(callback.GetRecoveredRecordCnt(), 10);

  for (int i = 0; i < 10; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string data = "StorageEngineMultiSSD";

    // Get method test
    auto get_res = engine->Get(key);
    EXPECT_TRUE(get_res.IsOk());
    auto buffer = std::move(get_res).Get();
    auto string_buffer = dynamic_cast<StringBufferPtr>(buffer.get());
    EXPECT_NE(nullptr, string_buffer);
    EXPECT_EQ(buffer->Size(), data.size());
    EXPECT_STREQ(buffer->Data(), data.data());

    buffer.reset();
    EXPECT_TRUE(engine->Delete(key).IsOk());
    EXPECT_FALSE(engine->Peek(key));
  }

  EXPECT_TRUE(engine->Stop());
  noodle::GetGlobalMetricRegistry()->Deregister("ti.mtcache.multi_ssd_test");
}

TEST_F(MultiSSDTest, StartStop) {
  auto registry = noodle::GetMetricRegistry("ti.mtcache.multi_ssd_test");
  std::unique_ptr<StorageEngine> engine =
      std::make_unique<StorageEngineMultiSSD>(ssd_paths_, CACHE_CAPACITY,
                                              registry);
  //  std::unique_ptr<StorageEngineSSD> engine =
  //      std::make_unique<StorageEngineTerarkDB>(ssd_paths_[0]);
  ASSERT_TRUE(engine->Start());
  EXPECT_TRUE(engine->Stop());
  noodle::GetGlobalMetricRegistry()->Deregister("ti.mtcache.multi_ssd_test");
}

}  // namespace mtcache
