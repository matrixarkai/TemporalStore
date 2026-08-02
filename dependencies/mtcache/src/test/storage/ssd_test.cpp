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

class RecoverDataCallback4Test : public StorageEngine::RecoverDataCallback {
 public:
  RecoverDataCallback4Test() = default;
  ~RecoverDataCallback4Test() = default;

  void OnRecoverData(const std::string& key,
                     CacheBufferSharedPtr buffer) override {
    LOG(INFO) << "callback key=" << key;
    last_recover_key_ = key;
    recovered_record_cnt_++;
  }

  std::string GetLastRecoverKey() { return last_recover_key_; }
  int64_t GetRecoveredRecordCnt() { return recovered_record_cnt_.load(); }

 private:
  std::string last_recover_key_{""};
  std::atomic<int64_t> recovered_record_cnt_{0};
} __attribute__((aligned));

class StorageEngineSSDTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char path[64] = "/tmp/mtcache_storage_ssd_test_XXXXXX";
    if (mkdtemp(path) == nullptr) {
      LOG(FATAL) << "create tmp_dir failed, errno=" << errno;
    }
    ssd_path_ = std::string(path);
  }

  void TearDown() override { std::filesystem::remove_all(ssd_path_); }

  std::string ssd_path_;

  RecoverDataCallback4Test callback_;
};

TEST_F(StorageEngineSSDTest, PutGetPeekDeleteResetZone) {
  // currently, StorageEngineZoneStore is not fully implemented.
  // the test has no meaning.
  // StorageEngineZonedStore* engine = new StorageEngineZonedStore(ssd_path_);
  // ASSERT_TRUE(engine->Start());
  // Put method test
  // std::string data = "StorageEngineSSD";
  // std::unique_ptr<folly::IOBuf> value = folly::IOBuf::copyBuffer(data);
  // auto put_res = engine->Put("key1", std::move(value));
  // EXPECT_TRUE(put_res.IsOk());
  // auto buffer = std::move(put_res).Get();
  // auto string_view_buffer = dynamic_cast<StringViewBufferPtr>(buffer.get());
  // EXPECT_NE(nullptr, string_view_buffer);
  // EXPECT_EQ(data.size(), buffer->Size());
  // EXPECT_DEATH(buffer->Data(), "");

  // EXPECT_TRUE(engine->Peek("key1"));
  // EXPECT_FALSE(engine->Peek("not_exist"));

  // // Get method test
  // auto get_res = engine->Get("key1");
  // EXPECT_TRUE(get_res.IsOk());
  // buffer = std::move(get_res).Get();
  // auto string_buffer = dynamic_cast<StringBufferPtr>(buffer.get());
  // EXPECT_NE(nullptr, string_buffer);
  // EXPECT_EQ(buffer->Size(), data.size());
  // EXPECT_STREQ(buffer->Data(), data.data());

  // buffer.reset();
  // EXPECT_TRUE(engine->Delete("key1").IsOk());
  // EXPECT_FALSE(engine->Peek("key1"));

  // EXPECT_TRUE(engine->Reset().IsOk());
  // EXPECT_TRUE(engine->Stop());
  // delete engine;
}

TEST_F(StorageEngineSSDTest, PutGetPeekDeleteReset) {
  auto registry = noodle::GetMetricRegistry("ti.mtcache.ssd_storage_test");
  StorageEngineTerarkDB* engine =
      new StorageEngineTerarkDB(ssd_path_, registry);
  ASSERT_TRUE(engine->Start());
  // Put method test
  std::string data = "StorageEngineSSD";
  std::unique_ptr<folly::IOBuf> value = folly::IOBuf::copyBuffer(data);
  auto put_res = engine->Put("key1", std::move(*value));
  EXPECT_TRUE(put_res.IsOk());
  auto buffer = std::move(put_res).Get();
  auto string_view_buffer = dynamic_cast<StringViewBufferPtr>(buffer.get());
  EXPECT_NE(nullptr, string_view_buffer);
  EXPECT_EQ(data.size(), buffer->Size());

  EXPECT_TRUE(engine->Peek("key1"));
  EXPECT_FALSE(engine->Peek("not_exist"));

  // Get method test
  auto get_res = engine->Get("key1");
  EXPECT_TRUE(get_res.IsOk());
  buffer = std::move(get_res).Get();
  auto string_buffer = dynamic_cast<StringBufferPtr>(buffer.get());
  EXPECT_NE(nullptr, string_buffer);
  EXPECT_EQ(buffer->Size(), data.size());
  EXPECT_STREQ(buffer->Data(), data.data());

  buffer.reset();
  EXPECT_TRUE(engine->Delete("key1").IsOk());
  EXPECT_FALSE(engine->Peek("key1"));

  EXPECT_TRUE(engine->Reset().IsOk());
  EXPECT_TRUE(engine->Stop());
  delete engine;
  noodle::GetGlobalMetricRegistry()->Deregister("ti.mtcache.ssd_storage_test");
}

TEST_F(StorageEngineSSDTest, PutGetPeekRecoverGet) {
  FLAGS_cache_ssd_data_recovery_in_background = false;
  auto registry = noodle::GetMetricRegistry("ti.mtcache.ssd_storage_test");
  StorageEngineTerarkDB* engine =
      new StorageEngineTerarkDB(ssd_path_, registry);
  ASSERT_TRUE(engine->Start());
  // Put method test
  std::string data = "StorageEngineSSD";
  std::unique_ptr<folly::IOBuf> value = folly::IOBuf::copyBuffer(data);
  auto put_res = engine->Put("key1", std::move(*value));
  EXPECT_TRUE(put_res.IsOk());
  auto buffer = std::move(put_res).Get();
  auto string_view_buffer = dynamic_cast<StringViewBufferPtr>(buffer.get());
  EXPECT_NE(nullptr, string_view_buffer);
  EXPECT_EQ(data.size(), buffer->Size());
  EXPECT_DEATH(buffer->Data(), "");

  EXPECT_TRUE(engine->Peek("key1"));
  EXPECT_FALSE(engine->Peek("not_exist"));

  // Get method test
  auto get_res = engine->Get("key1");
  EXPECT_TRUE(get_res.IsOk());
  buffer = std::move(get_res).Get();
  auto string_buffer = dynamic_cast<StringBufferPtr>(buffer.get());
  EXPECT_NE(nullptr, string_buffer);
  EXPECT_EQ(buffer->Size(), data.size());
  EXPECT_STREQ(buffer->Data(), data.data());

  // Stop
  EXPECT_TRUE(engine->Stop());
  delete engine;
  noodle::GetGlobalMetricRegistry()->Deregister("ti.mtcache.ssd_storage_test");

  // RecoverData
  RecoverDataCallback4Test callback_;
  registry = noodle::GetMetricRegistry("ti.mtcache.ssd_storage_test");
  engine = new StorageEngineTerarkDB(ssd_path_, registry);
  ASSERT_TRUE(engine->Start());
  LOG(INFO) << "callback address=" << reinterpret_cast<uint64_t>(&callback_);

  auto recover_res = engine->RecoverData(&callback_);
  EXPECT_TRUE(recover_res.IsOk());
  EXPECT_EQ(callback_.GetLastRecoverKey(), "key1");
  EXPECT_EQ(callback_.GetRecoveredRecordCnt(), 1);

  // re-Get
  get_res = engine->Get("key1");
  EXPECT_TRUE(get_res.IsOk());
  buffer = std::move(get_res).Get();
  string_buffer = dynamic_cast<StringBufferPtr>(buffer.get());
  EXPECT_NE(nullptr, string_buffer);
  EXPECT_EQ(buffer->Size(), data.size());
  EXPECT_STREQ(buffer->Data(), data.data());

  // Delete
  buffer.reset();
  EXPECT_TRUE(engine->Delete("key1").IsOk());
  EXPECT_FALSE(engine->Peek("key1"));

  EXPECT_TRUE(engine->Reset().IsOk());
  EXPECT_TRUE(engine->Stop());
  delete engine;
  noodle::GetGlobalMetricRegistry()->Deregister("ti.mtcache.ssd_storage_test");
}

TEST_F(StorageEngineSSDTest, PutGetPeekRecoverBgGet) {
  FLAGS_cache_ssd_data_recovery_in_background = true;
  auto registry = noodle::GetMetricRegistry("ti.mtcache.ssd_storage_test");
  StorageEngineTerarkDB* engine =
      new StorageEngineTerarkDB(ssd_path_, registry);
  ASSERT_TRUE(engine->Start());
  // Put method test
  std::string data = "StorageEngineSSD";
  std::unique_ptr<folly::IOBuf> value = folly::IOBuf::copyBuffer(data);
  auto put_res = engine->Put("key1", std::move(*value));
  EXPECT_TRUE(put_res.IsOk());
  auto buffer = std::move(put_res).Get();
  auto string_view_buffer = dynamic_cast<StringViewBufferPtr>(buffer.get());
  EXPECT_NE(nullptr, string_view_buffer);
  EXPECT_EQ(data.size(), buffer->Size());
  EXPECT_DEATH(buffer->Data(), "");

  EXPECT_TRUE(engine->Peek("key1"));
  EXPECT_FALSE(engine->Peek("not_exist"));

  // Get method test
  auto get_res = engine->Get("key1");
  EXPECT_TRUE(get_res.IsOk());
  buffer = std::move(get_res).Get();
  auto string_buffer = dynamic_cast<StringBufferPtr>(buffer.get());
  EXPECT_NE(nullptr, string_buffer);
  EXPECT_EQ(buffer->Size(), data.size());
  EXPECT_STREQ(buffer->Data(), data.data());

  // Stop
  EXPECT_TRUE(engine->Stop());
  delete engine;
  noodle::GetGlobalMetricRegistry()->Deregister("ti.mtcache.ssd_storage_test");

  // RecoverData
  LOG(INFO) << "Restart Engine";
  registry = noodle::GetMetricRegistry("ti.mtcache.ssd_storage_test");
  engine = new StorageEngineTerarkDB(ssd_path_, registry);
  ASSERT_TRUE(engine->Start());
  LOG(INFO) << "callback address=" << reinterpret_cast<uint64_t>(&callback_);

  auto recover_res = engine->RecoverData(&callback_);
  EXPECT_TRUE(recover_res.IsOk());
  LOG(INFO) << "usleep(100 * 1000)";
  usleep(100 * 1000);
  LOG(INFO) << "Check Recover";
  EXPECT_TRUE(engine->IsDataRecovered());

  EXPECT_EQ(callback_.GetLastRecoverKey(), "key1");
  EXPECT_EQ(callback_.GetRecoveredRecordCnt(), 1);
  auto recover_records_counter_ =
      noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
          noodle::MetricId("ti.mtcache.ssd_storage_test." + ssd_path_ +
                           "_ssd_terarkdb_recover_records"));
  EXPECT_EQ(recover_records_counter_->GetValue(), 1);

  // re-Get
  get_res = engine->Get("key1");
  EXPECT_TRUE(get_res.IsOk());
  buffer = std::move(get_res).Get();
  string_buffer = dynamic_cast<StringBufferPtr>(buffer.get());
  EXPECT_NE(nullptr, string_buffer);
  EXPECT_EQ(buffer->Size(), data.size());
  EXPECT_STREQ(buffer->Data(), data.data());

  // Delete
  buffer.reset();
  EXPECT_TRUE(engine->Delete("key1").IsOk());
  EXPECT_FALSE(engine->Peek("key1"));

  EXPECT_TRUE(engine->Reset().IsOk());
  EXPECT_TRUE(engine->Stop());
  delete engine;
  noodle::GetGlobalMetricRegistry()->Deregister("ti.mtcache.ssd_storage_test");
}

}  // namespace mtcache
