#include "common/numa_utils.h"
#include "storage/pmem_storage.h"
#include "test/storage/gc_copy_callback_mock.h"
#include "test/storage/recover_callback_mock.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <stdlib.h>

DECLARE_int32(used_num_numa_nodes);

namespace mtcache {

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

class PmemRecoverTest : public ::testing::Test {
 public:
  static void SetUpTestCase() { NumaInfo::Init(); }

  static void TearDownTestCase() { CacheExecutor::DestroyAllExecutors(); }

  void SetUp() {
    char path0[64] = "/tmp/mtcache_pmem_storage_recover_test0_XXXXXX";
    char path1[64] = "/tmp/mtcache_pmem_storage_recover_test1_XXXXXX";
    if (mkdtemp(path0) == nullptr || mkdtemp(path1) == nullptr) {
      LOG(FATAL) << "create tmp_dir failed, errno=" << errno;
    }
    pmem_path0_ = std::string(path0);
    pmem_path1_ = std::string(path1);
  }

  void TearDown() {
    std::filesystem::remove_all(pmem_path0_);
    std::filesystem::remove_all(pmem_path1_);
  }

  CacheBufferSharedPtr Put(StorageEnginePMem* engine, const std::string& key,
                           const std::string& value) {
    auto put_res = engine->Put(
        key, folly::IOBuf::wrapBufferAsValue(value.data(), value.size()));
    CHECK(put_res.IsOk());
    return std::move(put_res).Get();
  }

  CacheBufferSharedPtr PutNuma(StorageEnginePMem* engine,
                               const std::string& key, const std::string& value,
                               int32_t numa_id) {
    auto put_res = engine->TEST_PutToNuma(
        key, folly::IOBuf::wrapBuffer(value.data(), value.size()), numa_id);
    CHECK(put_res.IsOk());
    return std::move(put_res).Get();
  }

  // Memory layout for PMEM data in the cache:
  //
  //
  //                      payload
  //                        |
  //       +---------------||--------------------+
  //       |                                     |
  //       value size(4B)
  //         |
  //         |  key size(4B)
  //         |    |                    key
  //         |    |     value          |
  //         |    |     |              |
  //         v    v     v              v
  //  +------------------------------------------+----+
  //  |    |    |    |              |            |    |
  //  +------------------------------------------+----+
  //    ^                                          ^
  //    |                                          |
  //   payload_size(4B)                         CRC(4B)
  //
  //

  // Make the body of thid cache record corrputed.
  void CorruptPayload(const CacheBufferSharedPtr& buf) {
    char* val_ptr = const_cast<char*>(buf->Data());
    ASSERT_NE(val_ptr, nullptr);
    uint8_t corrupt_v;
    memcpy(&corrupt_v, val_ptr, sizeof(corrupt_v));
    corrupt_v = ~corrupt_v;
    memcpy(val_ptr, &corrupt_v, sizeof(corrupt_v));
  }

  // Make the head of thid cache record corrputed. The 'head' means the leading
  // uint32_t storing the length of this cache payload.
  void CorruptHead(const CacheBufferSharedPtr& buf) {
    char* head_ptr = const_cast<char*>(buf->Data()) - sizeof(uint32_t) * 3;
    ASSERT_NE(head_ptr, nullptr);
    uint32_t corrupt_v = kLogChunkSize + 1;
    memcpy(head_ptr, &corrupt_v, sizeof(corrupt_v));
  }

  // Set the head of this cache record as zero
  void ZeroHead(const CacheBufferSharedPtr& buf) {
    char* head_ptr = const_cast<char*>(buf->Data()) - sizeof(uint32_t) * 3;
    ASSERT_NE(head_ptr, nullptr);
    uint32_t zero_v = 0;  // kChunkStopMark == 0
    memcpy(head_ptr, &zero_v, sizeof(zero_v));
  }

 protected:
  uint64_t pmem_capacity_ = 6 * kLogChunkSize;  // 3 chunks per NUMA
  std::string pmem_path0_;
  std::string pmem_path1_;
};

TEST_F(PmemRecoverTest, SingleNumaSingleChunkNodup) {
  auto registry = noodle::GetMetricRegistry("ti.mtcache.pmem_recover_test");
  FLAGS_used_num_numa_nodes = 1;
  auto gccb = std::make_unique<GCCopyCallbackMock>();
  auto recover_cb = std::make_unique<RecoverDataCallbackMock>();
  auto* engine = new StorageEnginePMem(pmem_capacity_, {pmem_path0_},
                                       gccb.get(), registry);
  ASSERT_TRUE(engine->Start());

  std::string value(512, '1');
  std::vector<std::string> keys = {"key0", "key1", "key2",
                                   "key3", "key4", "key5"};
  auto buf0 = Put(engine, keys[0], value);
  auto buf1 = Put(engine, keys[1], value);
  auto buf2 = Put(engine, keys[2], value);
  auto buf3 = Put(engine, keys[3], value);
  auto buf4 = Put(engine, keys[4], value);
  auto buf5 = Put(engine, keys[5], value);
  buf1.reset();          // free buf1
  CorruptPayload(buf2);  // corrupt buf2
  ZeroHead(buf4);        // Write buf4 head to 0 so that buf5 can not be seen

  ASSERT_TRUE(engine->Stop());
  // memroy held by buf0,2,3,4,5 will not release because engine has stopped
  buf0.reset();
  buf2.reset();
  buf3.reset();
  buf4.reset();
  buf5.reset();

  delete engine;
  registry.reset();
  noodle::GetGlobalMetricRegistry()->Deregister("ti.mtcache.pmem_recover_test");

  // Restart engine and recover.

  registry = noodle::GetMetricRegistry("ti.mtcache.pmem_recover_test");
  engine = new StorageEnginePMem(pmem_capacity_, {pmem_path0_}, gccb.get(),
                                 registry);
  ASSERT_TRUE(engine->Start());

  auto recover_res = engine->RecoverData(recover_cb.get());
  ASSERT_TRUE(recover_res.IsOk());
  auto recover_stats = engine->TEST_GetRecoverStats();

  EXPECT_EQ(recover_stats.total_bytes_, kLogChunkSize);  // Only 1 chunk is used
  constexpr size_t kHeadLen = 4 + 4 + 4;
  constexpr size_t kTailLen = 4;
  size_t record_sz = value.size() + keys[0].size() + kHeadLen + kTailLen;
  EXPECT_EQ(recover_stats.valid_bytes_, 2 * record_sz);
  EXPECT_EQ(recover_stats.freed_bytes_, kLogChunkSize - 3 * record_sz);
  EXPECT_EQ(recover_stats.corrupted_bytes_, record_sz);
  EXPECT_EQ(recover_cb->GetRecoveredRecordCnt(), 2);

  ASSERT_TRUE(engine->Stop());
  delete engine;
  registry.reset();
  noodle::GetGlobalMetricRegistry()->Deregister("ti.mtcache.pmem_recover_test");
}

TEST_F(PmemRecoverTest, SingleNumaMultiChunkDupRecord) {
  auto registry = noodle::GetMetricRegistry("ti.mtcache.pmem_recover_test");
  FLAGS_used_num_numa_nodes = 1;
  auto gccb = std::make_unique<GCCopyCallbackMock>();
  auto recover_cb = std::make_unique<RecoverDataCallbackMock>();
  auto* engine = new StorageEnginePMem(pmem_capacity_, {pmem_path0_},
                                       gccb.get(), registry);
  ASSERT_TRUE(engine->Start());

  std::string value(kLogChunkSize / 4, '1');
  std::vector<std::string> keys = {"key0", "key1", "key2", "key3", "key4"};
  // buf0, buf1, buf2 on Chunk0
  auto buf0 = Put(engine, keys[0], value);
  auto buf1 = Put(engine, keys[1], value);
  auto buf2 = Put(engine, keys[0], value);  // Duplicated buf0 and buf2

  // buf3, buf4, buf5 on Chunk1
  auto buf3 = Put(engine, keys[0], value);
  auto buf4 = Put(engine, keys[3], value);
  auto buf5 = Put(engine, keys[4], value);

  // Make buf4 head corrupted so that buf5 and following space all corrupted
  CorruptHead(buf4);

  ASSERT_TRUE(engine->Stop());
  // memroy held by buf0,1,2,3,4 will not release because engine has stopped
  buf0.reset();
  buf1.reset();
  buf2.reset();
  buf3.reset();
  buf4.reset();
  buf5.reset();

  delete engine;
  registry.reset();
  noodle::GetGlobalMetricRegistry()->Deregister("ti.mtcache.pmem_recover_test");

  // Restart engine and recover.

  registry = noodle::GetMetricRegistry("ti.mtcache.pmem_recover_test");
  engine = new StorageEnginePMem(pmem_capacity_, {pmem_path0_}, gccb.get(),
                                 registry);
  ASSERT_TRUE(engine->Start());

  auto recover_res = engine->RecoverData(recover_cb.get());
  ASSERT_TRUE(recover_res.IsOk());
  auto recover_stats = engine->TEST_GetRecoverStats();

  EXPECT_EQ(recover_stats.total_bytes_, kLogChunkSize * 2);
  constexpr size_t kHeadLen = 4 + 4 + 4;
  constexpr size_t kTailLen = 4;
  size_t record_sz = value.size() + keys[0].size() + kHeadLen + kTailLen;
  EXPECT_EQ(recover_stats.valid_bytes_, 4 * record_sz);
  EXPECT_EQ(recover_stats.freed_bytes_, kLogChunkSize - 3 * record_sz);
  EXPECT_EQ(recover_stats.corrupted_bytes_, kLogChunkSize - record_sz);
  // buf0, buf2, buf3 are duplicated so they are all thought as invalid.
  EXPECT_EQ(recover_cb->GetRecoveredRecordCnt(), 1);

  ASSERT_TRUE(engine->Stop());
  delete engine;
  registry.reset();
  noodle::GetGlobalMetricRegistry()->Deregister("ti.mtcache.pmem_recover_test");
}

TEST_F(PmemRecoverTest, MultiNumaMultiChunkDupRecord) {
  // On CI env, we may have NUMA nodes that do not own any cores, therefore,
  // we do not run this test if the first two NUMA nodes are not valid.
  if (NumaInfo::GetMaxNumNumaNodes() < 2 ||
      NumaInfo::GetCpuCoresOfNumaNode(0).empty() ||
      NumaInfo::GetCpuCoresOfNumaNode(1).empty()) {
    LOG(WARNING) << "There are less than 2 NUMA nodes. Skip this UT";
    return;
  }
  auto registry = noodle::GetMetricRegistry("ti.mtcache.pmem_recover_test");
  // Destroy executors here otherwise the pmem_executors has inited with
  // FLAGS_used_num_numa_nodes = 1 above.
  CacheExecutor::DestroyAllExecutors();
  FLAGS_used_num_numa_nodes = 2;
  auto gccb = std::make_unique<GCCopyCallbackMock>();
  auto recover_cb = std::make_unique<RecoverDataCallbackMock>();
  auto* engine = new StorageEnginePMem(
      pmem_capacity_, {pmem_path0_, pmem_path1_}, gccb.get(), registry);
  ASSERT_TRUE(engine->Start());

  std::string value(kLogChunkSize / 4, '1');
  std::vector<std::string> keys = {"key0", "key1", "key2", "key3", "key4"};
  // buf0, buf1, buf2 on Chunk0 of NUMA0
  auto buf0 = PutNuma(engine, keys[0], value, 0);
  auto buf1 = PutNuma(engine, keys[1], value, 0);
  auto buf2 = PutNuma(engine, keys[2], value, 0);

  // buf3, buf4, buf5 on Chunk0 of NUMA1
  auto buf3 = PutNuma(engine, keys[0], value, 1);
  auto buf4 = PutNuma(engine, keys[3], value, 1);
  auto buf5 = PutNuma(engine, keys[4], value, 1);

  // Make buf4 head corrupted so that buf5 and following space all corrupted
  CorruptHead(buf4);

  ASSERT_TRUE(engine->Stop());
  // memroy held by buf0,1,2,3,4 will not release because engine has stopped
  buf0.reset();
  buf1.reset();
  buf2.reset();
  buf3.reset();
  buf4.reset();
  buf5.reset();

  delete engine;
  registry.reset();
  noodle::GetGlobalMetricRegistry()->Deregister("ti.mtcache.pmem_recover_test");

  // Restart engine and recover.

  registry = noodle::GetMetricRegistry("ti.mtcache.pmem_recover_test");
  engine = new StorageEnginePMem(pmem_capacity_, {pmem_path0_, pmem_path1_},
                                 gccb.get(), registry);
  ASSERT_TRUE(engine->Start());

  auto recover_res = engine->RecoverData(recover_cb.get());
  ASSERT_TRUE(recover_res.IsOk());
  auto recover_stats = engine->TEST_GetRecoverStats();

  EXPECT_EQ(recover_stats.total_bytes_, kLogChunkSize * 2);
  constexpr size_t kHeadLen = 4 + 4 + 4;
  constexpr size_t kTailLen = 4;
  size_t record_sz = value.size() + keys[0].size() + kHeadLen + kTailLen;
  EXPECT_EQ(recover_stats.valid_bytes_, 4 * record_sz);
  EXPECT_EQ(recover_stats.freed_bytes_, kLogChunkSize - 3 * record_sz);
  EXPECT_EQ(recover_stats.corrupted_bytes_, kLogChunkSize - record_sz);
  // buf0, buf3 are duplicated so they are all thought as invalid.
  EXPECT_EQ(recover_cb->GetRecoveredRecordCnt(), 2);

  ASSERT_TRUE(engine->Stop());
  delete engine;
  registry.reset();
  noodle::GetGlobalMetricRegistry()->Deregister("ti.mtcache.pmem_recover_test");
}

#pragma GCC diagnostic pop

}  // namespace mtcache
