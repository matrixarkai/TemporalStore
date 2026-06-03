#include "storage/zoned_store/buffer_manager.h"

#include "common/logging.h"
#include "noodle/test_util/sync_point.h"
#include "storage/zoned_store/codec.h"
#include "storage/zoned_store/index.h"
#include "storage/zoned_store/zone_manager.h"
#include "storage/zoned_store/zoned_store.h"

#include <folly/io/IOBuf.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <variant>
#include <vector>
namespace mtcache {

class MockIndex : public Index {
 public:
  bool UpdateIndex(const std::string& key, ValueType value) override {
    key_colored_ptr_vec.emplace_back(key,
                                     std::get<Index::SSDColoredPtr>(value));
    trigger_time_++;
    return true;
  }

  int trigger_time_ = 0;
  std::vector<std::pair<std::string, uint64_t>> key_colored_ptr_vec;
};

class MockZoneManager : public ZoneManager {
 public:
  MockZoneManager() : ZoneManager({}) {}
  ~MockZoneManager() {}

  uint64_t total_data_written_time_ = 0;
  uint64_t total_oplog_written_time_ = 0;
  int data_append_time_ = 0;
  int finish_group_time_ = 0;
  int zone_size_ = 25 << 10;

  std::deque<int> ensure_seqs_ = {};

  // Append buffer to the disk and set the written LBA offset to `offset`.
  // Return 0 if success.
  int Append(const char* buf, int sz, DataType type,
             uint64_t* offset) override {
    data_append_time_++;
    if (type == DataType::DATA) {
      total_data_written_time_++;
    } else {
      total_oplog_written_time_++;
    }
    return 0;
  }

  // Read data into buf. Note that the passed-in `buf` should be 4KB aligned and
  // pre-allocated, Return 0 if success
  virtual int Read(char* buf, uint64_t offset, int sz) override { return 1; }

  // Check if current group has enough space for both data & related meta.
  // Note that even the caller ensured the capacity, it may not append data
  // immedately, so we should tracke the claimed space here.
  // Return 1 if success & ready for data writing.
  // Return 0 if no enough space in current group.
  int EnsureAvailableSpace(int data_size, int meta_size) override {
    if (ensure_seqs_.size()) {
      int result = ensure_seqs_.front();
      ensure_seqs_.pop_front();
      return result;
    } else {
      return 1;
    }
  }

  // Finish current group and open a new group.
  // Return 0 if success, and -1 otherwise.
  int FinishGroup() override {
    finish_group_time_++;
    return 0;
  }

  // Locate the Zone with 'offset' and subtract its valid bytes with 'size'.
  void TrimBytes(uint64_t offset, int size) override { return; }

  // Return a group that has largest garbage ratio.
  // If there is no group in gc_list, return -1.
  std::pair<int16_t, GCMode> FindGCGroup() override { return {-1, LOSSY}; }

  // Reset the specified group
  // Return 0 if success.
  int ResetGroup(uint16_t group_id) override { return 1; }

  // Load the all zone metadata in the specified group into 'buf'.
  int LoadMetaData(int group_id,
                   GCWorker::LoadMetaCallback meta_callback) override {
    return 1;
  }

  bool GetProperty(std::string property, std::string* result) override {
    return true;
  }

  void Recovery(std::function<int(const char* buf)> meta_cb) override {
    return;
  }

  ZoneMode GetZoneMode() const override { return ZoneMode::LARGE; }

  uint64_t GetZoneCapacity() const override { return zone_size_; }

  uint64_t GetUsedSpace() override { return 0; }
};

class BufferManagerTest : public testing::Test {
 protected:
  std::shared_ptr<BufferManager> bm_;
  std::shared_ptr<MockZoneManager> zone_manager_;

  std::shared_ptr<BufferEncoder> encoder_;

  std::shared_ptr<MockIndex> index_;

  uint32_t gc_bufs_;
  uint32_t user_bufs_;
  double flush_threshold_;
  uint32_t cap_per_buf_;

  char key_base_ = 'k';
  char value_base_ = 'v';

  void SetUp() override {
    zone_manager_ = std::make_shared<MockZoneManager>();
    index_ = std::make_shared<MockIndex>();
    encoder_ = std::make_shared<BufferEncoder>(2 * cap_per_buf_);
  }

  void CreateBM(int user_bufs = 1, int cap_per_buf = (1 << 10), int gc_bufs = 1,
                double flush_threshold = 0.5) {
    user_bufs_ = user_bufs;
    gc_bufs_ = gc_bufs;
    flush_threshold_ = flush_threshold;
    cap_per_buf_ = cap_per_buf;
    auto index_updater = std::make_shared<IndexUpdater>(index_);
    bm_ = std::make_shared<BufferManager>(user_bufs, gc_bufs, zone_manager_,
                                          encoder_, std::move(index_updater),
                                          cap_per_buf, flush_threshold);
    bm_->Start();
  }

  void TearDown() override { bm_->Stop(); }
};

// buf_size = 2KB, flush threshold = 1KB.
// Flush Process should not be triggered if
// written size < 1KB.
TEST_F(BufferManagerTest, FlushBufferTestOne) {
  NOODLE_INIT_SYNC_POINT_SINGLETONS()->LoadDependency(
      {{"BufferManager::CodecBuffers_DONE", "FlushBufferTestOne_2"},
       {"FlushBufferTestOne_1", "BufferManager::CodecBuffers_BEGIN"}});
  NOODLE_INIT_SYNC_POINT_SINGLETONS()->EnableProcessing();

  int user_bufs = 1;
  int cap_per_buf = 2 << 10;  // 2KB
  CreateBM(user_bufs, cap_per_buf);
#if DCHECK_IS_ON()
  int key_size = 100;
  int value_size = 300;
  auto iobuf = ::folly::IOBuf::createCombined(value_size);
  std::memset(iobuf->writableData(), value_base_, value_size);
  iobuf->append(value_size);

  index_->Put(std::string(key_size, key_base_),
              std::make_pair(nullptr, Index::kSoftDel));
  bm_->Put({std::string(key_size, key_base_), std::move(iobuf)},
           WriteBufferType::kUserDataBuf);
  NOODLE_TEST_SYNC_POINT("FlushBufferTestOne_1");
  NOODLE_TEST_SYNC_POINT("FlushBufferTestOne_2");
  EXPECT_EQ(0, zone_manager_->data_append_time_);
  auto& buffer_pools = bm_->get_buffer_pools();
  auto& flush_queue = bm_->get_flush_queue();
  auto& buf = buffer_pools[0];
  EXPECT_TRUE(flush_queue.isEmpty());
  EXPECT_EQ(1, buf->count());
  EXPECT_EQ(key_size, buf->key_size());
  EXPECT_EQ(value_size, buf->value_size());

  NOODLE_INIT_SYNC_POINT_SINGLETONS()->DisableProcessing();
#endif
}

// buf_size = 8KB, flush threshold = 4KB.
// When 5KB data is written, flush process must
// be triggered.
TEST_F(BufferManagerTest, FlushBufferTestTwo) {
  NOODLE_INIT_SYNC_POINT_SINGLETONS()->LoadDependency(
      {{"BufferManager::FlushBuffer_END", "FlushBufferTestTwo"}});
  NOODLE_INIT_SYNC_POINT_SINGLETONS()->EnableProcessing();

  int user_bufs = 1;
  int cap_per_buf = 8 << 10;  // 2KB
  CreateBM(user_bufs, cap_per_buf);
#if DCHECK_IS_ON()
  int key_size = 1000;
  int value_size = 4000;
  auto iobuf = ::folly::IOBuf::createCombined(value_size);
  std::memset(iobuf->writableData(), value_base_, value_size);
  iobuf->append(value_size);

  index_->Put(std::string(key_size, key_base_),
              std::make_pair(nullptr, Index::kSoftDel));
  bm_->Put({std::string(key_size, key_base_), std::move(iobuf)},
           WriteBufferType::kUserDataBuf);
  NOODLE_TEST_SYNC_POINT("FlushBufferTestTwo");
  EXPECT_EQ(1, zone_manager_->data_append_time_);
  auto& buffer_pools = bm_->get_buffer_pools();
  auto& data_buf = buffer_pools[0];
  auto& log_buf = buffer_pools[2];
  EXPECT_EQ(0, data_buf->count());
  EXPECT_EQ(1, zone_manager_->total_data_written_time_);
  EXPECT_EQ(1, log_buf->count());

  NOODLE_INIT_SYNC_POINT_SINGLETONS()->DisableProcessing();
#endif
}

// buf_size = 12KB, flush threshold = 6KB.
// When current group can't hold new data,
// new group will be opened after old oplogs flushed.
TEST_F(BufferManagerTest, FlushBufferTestFour) {
  int flush_time = 0;
  NOODLE_INIT_SYNC_POINT_SINGLETONS()->SetCallback(
      "BufferManager::FlushBuffer_END",
      [&flush_time](void* arg) { flush_time++; });
  NOODLE_INIT_SYNC_POINT_SINGLETONS()->EnableProcessing();

  int user_bufs = 1;
  int cap_per_buf = 12 << 10;  // 12KB
  CreateBM(user_bufs, cap_per_buf);
#if DCHECK_IS_ON()
  zone_manager_->ensure_seqs_ = {1, 0};
  // zone_manager_->zone_size_ = 8 << 10;
  std::vector<int> key_sizes(2, 1000);
  std::vector<int> value_sizes(2, 6000);
  std::deque<WriteBuffer::BufferDataType> q{};
  for (int i = 0; i < key_sizes.size(); i++) {
    auto iobuf = ::folly::IOBuf::createCombined(value_sizes[i]);
    std::memset(iobuf->writableData(), value_base_, value_sizes[i]);
    iobuf->append(value_sizes[i]);

    q.push_back(
        std::make_pair(std::string(key_sizes[i], key_base_), std::move(iobuf)));
  }

  while (!q.empty()) {
    index_->Put(q.front().first, std::make_pair(nullptr, Index::kSoftDel));
    bm_->Put(std::make_pair(q.front().first, std::move(q.front().second)),
             WriteBufferType::kUserDataBuf);
    q.pop_front();
  }

  while (flush_time != 2) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  auto& buffer_pools = bm_->get_buffer_pools();
  auto& data_buf = buffer_pools[0];
  // auto& log_buf = buffer_pools[2];
  EXPECT_EQ(3, zone_manager_->data_append_time_);
  EXPECT_EQ(0, data_buf->count());
  EXPECT_EQ(2, zone_manager_->total_data_written_time_);
  EXPECT_EQ(1, zone_manager_->total_oplog_written_time_);
  EXPECT_EQ(1, zone_manager_->finish_group_time_);

  NOODLE_INIT_SYNC_POINT_SINGLETONS()->DisableProcessing();
#endif
}

// Test whether `AggregateIOBuf` could simply aggregates
// IOBufs correctly.
// Notes: Final IOBuf has 8 byte field for recording
// following valid bytes length.
TEST_F(BufferManagerTest, AggregateIOBufTest) {
  int user_bufs = 1;
  int cap_per_buf = 2 << 20;  // 2MB
  CreateBM(user_bufs, cap_per_buf);

  std::vector<int> key_sizes = {100, 200, 300};
  std::vector<int> value_sizes = {3000, 4000, 5000};
  std::deque<WriteBuffer::BufferDataType> q{};
  for (int i = 0; i < key_sizes.size(); i++) {
    auto iobuf = ::folly::IOBuf::createCombined(value_sizes[i]);
    std::memset(iobuf->writableData(), value_base_, value_sizes[i]);
    iobuf->append(value_sizes[i]);

    q.push_back(
        std::make_pair(std::string(key_sizes[i], key_base_), std::move(iobuf)));
  }
  int total_valid_value =
      std::accumulate(value_sizes.begin(), value_sizes.end(), 0);
  std::unique_ptr<::folly::IOBuf> actual_iobuf =
      bm_->AggregateIOBuf(q, total_valid_value);

  EXPECT_NE(actual_iobuf, nullptr);
  EXPECT_NE(actual_iobuf->data(), nullptr);
  // 8bytes for header field.
  uint32_t expected_size = total_valid_value + 8;
  expected_size += (StorageEngineZonedStore::kAlignSize -
                    expected_size % StorageEngineZonedStore::kAlignSize) %
                   StorageEngineZonedStore::kAlignSize;

  EXPECT_EQ(expected_size, actual_iobuf->length());
  EXPECT_EQ(total_valid_value + 8,
            *reinterpret_cast<const uint64_t*>(actual_iobuf->data()));
  EXPECT_EQ(std::string(total_valid_value, value_base_),
            std::string(reinterpret_cast<const char*>(actual_iobuf->data()) + 8,
                        total_valid_value));
}

TEST_F(BufferManagerTest, SplitBufferTest) {
  NOODLE_INIT_SYNC_POINT_SINGLETONS()->LoadDependency(
      {{"BufferManager::SplitBuffer", "SplitBuffer"}});
  int flush_time = 0;
  NOODLE_INIT_SYNC_POINT_SINGLETONS()->SetCallback(
      "BufferManager::FlushBuffer_END",
      [&flush_time](void* arg) { flush_time++; });
  NOODLE_INIT_SYNC_POINT_SINGLETONS()->EnableProcessing();
  constexpr int kOneKB = 1024;
  int user_bufs = 1;
  int cap_per_buf = 8 * kOneKB;
  CreateBM(user_bufs, cap_per_buf);
#if DCHECK_IS_ON()
  // zone_manager_->zone_size_ = 6 * kOneKB;

  std::vector<int> key_sizes(3, 2 * kOneKB);
  std::vector<int> value_sizes{2 * kOneKB, 1 * kOneKB, 12 * kOneKB};
  std::deque<WriteBuffer::BufferDataType> q{};
  for (int i = 0; i < key_sizes.size(); i++) {
    auto iobuf = ::folly::IOBuf::createCombined(value_sizes[i]);
    std::memset(iobuf->writableData(), value_base_, value_sizes[i]);
    iobuf->append(value_sizes[i]);

    q.push_back(
        std::make_pair(std::string(key_sizes[i], key_base_), std::move(iobuf)));
  }

  while (!q.empty()) {
    index_->Put(q.front().first, std::make_pair(nullptr, Index::kSoftDel));
    bm_->Put(std::make_pair(q.front().first, std::move(q.front().second)),
             WriteBufferType::kUserDataBuf);
    q.pop_front();
  }

  NOODLE_TEST_SYNC_POINT("SplitBuffer");
  LOG(INFO) << "Split Buffer End";
  while (flush_time != 3) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  LOG(INFO) << "Flush End";
  auto& flush_queue = bm_->get_flush_queue();
  auto& buffer_pools = bm_->get_buffer_pools();
  // auto& data_buf = buffer_pools[0];
  auto& log_buf = buffer_pools[2];
  EXPECT_TRUE(flush_queue.isEmpty());
  EXPECT_EQ(3, zone_manager_->data_append_time_);
  EXPECT_EQ(3, log_buf->count());
  EXPECT_EQ(3, zone_manager_->total_data_written_time_);
  EXPECT_EQ(0, zone_manager_->total_oplog_written_time_);

  NOODLE_INIT_SYNC_POINT_SINGLETONS()->DisableProcessing();
#endif
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace mtcache
