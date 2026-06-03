#include "common/logging.h"
#include "storage/zoned_store/buffer_manager.h"
#include "storage/zoned_store/codec.h"
#include "storage/zoned_store/gc.h"
#include "storage/zoned_store/index.h"
#include "storage/zoned_store/util.h"
#include "storage/zoned_store/zone_manager.h"
#include "storage/zoned_store/zoned_store.h"
#include "tools/utils.h"

#include <folly/io/IOBuf.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

namespace mtcache {

// In memory ZoneManager.
class MockZoneManager : public ZoneManager {
 public:
  MockZoneManager() : ZoneManager({}) {}
  ~MockZoneManager() { free(buf_start_point_); }

 public:
  // Use Memory to simulate SSD.
  int Append(const char* buf, int sz, DataType type,
             uint64_t* offset) override {
    DCHECK_LE(sz + current_buf_ - buf_start_point_, buf_len_);
    std::memcpy(current_buf_, buf, sz);
    *offset = current_buf_ - buf_start_point_;
    current_buf_ += sz;
    return 1;
  }

  int Read(char* buf, uint64_t offset, int sz) override {
    DCHECK(buf);
    DCHECK_LE(offset + sz, buf_len_);
    std::memcpy(buf, buf_start_point_ + offset, sz);
    return 0;
  }

  int EnsureAvailableSpace(int data_size, int meta_size) override { return 0; }

  void TrimBytes(uint64_t offset, int size) override {}

  std::pair<int16_t, GCMode> FindGCGroup() override { return {-1, LOSSY}; }

  int ResetGroup(uint16_t group_id) override { return 0; }

  int LoadMetaData(int group_id,
                   GCWorker::LoadMetaCallback meta_callback) override {
    return 1;
  }

  bool GetProperty(std::string property, std::string* result) override {
    return true;
  }

  int FinishGroup() override { return 0; };

  void Recovery(std::function<int(const char* buf)> meta_cb) override {}

  uint64_t GetUsedSpace() override { return 0; }

  void init(int size) {
    buf_start_point_ = static_cast<char*>(memalign(4096, size));
    current_buf_ = buf_start_point_;
    buf_len_ = size;
  }

  int buf_len_;
  char* buf_start_point_;
  char* current_buf_;
};

class GCWorkerTest : public testing::Test {
 protected:
  std::shared_ptr<GCWorker> gc_;
  std::shared_ptr<BufferEncoder> encoder_;
  std::shared_ptr<Index> index_;
  std::shared_ptr<MockZoneManager> zone_manager_;
  std::shared_ptr<IndexUpdater> index_updater_;

  char* oplog_;
  uint32_t oplog_size_ = 0;
  uint32_t zone_size_ = 0;
  const int encoder_buf_size_ = (96UL << 20);
  char* encoded_data_buf_ = nullptr;

  std::string expected_key;
  std::string expected_value;

  void SetUp() override {
    zone_manager_ = std::make_shared<MockZoneManager>();
    index_ = std::make_shared<Index>();
    encoder_ = std::make_shared<BufferEncoder>(encoder_buf_size_);
    index_updater_ = std::make_shared<IndexUpdater>(index_);
    gc_ = std::make_shared<GCWorker>(nullptr, zone_manager_, encoder_,
                                     index_updater_, (16 << 20));
    encoded_data_buf_ = reinterpret_cast<char*>(
        memalign(StorageEngineZonedStore::kAlignSize, 100 << 10));

    gc_->Start();
    zone_size_ = (1 << 20);
    zone_manager_->init(zone_size_);
    oplog_ = static_cast<char*>(
        memalign(StorageEngineZonedStore::kAlignSize, zone_size_));
  }

  void TearDown() override {
    gc_->Stop();
    free(encoded_data_buf_);
    free(oplog_);
  }

  // TODO(fangliming) : currently, `is_lossy` and `type` isn't used.
  // @type:1 for normal,2 for soft deleted,3 for pinned.
  // It creates oplog and data.
  void CreateOplog(const std::string& key, const std::string& value) {
    auto wb = std::make_unique<WriteBuffer>();
    wb->push_back({key, folly::IOBuf::copyBuffer(value.data(), value.size())});
    index_->Put(
        key, std::make_pair(folly::IOBuf::copyBuffer(value), Index::kSoftDel));

    char* tmp_buf = encoder_->SerializeData(
        ::folly::IOBuf::copyBuffer(value.data(), value.size()),
        encoded_data_buf_);
    uint64_t batch_begin_offset = 0ul;
    zone_manager_->Append(encoded_data_buf_, tmp_buf - encoded_data_buf_, {},
                          &batch_begin_offset);

    int oplog_size = encoder_->CalculateEncodedOpLogSize(wb);
    auto update_entry_cb = [&updater = index_updater_](
                               const std::string& key,
                               Index::ValueType new_value) {
      return updater->UpdateIndex(key, new_value);
    };
    auto serialized_oplog = encoder_->SerializeOplog(
        wb->get_buf_q(), update_entry_cb, batch_begin_offset, oplog_size);
    PutFixedUint64(oplog_, 8 + serialized_oplog->length());
    std::memcpy(oplog_ + 8, serialized_oplog->data(),
                serialized_oplog->length());
    oplog_size_ = 8 + serialized_oplog->length();
  }
};

// Test if `GCWorker` could quickly start and finish.
TEST_F(GCWorkerTest, BasicTest) {
  // No statement is required.
}

TEST_F(GCWorkerTest, ConstructSingleRecordTestOne) {
  const std::string expected_key(100, 'k');
  const std::string expected_value(10 << 10, 'v');

  CreateOplog(expected_key, expected_value);
  WriteBuffer::BufferDataType data;

  gc_->ConstructSingleRecord(oplog_ + 8, data);
  ASSERT_EQ(expected_key.size(), data.first.size());
  ASSERT_EQ(expected_value.size(), data.second->length());
  EXPECT_STREQ(expected_key.data(), data.first.data());
  std::string actual_value(data.second->length(), 'v');
  EXPECT_STREQ(expected_value.data(), actual_value.data());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace mtcache
