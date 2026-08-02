#include "storage/zoned_store/codec.h"

#include "storage/zoned_store/index.h"
#include "storage/zoned_store/util.h"
#include "storage/zoned_store/zoned_store.h"

#include <gtest/gtest.h>
#include <sys/types.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

namespace mtcache {

class MockIndex : public Index {
 public:
  bool UpdateIndex(const std::string& key, ValueType value) override {
    key_colored_ptr_vec.emplace_back(key,
                                     std::get<Index::SSDColoredPtr>(value));
    return true;
  }

  std::vector<std::pair<std::string, Index::SSDColoredPtr>> key_colored_ptr_vec;
};

class TestBufferEncoder : public ::testing::Test {
 protected:
  void SetUp() override {
    index_ = std::make_shared<MockIndex>();
    encoder_ = new BufferEncoder(96 << 20);
    index_updater_ = std::make_shared<IndexUpdater>(index_);

    uint64_t temp_batch_offset_ = batch_begin_offset_;
    for (int i = 0; i < kv_count_; i++) {
      uint32_t value_len = value_size_vec_[i % value_size_vec_.size()];
      uint32_t key_len = key_size_vec_[i % key_size_vec_.size()];

      // Data part.
      total_bytes_data_ += (encoder_->data_fixed_part_size_ + value_len);
      record_lba_value_.push_back(temp_batch_offset_);
      data_length_value_.push_back(value_len + encoder_->data_fixed_part_size_);
      temp_batch_offset_ += (encoder_->data_fixed_part_size_ + value_len);
      // Oplog part.
      total_bytes_oplog_ += (encoder_->oplog_fixed_part_size_ + key_len);
      oplog_length_value_.push_back(key_len + encoder_->oplog_fixed_part_size_);

      // k-v parir part.
      auto iobuf = ::folly::IOBuf::createCombined(value_len);
      iobuf->append(value_len);
      std::memset(reinterpret_cast<char*>(iobuf->writableBuffer()), value_base_,
                  value_len);
      user_data_.emplace_back(std::string(key_len, key_base_),
                              std::move(iobuf));
      index_->Put(std::string(key_len, key_base_),
                  std::make_pair(nullptr, Index::kSoftDel));
    }

    EXPECT_EQ(
        record_lba_value_.back(),
        batch_begin_offset_ + total_bytes_data_ - data_length_value_.back());
  }

  void TearDown() override { delete encoder_; }

  std::shared_ptr<MockIndex> index_;
  std::shared_ptr<IndexUpdater> index_updater_;
  BufferEncoder* encoder_;
  std::deque<WriteBuffer::BufferDataType> user_data_;

  const int kv_count_ = 16;
  std::vector<int> value_size_vec_{201, 321, 321, 543, 102, 340, 104, 777};
  std::vector<int> key_size_vec_{10, 21, 33, 40, 40, 40, 21, 10};
  uint64_t batch_begin_offset_ = 0x1000;
  char key_base_ = 'k';
  char value_base_ = 'a';

  uint32_t total_bytes_data_ = 0;
  uint32_t total_bytes_oplog_ = 0;
  std::vector<uint64_t> record_lba_value_{};
  std::vector<uint32_t> data_length_value_{};
  std::vector<uint32_t> oplog_length_value_{};
};

TEST_F(TestBufferEncoder, DataTest) {
  char encoded_buf[4097];
  int j = 0;
  bool is_corrupted;
  uint32_t actual_value_len;
  uint32_t expected_value_len;
  char actual_value[4096];
  std::string expected_value;
  for (auto& value_buf : user_data_) {
    expected_value_len = value_size_vec_[j++];
    j = j % value_size_vec_.size();
    char* tmp_encoded_buf = encoded_buf;
    encoder_->SerializeData(value_buf.second, tmp_encoded_buf);
    encoder_->DeserializeData(tmp_encoded_buf, &actual_value_len, actual_value,
                              &is_corrupted);
    actual_value[actual_value_len] = 0;
    ASSERT_TRUE(!is_corrupted);
    ASSERT_EQ(expected_value_len, actual_value_len);
    expected_value = std::string(expected_value_len, value_base_);
    EXPECT_STREQ(expected_value.data(), actual_value);
  }
}

TEST_F(TestBufferEncoder, OplogTest) {
  auto update_entry_cb = [&updater = this->index_updater_](
                             const std::string& key,
                             Index::ValueType new_value) {
    return updater->UpdateIndex(key, new_value);
  };
  auto encoded_oplog_ptr = encoder_->SerializeOplog(
      user_data_, update_entry_cb, batch_begin_offset_, total_bytes_oplog_);
  EXPECT_NE(encoded_oplog_ptr, nullptr);

  // Check if colored pointer is right.
  for (int i = 0; i < kv_count_; i++) {
    int j = i % value_size_vec_.size();
    uint64_t colored_ptr = index_->key_colored_ptr_vec[i].second;

    // 43 LBA bits.
    EXPECT_EQ(record_lba_value_[i],
              ((colored_ptr & StorageEngineZonedStore::kSSDLBAFlags) >> 19));
    // 12 size bits.
    EXPECT_EQ(
        (value_size_vec_[j] + 4096 - 1) >> 12,
        ((colored_ptr & StorageEngineZonedStore::kSSDRecordSizeFlags) >> 7));
  }

  // Check if aggragated oplog iobuf is right
  const char* current_ptr =
      reinterpret_cast<const char*>(encoded_oplog_ptr->data());
  uint32_t expected_key_len = 0;
  uint32_t actual_key_len = 0;
  std::string expected_key = "";
  std::string actual_key = "";
  uint64_t actual_offset = 0;
  bool is_corrupted;
  for (int i = 0; i < kv_count_; i++) {
    int j = i % value_size_vec_.size();
    current_ptr =
        encoder_->DeserializeOplog(current_ptr, &actual_key_len, &actual_key,
                                   &actual_offset, &is_corrupted);
    expected_key_len = key_size_vec_[j];
    // expected_offset = record_lba_value_[i];
    uint64_t colored_ptr = index_->key_colored_ptr_vec[i].second;
    ASSERT_TRUE(!is_corrupted);
    ASSERT_EQ(expected_key_len, actual_key_len);
    expected_key = std::string(expected_key_len, key_base_);
    EXPECT_STREQ(expected_key.data(), actual_key.data());
    EXPECT_EQ(colored_ptr, actual_offset);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace mtcache
