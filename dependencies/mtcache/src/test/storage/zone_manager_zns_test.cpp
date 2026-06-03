
#include "common/logging.h"
#include "storage/zoned_store/index.h"
#include "storage/zoned_store/zone_manager.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <malloc.h>
#include <map>
#include <unistd.h>

namespace mtcache {

#define DISK_CAPACITY (16UL << 30)

class ZoneManagerTest : public testing::Test {
 protected:
  std::shared_ptr<ZoneManager> zmgr_;
  std::string db_path = "";

  void SetUp() override {
    auto dev_ = NewDevice(db_path.c_str(), 16UL << 30 /* capacity */,
                          1 /* large mode */);
    zmgr_ = NewZoneManager(std::move(dev_), false /* using existing db */);
  }

  void TearDown() override {}
};

TEST_F(ZoneManagerTest, WriteTest) {
  std::string stats;
  zmgr_->GetProperty("device", &stats);
  LOG(INFO) << stats;

  const uint64_t page_size = 4096;

  uint64_t head_size = page_size;
  uint64_t group_size = zmgr_->GetGroupSize();
  uint64_t zone_capacity = zmgr_->GetZoneCapacity();

  const size_t kScale = 4000;  // 4 GB

  const size_t kDataSize = 1UL << 20;  // 1 MB
  const size_t kMetaSize = 1UL << 10;  // 1 KB

  uint64_t need_bytes = head_size;
  uint64_t num_data_in_zone = 0;
  while (true) {
    need_bytes += (kDataSize + kMetaSize);
    if (need_bytes > zone_capacity) {
      break;
    }
    num_data_in_zone++;
  }

  LOG(INFO) << "Num Data In Zone: " << num_data_in_zone;

  char* data = reinterpret_cast<char*>(memalign(4096, kDataSize));
  memset(data, 0, kDataSize);

  uint64_t meta_off = 8;
  char* meta_buf = reinterpret_cast<char*>(memalign(page_size, 4UL << 20));
  memset(meta_buf, 0, 4UL << 20);

  uint64_t max_zone_id = 0;
  for (uint64_t i = 0; i < kScale; i++) {
    // Before appending data or metalog, we need to eusure there has enough
    // available space in current group. Return 0 if success & read for data
    // writing.
    uint64_t key = i + 1;
    // LOG(INFO) << "key: " << key;
    snprintf(data, kDataSize, "%ld", key);

    // padding bytes
    uint64_t padding_bytes = (page_size - meta_off % page_size) % page_size;
    while (!zmgr_->EnsureAvailableSpace(kDataSize, meta_off + padding_bytes)) {
      // Put meta data
      uint64_t cnt = meta_off / kMetaSize;
      memcpy(meta_buf, &cnt, sizeof(uint64_t));

      zmgr_->Append(meta_buf, meta_off + padding_bytes, DataType::META_LOG,
                    nullptr);
      if (zmgr_->FinishGroup() < 0) {
        break;
      } else {
        memset(meta_buf, 0, sizeof(uint64_t));
        meta_off = 8;
        padding_bytes = (page_size - meta_off % page_size) % page_size;
      }
    }

    // Put data
    max_zone_id = i / num_data_in_zone;
    {
      uint64_t offset = 0;
      uint64_t loc = i % num_data_in_zone;
      ASSERT_FALSE(zmgr_->Append(data, kDataSize, DataType::DATA, &offset));
      uint64_t exp_data_off =
          head_size + max_zone_id * group_size + loc * kDataSize;
      ASSERT_EQ(offset, exp_data_off);
    }

    // Put meta data into buf
    {
      memcpy(meta_buf + meta_off, &key, sizeof(uint64_t));
      meta_off += kMetaSize;
    }
  }

  {
    // padding bytes
    uint64_t padding_bytes = (page_size - meta_off % page_size) % page_size;

    // Put meta data
    uint64_t cnt = meta_off / kMetaSize;
    memcpy(meta_buf, &cnt, sizeof(uint64_t));

    zmgr_->Append(meta_buf, meta_off + padding_bytes, DataType::META_LOG,
                  nullptr);
  }

  //  check
  int start_key = 1;

  std::function<int(const char* buf)> meta_cb([&](const char* meta_buf) -> int {
    uint64_t cnt = 0;
    uint64_t meta_off = 0;
    memcpy(&cnt, meta_buf, sizeof(uint64_t));
    meta_off += sizeof(uint64_t);
    uint64_t key = 0;
    for (int i = 0; i < cnt; i++) {
      memcpy(&key, meta_buf + meta_off, sizeof(uint64_t));
      assert(key == start_key);
      start_key++;
      meta_off += kMetaSize;
    }
    return 0;
  });

  for (uint64_t i = 0; i <= max_zone_id; i++) {
    zmgr_->LoadMetaData(i, meta_cb);
  }

  free(data);
  free(meta_buf);
}

TEST_F(ZoneManagerTest, Trim) {
  std::string stats;
  zmgr_->GetProperty("device", &stats);
  LOG(INFO) << stats;

  const uint64_t page_size = 4096;

  uint64_t head_size = page_size;
  uint64_t group_size = zmgr_->GetGroupSize();
  uint64_t zone_capacity = zmgr_->GetZoneCapacity();

  const size_t kScale = 4000;  // 4 GB

  const size_t kDataSize = 1UL << 20;  // 1 MB
  const size_t kMetaSize = 1UL << 10;  // 1 KB

  uint64_t need_bytes = head_size;
  uint64_t num_data_in_zone = 0;
  while (true) {
    need_bytes += (kDataSize + kMetaSize);
    if (need_bytes > zone_capacity) {
      break;
    }
    num_data_in_zone++;
  }

  LOG(INFO) << "Num Data In Zone: " << num_data_in_zone;

  char* data = reinterpret_cast<char*>(memalign(4096, kDataSize));
  memset(data, 0, kDataSize);

  uint64_t meta_off = 8;
  char* meta_buf = reinterpret_cast<char*>(memalign(page_size, 4UL << 20));
  memset(meta_buf, 0, 4UL << 20);

  std::map<int, uint64_t> index;
  uint64_t max_zone_id = 0;
  for (uint64_t i = 0; i < kScale; i++) {
    // Before appending data or metalog, we need to eusure there has enough
    // available space in current group. Return 0 if success & read for data
    // writing.
    uint64_t key = i + 1;
    // LOG(INFO) << "key: " << key;
    snprintf(data, kDataSize, "%ld", key);

    // padding bytes
    uint64_t padding_bytes = (page_size - meta_off % page_size) % page_size;
    while (!zmgr_->EnsureAvailableSpace(kDataSize, meta_off + padding_bytes)) {
      // Put meta data
      uint64_t cnt = meta_off / kMetaSize;
      memcpy(meta_buf, &cnt, sizeof(uint64_t));

      zmgr_->Append(meta_buf, meta_off + padding_bytes, DataType::META_LOG,
                    nullptr);
      if (zmgr_->FinishGroup() < 0) {
        break;
      } else {
        memset(meta_buf, 0, sizeof(uint64_t));
        meta_off = 8;
        padding_bytes = (page_size - meta_off % page_size) % page_size;
      }
    }

    // Put data
    max_zone_id = i / num_data_in_zone;
    {
      uint64_t offset = 0;
      uint64_t loc = i % num_data_in_zone;
      ASSERT_FALSE(zmgr_->Append(data, kDataSize, DataType::DATA, &offset));
      uint64_t exp_data_off =
          head_size + max_zone_id * group_size + loc * kDataSize;
      ASSERT_EQ(offset, exp_data_off);

      index.insert(std::make_pair(key, offset));
    }

    // Put meta data into buf
    {
      memcpy(meta_buf + meta_off, &key, sizeof(uint64_t));
      meta_off += kMetaSize;
    }
  }

  {
    // padding bytes
    uint64_t padding_bytes = (page_size - meta_off % page_size) % page_size;

    // Put meta data
    uint64_t cnt = meta_off / kMetaSize;
    memcpy(meta_buf, &cnt, sizeof(uint64_t));

    zmgr_->Append(meta_buf, meta_off + padding_bytes, DataType::META_LOG,
                  nullptr);
  }

  zmgr_->GetProperty("group", &stats);
  LOG(INFO) << stats;

  zmgr_->GetProperty("garbage", &stats);
  LOG(INFO) << stats;

  //////////////// Trim ////////////////////
  uint64_t first_key = 1;
  for (uint64_t i = 0; i < num_data_in_zone * 0.4; i++) {
    auto it = index.find(first_key + i);
    zmgr_->TrimBytes(it->second, kDataSize);
  }
  /////////////////////////////////////////

  zmgr_->GetProperty("group", &stats);
  LOG(INFO) << stats;

  zmgr_->GetProperty("garbage", &stats);
  LOG(INFO) << stats;

  /////////////// Reset ////////////////////
  bool is_lossy;
  int16_t gid;
  std::tie(gid, is_lossy) = zmgr_->FindGCGroup();
  if (gid > 0) {
    zmgr_->ResetGroup(gid);
  }

  /////////////////////////////////////////

  zmgr_->GetProperty("group", &stats);
  LOG(INFO) << stats;

  zmgr_->GetProperty("garbage", &stats);
  LOG(INFO) << stats;

  free(data);
  free(meta_buf);
}

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace mtcache
