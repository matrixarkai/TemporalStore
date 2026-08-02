
#include "storage/zoned_store/zone_manager.h"

#include "common/logging.h"
#include "storage/zoned_store/index.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <malloc.h>
#include <map>
#include <unistd.h>

namespace mtcache {

#define DISK_CAPACITY (10UL << 30)
#define DISK_PATH "/tmp/zone_manager"

class ZoneManagerTest : public testing::Test {
 protected:
  std::shared_ptr<ZoneManager> zmgr_;

  std::string db_path = DISK_PATH;

  void SetUp() override {
    // create directory
    ::mkdir(db_path.c_str(), 0755);
    std::string filename = db_path + "/dbase";

    auto io_module =
        NewIOModule(filename.c_str(), DISK_CAPACITY, 0 /* small mode */);
    zmgr_ = NewZoneManager(std::move(io_module), false /* using existing db*/);
  }

  void TearDown() override {
    {
      DIR* dir;
      struct dirent* dir_info;
      struct stat stat_buf;
      lstat(db_path.c_str(), &stat_buf);
      if ((dir = opendir(db_path.c_str())) == nullptr) {
        return;
      }

      while ((dir_info = readdir(dir)) != nullptr) {
        if (strcmp(dir_info->d_name, ".") == 0 ||
            strcmp(dir_info->d_name, "..") == 0) {
          continue;
        }
        std::string filename = db_path + "/" + dir_info->d_name;
        ::unlink(filename.c_str());
      }
      closedir(dir);
    }
    if (::rmdir(db_path.c_str()) != 0) {
      LOG(INFO) << strerror(errno);
    }
  }
};

TEST_F(ZoneManagerTest, WriteTest) {
  std::string stats;
  zmgr_->GetProperty("device", &stats);
  // LOG(INFO) << stats;

  uint64_t head_size = 4096;
  uint64_t group_size = zmgr_->GetGroupSize();
  uint64_t zone_size = zmgr_->GetZoneCapacity();

  // const size_t kScale = 200;  // 200 MB (one group)
  const size_t kScale = 2000;  // 2 GB

  const size_t kDataSize = 1UL << 20;  // 1 MB
  // const size_t kMetaSize = 4UL << 10;  // 4 KB (one meta)
  const size_t kMetaSize = 1UL << 20;  // 1 MB

  int num_zones_in_group = group_size / zone_size;
  int num_data_in_zone = (zone_size - head_size) / kDataSize;
  int num_meta_in_zone = (zone_size - head_size) / kMetaSize;

  // int num_data_zones_in_group = num_zones_in_group - 1; (one meta)
  int num_data_zones_in_group = num_zones_in_group / 2;
  int num_data_in_group = num_data_zones_in_group * num_data_in_zone;

  int num_meta_zones_in_group = num_zones_in_group - num_data_zones_in_group;
  int num_meta_in_group = num_meta_zones_in_group * num_meta_in_zone;

  char* data = reinterpret_cast<char*>(memalign(4096, kDataSize));
  memset(data, 0, kDataSize);

  char* meta = reinterpret_cast<char*>(memalign(4096, kMetaSize));
  memset(meta, 0, kMetaSize);

  for (uint64_t i = 0; i < kScale; i++) {
    // Before appending data or metalog, we need to eusure there has enough
    // available space in current group. Return 0 if success & read for data
    // writing.
    uint64_t key = i + 1;
    // LOG(INFO) << "key: " << key;
    snprintf(data, kDataSize, "%ld", i);
    while (!zmgr_->EnsureAvailableSpace(kDataSize, kMetaSize)) {
      if (zmgr_->FinishGroup() < 0) {
        break;
      }
    }

    // put data
    uint64_t group_id = i / num_data_in_group;
    {
      uint64_t offset = 0;
      uint64_t zone_id = (i % num_data_in_group) / num_data_in_zone;
      uint64_t loc = (i % num_meta_in_group) % num_data_in_zone;
      ASSERT_FALSE(zmgr_->Append(data, kDataSize, DataType::DATA, &offset));
      uint64_t exp_data_off = group_id * group_size + zone_id * zone_size +
                              head_size + loc * kDataSize;
      ASSERT_EQ(offset, exp_data_off);
    }

    // put meta data
    {
      uint64_t offset = 0;
      uint64_t zone_id =
          num_zones_in_group - ((i % num_data_in_group) / num_meta_in_zone) - 1;
      uint64_t loc = (i % num_data_in_group) % num_meta_in_zone;
      ASSERT_FALSE(zmgr_->Append(meta, kMetaSize, DataType::META_LOG, &offset));
      uint64_t exp_meta_off = group_id * group_size + zone_id * zone_size +
                              head_size + loc * kMetaSize;
      ASSERT_EQ(offset, exp_meta_off);
    }
  }
  free(data);
  free(meta);
}

TEST_F(ZoneManagerTest, ReadTest) {
  std::string stats;
  zmgr_->GetProperty("device", &stats);
  // LOG(INFO) << stats;

  uint64_t head_size = 4096;
  uint64_t group_size = zmgr_->GetGroupSize();
  uint64_t zone_size = zmgr_->GetZoneCapacity();

  // const size_t kScale = 200;  // 200 MB (one group)
  const size_t kScale = 2000;  // 2 GB

  const size_t kDataSize = 1UL << 20;  // 1 MB
  // const size_t kMetaSize = 4UL << 10;  // 4 KB (one meta)
  const size_t kMetaSize = 1UL << 20;  // 1 MB

  int num_zones_in_group = group_size / zone_size;
  int num_data_in_zone = (zone_size - head_size) / kDataSize;
  int num_meta_in_zone = (zone_size - head_size) / kMetaSize;

  // int num_data_zones_in_group = num_zones_in_group - 1; (one meta)
  int num_data_zones_in_group = num_zones_in_group / 2;
  int num_data_in_group = num_data_zones_in_group * num_data_in_zone;

  int num_meta_zones_in_group = num_zones_in_group - num_data_zones_in_group;
  int num_meta_in_group = num_meta_zones_in_group * num_meta_in_zone;

  std::map<int, uint64_t> index;

  char* data = reinterpret_cast<char*>(memalign(4096, kDataSize));
  memset(data, 0, kDataSize);

  char* meta = reinterpret_cast<char*>(memalign(4096, kMetaSize));
  memset(meta, 0, kMetaSize);

  for (uint64_t i = 0; i < kScale; i++) {
    // Before appending data or metalog, we need to eusure there has enough
    // available space in current group. Return 0 if success & read for data
    // writing.
    uint64_t key = i + 1;
    // LOG(INFO) << "key: " << key;
    snprintf(data, kDataSize, "%ld", key);
    while (!zmgr_->EnsureAvailableSpace(kDataSize, kMetaSize)) {
      if (zmgr_->FinishGroup() < 0) {
        break;
      }
    }

    // put data
    uint64_t group_id = i / num_data_in_group;
    {
      uint64_t offset = 0;
      uint64_t zone_id = (i % num_data_in_group) / num_data_in_zone;
      uint64_t loc = (i % num_meta_in_group) % num_data_in_zone;
      ASSERT_FALSE(zmgr_->Append(data, kDataSize, DataType::DATA, &offset));
      uint64_t exp_data_off = group_id * group_size + zone_id * zone_size +
                              head_size + loc * kDataSize;
      ASSERT_EQ(offset, exp_data_off);

      index.insert(std::make_pair(key, offset));
    }

    // put meta data
    {
      uint64_t offset = 0;
      uint64_t zone_id =
          num_zones_in_group - ((i % num_data_in_group) / num_meta_in_zone) - 1;
      uint64_t loc = (i % num_data_in_group) % num_meta_in_zone;
      ASSERT_FALSE(zmgr_->Append(meta, kMetaSize, DataType::META_LOG, &offset));
      uint64_t exp_meta_off = group_id * group_size + zone_id * zone_size +
                              head_size + loc * kMetaSize;
      ASSERT_EQ(offset, exp_meta_off);
    }
  }

  // Read
  char* buf = reinterpret_cast<char*>(memalign(4096, kDataSize));
  memset(buf, 0, kDataSize);

  for (uint64_t key = 1; key <= kScale; key++) {
    auto it = index.find(key);
    uint64_t value = 0;
    ASSERT_FALSE(zmgr_->Read(buf, it->second, kDataSize));
    uint64_t res;
    sscanf(buf, "%ld", &res);
    ASSERT_EQ(res, key);
  }

  free(buf);
  free(data);
  free(meta);
}

TEST_F(ZoneManagerTest, Trim) {
  std::string stats;
  zmgr_->GetProperty("device", &stats);
  // LOG(INFO) << stats;

  uint64_t head_size = 4096;
  uint64_t group_size = zmgr_->GetGroupSize();
  uint64_t zone_size = zmgr_->GetZoneCapacity();

  // const size_t kScale = 200;  // 200 MB (one group)
  const size_t kScale = 1000;  // 1 GB

  const size_t kDataSize = 1UL << 20;  // 1 MB
  const size_t kMetaSize = 4UL << 10;  // 4 KB (one meta)
  // const size_t kMetaSize = 1UL << 20;  // 1 MB

  int num_zones_in_group = group_size / zone_size;
  int num_data_in_zone = (zone_size - head_size) / kDataSize;
  int num_meta_in_zone = (zone_size - head_size) / kMetaSize;

  int num_data_zones_in_group = num_zones_in_group - 1;  // (one meta)
  // int num_data_zones_in_group = num_zones_in_group / 2;
  int num_data_in_group = num_data_zones_in_group * num_data_in_zone;

  int num_meta_zones_in_group = num_zones_in_group - num_data_zones_in_group;
  int num_meta_in_group = num_meta_zones_in_group * num_meta_in_zone;

  char* data = reinterpret_cast<char*>(memalign(4096, kDataSize));
  memset(data, 0, kDataSize);

  char* meta = reinterpret_cast<char*>(memalign(4096, kMetaSize));
  memset(meta, 0, kMetaSize);

  std::map<int, uint64_t> index;

  for (uint64_t i = 0; i < kScale; i++) {
    // Before appending data or metalog, we need to eusure there has enough
    // available space in current group. Return 0 if success & read for data
    // writing.
    uint64_t key = i + 1;
    // LOG(INFO) << "key: " << key;
    snprintf(data, kDataSize, "%ld", i);
    while (!zmgr_->EnsureAvailableSpace(kDataSize, kMetaSize)) {
      if (zmgr_->FinishGroup() < 0) {
        break;
      }
    }

    // put data
    uint64_t group_id = i / num_data_in_group;
    {
      uint64_t offset = 0;
      uint64_t zone_id = (i % num_data_in_group) / num_data_in_zone;
      uint64_t loc = (i % num_meta_in_group) % num_data_in_zone;
      ASSERT_FALSE(zmgr_->Append(data, kDataSize, DataType::DATA, &offset));
      uint64_t exp_data_off = group_id * group_size + zone_id * zone_size +
                              head_size + loc * kDataSize;
      ASSERT_EQ(offset, exp_data_off);

      index.insert(std::make_pair(key, offset));
    }

    // put meta data
    {
      uint64_t offset = 0;
      uint64_t zone_id =
          num_zones_in_group - ((i % num_data_in_group) / num_meta_in_zone) - 1;
      uint64_t loc = (i % num_data_in_group) % num_meta_in_zone;
      ASSERT_FALSE(zmgr_->Append(meta, kMetaSize, DataType::META_LOG, &offset));
      uint64_t exp_meta_off = group_id * group_size + zone_id * zone_size +
                              head_size + loc * kMetaSize;
      ASSERT_EQ(offset, exp_meta_off);
    }
  }

  zmgr_->GetProperty("group", &stats);
  // LOG(INFO) << stats;

  zmgr_->GetProperty("garbage", &stats);
  // LOG(INFO) << stats;

  // Test Trim

  // Group 0
  int first_key = 1;
  for (int i = 0; i < num_data_in_group * 0.4; i++) {
    auto it = index.find(first_key + i);
    ASSERT_NE(it, index.end());
    zmgr_->TrimBytes(it->second, kDataSize);
  }

  // Group 1
  // first_key = num_data_in_group;
  // for (int i = 0; i < num_data_in_group * 0.8; i++) {
  //   auto it = index.find(first_key + i);
  //   zmgr_->TrimBytes(it->second, kDataSize);
  // }

  zmgr_->GetProperty("garbage", &stats);
  // LOG(INFO) << stats;

  /////////////////////////////////////////
  bool is_lossy;
  int16_t gid;
  std::tie(gid, is_lossy) = zmgr_->FindGCGroup();
  if (gid > 0) {
    zmgr_->ResetGroup(gid);
  }

  zmgr_->GetProperty("group", &stats);
  // LOG(INFO) << stats;

  zmgr_->GetProperty("garbage", &stats);
  // LOG(INFO) << stats;

  free(data);
  free(meta);
}

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace mtcache
