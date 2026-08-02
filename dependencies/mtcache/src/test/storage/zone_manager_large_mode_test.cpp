
#include "common/logging.h"
#include "storage/zoned_store/index.h"
#include "storage/zoned_store/zone_manager.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <filesystem>
#include <malloc.h>
#include <map>
#include <unistd.h>

namespace mtcache {

#define DISK_CAPACITY (10UL << 30)

DEFINE_string(test_zone_manager_device_name, "", "device name");

class ZoneManagerTest : public testing::Test {
 protected:
  std::string db_path = "/tmp/zone_manager";

  std::shared_ptr<ZoneManager> CreateZoneManagerObj(bool using_existing_db) {
    std::string filename;
    if (FLAGS_test_zone_manager_device_name.empty()) {
      filename = db_path + "/dbase";
    } else {
      filename = FLAGS_test_zone_manager_device_name;
    }
    auto dev = NewDevice(filename.c_str(), DISK_CAPACITY, 1 /* large mode */);
    auto zmgr = NewZoneManager(std::move(dev), using_existing_db);
    return zmgr;
  }

  void SetUp() override {
    // create database directory
    if (FLAGS_test_zone_manager_device_name.empty()) {
      ::mkdir(db_path.c_str(), 0755);
    }
  }

  void TearDown() override {
    if (FLAGS_test_zone_manager_device_name.empty()) {
      std::filesystem::remove_all(db_path);
    }
  }
};

TEST_F(ZoneManagerTest, WriteTest) {
  std::string stats;
  auto zmgr = CreateZoneManagerObj(false);
  zmgr->GetProperty("device", &stats);
  LOG(INFO) << stats;
  uint64_t zone_size = 1ul << 30;
  EXPECT_EQ(zmgr->GetZoneSize(), zone_size);

  const uint64_t page_size = 4096;
  const uint64_t header_size = page_size;

  const size_t kScale = 4000;  // 4 GB

  const size_t kDataSize = 1UL << 20;  // 1 MB
  const size_t kMetaSize = 1UL << 10;  // 1 KB

  // header : 4 KB
  // data : 1022 MB
  // meta : 8 + 1022 KB
  // padding : 1016 KB
  // footer : 4KB
  int num_data_in_zone = 1022;

  char* data = reinterpret_cast<char*>(memalign(4096, kDataSize));
  memset(data, 0, kDataSize);

  uint64_t meta_off = 8;
  char* meta_buf = reinterpret_cast<char*>(memalign(page_size, 4UL << 20));
  memset(meta_buf, 0, 4UL << 20);

  uint64_t max_zone_id = 0;
  for (uint64_t i = 0; i < kScale; i++) {
    // Before appending data or metalog, we need to eusure there has enough
    // available space in current group.
    uint64_t key = i + 1;
    // LOG(INFO) << "key: " << key;
    snprintf(data, kDataSize, "%ld", key);

    // padding bytes
    uint64_t padding_bytes = (page_size - meta_off % page_size) % page_size;
    while (!zmgr->EnsureAvailableSpace(kDataSize, meta_off + padding_bytes)) {
      // Put meta data
      uint64_t cnt = meta_off / kMetaSize;
      memcpy(meta_buf, &cnt, sizeof(uint64_t));

      zmgr->Append(meta_buf, meta_off + padding_bytes, DataType::META_LOG,
                   nullptr);
      if (zmgr->FinishGroup() < 0) {
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
      ASSERT_FALSE(zmgr->Append(data, kDataSize, DataType::DATA, &offset));
      uint64_t exp_data_off =
          header_size + max_zone_id * zone_size + loc * kDataSize;
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

    zmgr->Append(meta_buf, meta_off + padding_bytes, DataType::META_LOG,
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
    zmgr->LoadMetaData(i, meta_cb);
  }

  free(data);
  free(meta_buf);
}

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}

}  // namespace mtcache
