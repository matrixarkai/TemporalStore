#include "common/logging.h"

#include <gtest/gtest.h>
#include <rocksdb/db.h>

#include <filesystem>
#include <string>

namespace mtcache {

class TerarkDBTest : public testing::Test {
 public:
  void SetUp() override {
    // clean TMP_DIR
    char path[256] = "/tmp/terarkdb_XXXXXX";
    if (mkdtemp(path) == nullptr) {
      LOG(FATAL) << "create tmp_dir failed, errno=" << errno;
    }
    db_dir_ = std::string(path);
    LOG(INFO) << "create tmp_dir, tmp_dir=" << db_dir_;
  }

  void TearDown() override {
    // clean TMP_DIR
    LOG(INFO) << "clean tmp, tmp_dir=" << db_dir_;
    std::filesystem::remove_all(db_dir_);
  }

  std::string db_dir_;
};

TEST_F(TerarkDBTest, Basic) {
  rocksdb::DB* db;
  rocksdb::Options options;
  options.create_if_missing = true;
  options.wal_bytes_per_sync = 32768;
  options.bytes_per_sync = 32768;

  auto s = rocksdb::DB::Open(options, db_dir_, &db);
  EXPECT_TRUE(s.ok()) << s.ToString();

  std::string value;
  s = db->Put(rocksdb::WriteOptions(), "key1", "value1");
  s = db->Get(rocksdb::ReadOptions(), "key1", &value);
  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ("value1", value);
  s = db->Delete(rocksdb::WriteOptions(), "key1");
  EXPECT_TRUE(s.ok()) << s.ToString();
  s = db->Get(rocksdb::ReadOptions(), "key1", &value);
  EXPECT_FALSE(s.ok()) << s.ToString();

  delete db;
  db = nullptr;
}

TEST_F(TerarkDBTest, OtherOpen) {
  rocksdb::DB* db = nullptr;
  rocksdb::DBOptions options;
  options.create_if_missing = true;
  options.wal_bytes_per_sync = 32768;
  options.bytes_per_sync = 32768;

  auto cfopt = rocksdb::ColumnFamilyOptions();
  cfopt.num_levels = 2;

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  column_families.emplace_back("default", cfopt);

  std::vector<rocksdb::ColumnFamilyHandle*> handles;

  auto s = rocksdb::DB::Open(options, db_dir_, column_families, &handles, &db);
  EXPECT_TRUE(s.ok()) << s.ToString();

  EXPECT_GE(handles.size(), 1);
  rocksdb::ColumnFamilyHandle* handle = handles[0];

  std::string value;
  s = db->Put(rocksdb::WriteOptions(), handle, "key1", "value1");
  s = db->Get(rocksdb::ReadOptions(), handle, "key1", &value);
  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ("value1", value);
  s = db->Delete(rocksdb::WriteOptions(), handle, "key1");
  EXPECT_TRUE(s.ok()) << s.ToString();
  s = db->Get(rocksdb::ReadOptions(), handle, "key1", &value);
  EXPECT_FALSE(s.ok()) << s.ToString();

  for (auto handle : handles) {
    delete handle;
  }
  delete db;
  db = nullptr;
}

TEST_F(TerarkDBTest, DropAll) {
  rocksdb::DB* db = nullptr;
  rocksdb::DBOptions options;
  options.create_if_missing = true;
  options.wal_bytes_per_sync = 32768;
  options.bytes_per_sync = 32768;
  options.create_missing_column_families = true;

  auto cfopt = rocksdb::ColumnFamilyOptions();
  cfopt.num_levels = 2;

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  column_families.emplace_back("default", cfopt);
  column_families.emplace_back("default-2", cfopt);

  std::vector<rocksdb::ColumnFamilyHandle*> handles;

  auto s = rocksdb::DB::Open(options, db_dir_, column_families, &handles, &db);
  EXPECT_TRUE(s.ok()) << s.ToString();

  EXPECT_GE(handles.size(), 2);
  rocksdb::ColumnFamilyHandle* handle = handles[1];
  EXPECT_NE(nullptr, handle);

  std::string value;
  s = db->Put(rocksdb::WriteOptions(), handle, "key1", "value1");
  s = db->Get(rocksdb::ReadOptions(), handle, "key1", &value);
  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ("value1", value);

  // Drop All
  s = db->DropColumnFamily(handle);
  EXPECT_TRUE(s.ok()) << s.ToString();
  // Destroy handle
  s = db->DestroyColumnFamilyHandle(handle);
  EXPECT_TRUE(s.ok()) << s.ToString();
  handle = nullptr;

  // Reopen handle
  s = db->CreateColumnFamily(cfopt, "default-2", &handle);
  EXPECT_TRUE(s.ok()) << s.ToString();
  handles[1] = handle;

  // Get
  s = db->Get(rocksdb::ReadOptions(), handle, "key1", &value);
  EXPECT_FALSE(s.ok()) << s.ToString();

  for (auto handle : handles) {
    delete handle;
  }
  delete db;
  db = nullptr;
}

}  // namespace mtcache

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
