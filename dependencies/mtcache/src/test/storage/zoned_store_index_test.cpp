
#include "common/logging.h"
#include "storage/zoned_store/index.h"
#include "storage/zoned_store/zoned_store.h"

#include <folly/io/IOBuf.h>
#include <gtest/gtest.h>

#include <memory>
#include <variant>

namespace mtcache {

class IndexTest : public testing::Test {
 protected:
  std::shared_ptr<Index> idx;
  void SetUp() override {
    idx = std::make_shared<Index>();
    std::shared_ptr<folly::IOBuf> value_buf(folly::IOBuf::createCombined(9));
    value_buf->append(9);
    idx->Put("abc", 5u);
    idx->Put("cba", std::make_pair(value_buf, Index::kSoftDel));
  }

  void TearDown() override {}
};

TEST_F(IndexTest, GetaAndPutTest) {
  auto value = idx->Get("abc");
  EXPECT_TRUE(std::holds_alternative<uint64_t>(value));
  EXPECT_EQ(5u, std::get<uint64_t>(value));
  value = idx->Get("cba");
  EXPECT_TRUE(std::holds_alternative<Index::MemoryColoredPtr>(value));
  auto buf = std::get<Index::MemoryColoredPtr>(value);
  EXPECT_EQ(9, buf.first->length());
}

TEST_F(IndexTest, UpdateTest) {
  std::shared_ptr<folly::IOBuf> new_value_buf(folly::IOBuf::createCombined(90));
  new_value_buf->append(90);
  idx->UpdateIndex("abc", std::make_pair(new_value_buf, Index::kSoftDel));
  idx->UpdateIndex("cba", 21u);

  auto value = idx->Get("cba");
  EXPECT_TRUE(std::holds_alternative<uint64_t>(value));
  EXPECT_EQ(21u, std::get<uint64_t>(value));
  value = idx->Get("abc");
  EXPECT_TRUE(std::holds_alternative<Index::MemoryColoredPtr>(value));
  auto buf = std::get<Index::MemoryColoredPtr>(value);
  EXPECT_EQ(90, buf.first->length());
}

}  // namespace mtcache

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
