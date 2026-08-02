#include "buffer/raw_buffer.h"
#include "buffer/string_buffer.h"
#include "buffer/string_view_buffer.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <string>

namespace mtcache {

TEST(RawBuffer, BaseTest) {
  char* buf1 = new char[10];
  RawBuffer buffer1(buf1, 10, nullptr, false);

  // the two lines should cause compile error
  // RawBuffer buffer2 = buffer1;
  // RawBuffer buffer3(buffer1);

  EXPECT_TRUE(buffer1.Key().empty());
  std::string buffer_key("buffer1");
  buffer1.SetKey(buffer_key);
  EXPECT_EQ(buffer1.Key(), buffer_key);

  RawBuffer buffer4 = std::move(buffer1);
  EXPECT_EQ(buffer1.Size(), 0);
  EXPECT_EQ(buffer1.Data(), nullptr);
  EXPECT_EQ(buffer4.Size(), 10);
  EXPECT_EQ(buffer4.Data(), buf1);

  RawBuffer buffer5(std::move(buffer4));
  // EXPECT_EQ(buffer4.Size(), 0);
  EXPECT_EQ(buffer4.Data(), nullptr);
  EXPECT_EQ(buffer5.Size(), 10);
  EXPECT_EQ(buffer5.Data(), buf1);

  // buf1 will be deleted when buffer5 is destroyed.
}

TEST(StringViewBuffer, BaseTest) {
  StringViewBuffer buffer(10);
  EXPECT_EQ(buffer.Size(), 10);
}

TEST(StringBuffer, BaseTest) {
  std::string buf1("test");
  StringBuffer buffer0(buf1);             // copy buf1 as the value to buffer0
  StringBuffer buffer1(std::move(buf1));  // move buf1 as the value to buffer1
  StringBuffer buffer3 = std::move(buffer1);
  EXPECT_STREQ(buffer3.Data(), "test");
  EXPECT_EQ(buffer3.Size(), 4);
}

}  // namespace mtcache

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
