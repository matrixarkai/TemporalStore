#include "storage/simple_storage.h"

#include <gtest/gtest.h>

namespace mtcache {

TEST(StorageEngineSimple, PutGetAndDelete) {
  StorageEngineSimple engine;
  EXPECT_TRUE(engine.Start());
  // Put method test
  std::string data = "StorageEngineSimple";
  std::unique_ptr<folly::IOBuf> value = folly::IOBuf::copyBuffer(data);
  auto put_res = engine.Put("key", std::move(*value));
  ASSERT_TRUE(put_res.IsOk());
  auto buffer = std::move(put_res).Get();
  EXPECT_EQ(data.size(), buffer->Size());
  EXPECT_EQ(0, memcmp(data.c_str(), buffer->Data(), buffer->Size()));

  // Get method test
  auto get_res = engine.Get("key");
  EXPECT_FALSE(get_res.IsOk());
  EXPECT_EQ(get_res.GetError(), &Errors::kNotImplemented);
}

TEST(StorageEngineSimple, NotImplementedMethods) {
  StorageEngineSimple engine;

  auto reset_res = engine.Reset();
  EXPECT_TRUE(reset_res.IsOk());

  auto recover_res = engine.RecoverData(nullptr);
  EXPECT_FALSE(recover_res.IsOk());
  EXPECT_EQ(recover_res.GetError(), &Errors::kNotImplemented);
}

}  // namespace mtcache
