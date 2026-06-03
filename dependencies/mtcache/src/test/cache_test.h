// The abstract tests for the generic Cache API.
//
// All implementations of the Cache interface are expected to pass these tests.
//

#pragma once

#include <gtest/gtest.h>

namespace mtcache {

template <class CacheType>
class CacheTest : public ::testing::Test {
 protected:
  static const size_t kCapacity = 5;
};

/*static*/
template <class CacheType>
const size_t CacheTest<CacheType>::kCapacity;

TYPED_TEST_SUITE_P(CacheTest);

TYPED_TEST_P(CacheTest, LookupNonexistent) {
  TypeParam cache(TestFixture::kCapacity);
  EXPECT_EQ(cache.Capacity(), TestFixture::kCapacity);
  EXPECT_EQ(cache.Size(), 0);
  EXPECT_FALSE(cache.Lookup("none").has_value());
}

TYPED_TEST_P(CacheTest, Lookup) {
  TypeParam cache(TestFixture::kCapacity);
  cache.Insert("foo", "aa");
  EXPECT_EQ(cache.Size(), 1);
  cache.Insert("bar", "bb", 2);
  EXPECT_EQ(cache.Size(), 3);

  auto foo = cache.Lookup("foo");
  EXPECT_TRUE(foo.has_value());
  EXPECT_EQ(foo.value(), "aa");
  auto bar = cache.Lookup("bar");
  EXPECT_TRUE(bar.has_value());
  EXPECT_EQ(bar.value(), "bb");
  EXPECT_FALSE(cache.Lookup("baz").has_value());
  EXPECT_EQ(cache.Size(), 3);
}

TYPED_TEST_P(CacheTest, Replacement) {
  TypeParam cache(TestFixture::kCapacity);
  cache.Insert("foo", "aa");
  cache.Insert("bar", "bb", 2);
  EXPECT_EQ(cache.Size(), 3);

  auto foo = cache.Lookup("foo");
  ASSERT_TRUE(foo.has_value());
  EXPECT_EQ(foo, "aa");

  cache.Insert("foo", "aaa", 3);
  foo = cache.Lookup("foo");
  ASSERT_TRUE(foo.has_value());
  EXPECT_EQ(foo, "aaa");
  EXPECT_EQ(cache.Size(), 5);

  cache.Insert("bar", "bbb", 1);
  auto bar = cache.Lookup("bar");
  ASSERT_TRUE(bar);
  EXPECT_EQ(bar, "bbb");
  EXPECT_EQ(cache.Size(), 4);
}

TYPED_TEST_P(CacheTest, Remove) {
  TypeParam cache(TestFixture::kCapacity);
  cache.Insert("foo", "aa");
  cache.Insert("bar", "bb");
  cache.Insert("baz", "cc", 2);
  ASSERT_EQ(cache.Size(), 4);

  ASSERT_TRUE(cache.Lookup("foo").has_value());
  cache.Remove("foo");
  EXPECT_FALSE(cache.Lookup("foo").has_value());
  EXPECT_EQ(cache.Size(), 3);

  ASSERT_TRUE(cache.Lookup("baz").has_value());
  cache.Remove("baz");
  EXPECT_FALSE(cache.Lookup("baz").has_value());
  EXPECT_EQ(cache.Size(), 1);
}

TYPED_TEST_P(CacheTest, RemoveAll) {
  TypeParam cache(TestFixture::kCapacity);
  cache.Insert("foo", "aa");
  cache.Insert("bar", "bb");
  ASSERT_TRUE(cache.Lookup("foo").has_value());
  ASSERT_TRUE(cache.Lookup("bar").has_value());
  EXPECT_EQ(cache.Size(), 2);

  cache.RemoveAll();
  EXPECT_FALSE(cache.Lookup("foo").has_value());
  EXPECT_FALSE(cache.Lookup("bar").has_value());
  EXPECT_EQ(cache.Size(), 0);
}

TYPED_TEST_P(CacheTest, SetCapacity) {
  TypeParam cache(TestFixture::kCapacity);
  cache.Insert("foo", "aa");
  cache.Insert("bar", "bb");
  cache.Insert("baz", "cc");
  EXPECT_EQ(cache.Size(), 3);

  cache.SetCapacity(4);
  EXPECT_EQ(cache.Size(), 3);
  cache.SetCapacity(2);
  EXPECT_EQ(cache.Size(), 2);
}

REGISTER_TYPED_TEST_SUITE_P(CacheTest, LookupNonexistent, Lookup, Replacement,
                            Remove, RemoveAll, SetCapacity);

}  // namespace mtcache
