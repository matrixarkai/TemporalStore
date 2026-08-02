// The abstract tests for an LRU cache.
//
// All LRU cache implementations are expected to pass these tests.
//

#pragma once

namespace mtcache {

template <class LruCacheType>
class LruCacheTest : public ::testing::Test {};

TYPED_TEST_SUITE_P(LruCacheTest);

TYPED_TEST_P(LruCacheTest, InOrderEviction) {
  TypeParam cache(4);
  cache.Insert("foo", "aa");
  cache.Insert("bar", "bb");
  cache.Insert("baz", "cc");
  cache.Insert("qux", "dd");
  EXPECT_EQ(cache.Size(), 4);
  cache.Insert("quux", "ee");
  EXPECT_EQ(cache.Size(), 4);

  EXPECT_FALSE(cache.Lookup("foo").has_value());
  ASSERT_TRUE(cache.Lookup("bar").has_value());
  EXPECT_EQ(cache.Lookup("bar"), "bb");
  ASSERT_TRUE(cache.Lookup("baz").has_value());
  EXPECT_EQ(cache.Lookup("baz"), "cc");
  ASSERT_TRUE(cache.Lookup("qux").has_value());
  EXPECT_EQ(cache.Lookup("qux"), "dd");
  ASSERT_TRUE(cache.Lookup("quux").has_value());
  EXPECT_EQ(cache.Lookup("quux"), "ee");
}

TYPED_TEST_P(LruCacheTest, Eviction) {
  TypeParam cache(4);
  cache.Insert("foo", "aa");
  cache.Insert("bar", "bb");
  cache.Insert("baz", "cc");
  cache.Insert("qux", "dd");
  EXPECT_EQ(cache.Size(), 4);
  // Lookup makes "foo" the MRU instead of LRU.
  cache.Lookup("foo");
  cache.Insert("quux", "ee");
  EXPECT_EQ(cache.Size(), 4);

  ASSERT_TRUE(cache.Lookup("foo").has_value());
  EXPECT_EQ(cache.Lookup("foo"), "aa");
  EXPECT_FALSE(cache.Lookup("bar").has_value());
  ASSERT_TRUE(cache.Lookup("baz").has_value());
  EXPECT_EQ(cache.Lookup("baz"), "cc");
  ASSERT_TRUE(cache.Lookup("qux").has_value());
  EXPECT_EQ(cache.Lookup("qux"), "dd");
  ASSERT_TRUE(cache.Lookup("quux").has_value());
  EXPECT_EQ(cache.Lookup("quux"), "ee");
}

TYPED_TEST_P(LruCacheTest, VariableSizeEviction) {
  TypeParam cache(10);
  cache.Insert("foo", "aa", 1);
  cache.Insert("bar", "bb", 2);
  cache.Insert("baz", "cc", 3);
  cache.Insert("qux", "dd", 4);
  ASSERT_EQ(cache.Size(), 10);

  // "foo", "bar", and "baz" must be evicted to make enough room for "quux".
  cache.Insert("quux", "ee", 5);
  EXPECT_EQ(cache.Size(), 9);
  EXPECT_FALSE(cache.Lookup("foo"));
  EXPECT_FALSE(cache.Lookup("bar"));
  EXPECT_FALSE(cache.Lookup("baz"));
  EXPECT_TRUE(cache.Lookup("qux"));
  EXPECT_TRUE(cache.Lookup("quux"));
}

REGISTER_TYPED_TEST_SUITE_P(LruCacheTest, InOrderEviction, Eviction,
                            VariableSizeEviction);

}  // namespace mtcache
