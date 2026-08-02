#include "simple_lru_cache.h"

#include "common/logging.h"
#include "test/cache_test.h"
#include "test/lru_cache_test.h"

#include <gtest/gtest.h>

namespace mtcache {

namespace {

const size_t kCapacity = 5;

}  // namespace

using SimpleLRUCacheType =
    ::testing::Types<SimpleLRUCache<std::string, std::string>>;
INSTANTIATE_TYPED_TEST_SUITE_P(SimpleLRUCache, CacheTest, SimpleLRUCacheType);
INSTANTIATE_TYPED_TEST_SUITE_P(SimpleLRUCache, LruCacheTest,
                               SimpleLRUCacheType);

using ZeroCopySimpleLRUCacheType =
    ::testing::Types<ZeroCopySimpleLRUCache<std::string, std::string>>;
INSTANTIATE_TYPED_TEST_SUITE_P(ZeroCopySimpleLRUCache, CacheTest,
                               ZeroCopySimpleLRUCacheType);
INSTANTIATE_TYPED_TEST_SUITE_P(ZeroCopySimpleLRUCache, LruCacheTest,
                               ZeroCopySimpleLRUCacheType);

TEST(ZeroCopySimpleLRUCache, HandleValidAfterRemove) {
  ZeroCopySimpleLRUCache<std::string, std::string> cache(kCapacity);
  cache.Insert("foo", "aa");
  cache.Insert("bar", "bb");
  cache.Insert("baz", "cc");
  EXPECT_EQ(cache.Size(), 3);
  auto foo = cache.Acquire("foo");
  ASSERT_NE(foo, nullptr);
  EXPECT_EQ(foo->key(), "foo");
  EXPECT_EQ(foo->value(), "aa");
  auto bar = cache.Acquire("bar");
  ASSERT_NE(bar, nullptr);
  EXPECT_EQ(bar->key(), "bar");
  EXPECT_EQ(bar->value(), "bb");

  // Now remove "foo" from the cache, which makes it disappear from further
  // lookups.
  cache.Remove("foo");
  EXPECT_FALSE(cache.Lookup("foo"));
  EXPECT_EQ(cache.Acquire("foo"), nullptr);
  // The cache size remains at 3, since the removed cache entry is pinned.
  EXPECT_EQ(cache.Size(), 3);
  // The outstanding handle should remain valid for reference.
  EXPECT_EQ(foo->key(), "foo");
  EXPECT_EQ(foo->value(), "aa");

  // Releasing the pin discards the removed cache entry, and reduces the cache
  // size by 1.
  cache.Release(foo);
  EXPECT_EQ(cache.Size(), 2);

  // Similarly after RemoveAll, the outstanding handle for "bar" remains valid.
  cache.RemoveAll();
  EXPECT_FALSE(cache.Lookup("bar"));
  EXPECT_EQ(cache.Acquire("bar"), nullptr);
  EXPECT_EQ(cache.Size(), 1);
  EXPECT_EQ(bar->key(), "bar");
  EXPECT_EQ(bar->value(), "bb");
  cache.Release(bar);
  EXPECT_EQ(cache.Size(), 0);
}

TEST(ZeroCopySimpleLRUCache, AcquireNotEvictedUntilRelease) {
  ZeroCopySimpleLRUCache<std::string, std::string> cache(4);

  // Place two pins on "foo".
  cache.Insert("foo", "aa");
  auto handle1 = cache.Acquire("foo");
  ASSERT_NE(handle1, nullptr);
  auto handle2 = cache.Acquire("foo");
  ASSERT_NE(handle2, nullptr);

  // "foo" is not evicted due to the outstanding pins.
  cache.Insert("bar", "bb");
  cache.Insert("baz", "cc");
  cache.Insert("qux", "dd");
  cache.Insert("quux", "ee");
  EXPECT_EQ(cache.Size(), 4);
  ASSERT_TRUE(cache.Lookup("foo"));
  EXPECT_EQ(cache.Lookup("foo"), "aa");
  // "bar" is evicted instead.
  EXPECT_FALSE(cache.Lookup("bar"));
  ASSERT_TRUE(cache.Lookup("baz"));
  EXPECT_EQ(cache.Lookup("baz"), "cc");
  ASSERT_TRUE(cache.Lookup("qux"));
  EXPECT_EQ(cache.Lookup("qux"), "dd");
  ASSERT_TRUE(cache.Lookup("quux"));
  EXPECT_EQ(cache.Lookup("quux"), "ee");

  // Releasing handle1 does not affect handle2.
  cache.Release(handle1);
  EXPECT_EQ(handle2->key(), "foo");
  EXPECT_EQ(handle2->value(), "aa");

  // With one pin remaining, "foo" is still ineligible for eviction.
  cache.Insert("bar", "bb");
  EXPECT_EQ(cache.Size(), 4);
  ASSERT_TRUE(cache.Lookup("foo"));
  EXPECT_EQ(cache.Lookup("foo"), "aa");
  // "baz" is evicted instead.
  EXPECT_FALSE(cache.Lookup("baz"));
  ASSERT_TRUE(cache.Lookup("qux"));
  EXPECT_EQ(cache.Lookup("qux"), "dd");
  ASSERT_TRUE(cache.Lookup("quux"));
  EXPECT_EQ(cache.Lookup("quux"), "ee");
  ASSERT_TRUE(cache.Lookup("bar"));
  EXPECT_EQ(cache.Lookup("bar"), "bb");

  // Release the last remaining pin on "foo" makes it eligible for eviction.
  // NOTE: the API does not dictates how soon a newly released cache entry
  // will be evicted, so we fill the cache with enough entries to flush it out.
  cache.Release(handle2);
  cache.Insert("bar", "bb");
  cache.Insert("baz", "cc");
  cache.Insert("qux", "dd");
  cache.Insert("quux", "ee");
  EXPECT_EQ(cache.Size(), 4);
  EXPECT_FALSE(cache.Lookup("foo"));
  EXPECT_EQ(cache.Acquire("foo"), nullptr);
  EXPECT_TRUE(cache.Lookup("bar"));
  EXPECT_EQ(cache.Lookup("bar"), "bb");
  ASSERT_TRUE(cache.Lookup("baz"));
  EXPECT_EQ(cache.Lookup("baz"), "cc");
  ASSERT_TRUE(cache.Lookup("qux"));
  EXPECT_EQ(cache.Lookup("qux"), "dd");
  ASSERT_TRUE(cache.Lookup("quux"));
  EXPECT_EQ(cache.Lookup("quux"), "ee");
}

TEST(ZeroCopySimpleLRUCache, ScopedLookup) {
  ZeroCopySimpleLRUCache<std::string, std::string> cache(4);

  {
    cache.Insert("foo", "aa");
    ZeroCopySimpleLRUCache<std::string, std::string>::ScopedLookup lookup(
        &cache, "foo");
    EXPECT_TRUE(lookup.Found());
    EXPECT_EQ(lookup.key(), "foo");
    EXPECT_EQ(lookup.value(), "aa");

    // "foo" is not evicted due to the outstanding pin.
    cache.Insert("bar", "bb");
    cache.Insert("baz", "cc");
    cache.Insert("qux", "dd");
    cache.Insert("quux", "ee");

    EXPECT_EQ(cache.Size(), 4);
    ASSERT_TRUE(cache.Lookup("foo"));
    EXPECT_EQ(cache.Lookup("foo"), "aa");
    EXPECT_TRUE(lookup.Found());
    EXPECT_EQ(lookup.key(), "foo");
    EXPECT_EQ(lookup.value(), "aa");
  }

  // "lookup" is now out of scope, so "foo" becomes eligible for eviction.
  cache.Insert("bar", "bb");
  cache.Insert("baz", "cc");
  cache.Insert("qux", "dd");
  cache.Insert("quux", "ee");
  EXPECT_EQ(cache.Size(), 4);
  ZeroCopySimpleLRUCache<std::string, std::string>::ScopedLookup lookup2(&cache,
                                                                         "foo");
  EXPECT_FALSE(lookup2.Found());
}

TEST(ZeroCopySimpleLRUCache, InsertPinnedGrowSizeOverCapacity) {
  ZeroCopySimpleLRUCache<std::string, std::string> cache(2);
  auto foo = cache.InsertPinned("foo", "aa");
  EXPECT_NE(foo, nullptr);
  auto bar = cache.InsertPinned("bar", "bb");
  EXPECT_NE(bar, nullptr);
  auto baz = cache.InsertPinned("baz", "cc");
  EXPECT_NE(baz, nullptr);
  EXPECT_EQ(cache.Size(), 3);

  // With cache size over capacity due to pinned cache entries, any cache
  // entry will be discard as soon as is released.
  cache.Release(baz);
  EXPECT_EQ(cache.Size(), 2);
  EXPECT_TRUE(cache.Lookup("foo"));
  EXPECT_TRUE(cache.Lookup("bar"));
  EXPECT_FALSE(cache.Lookup("baz"));

  // Clean up
  cache.Release(foo);
  cache.Release(bar);
}

TEST(ZeroCopySimpleLRUCache, SetCapacityWithPinned) {
  ZeroCopySimpleLRUCache<std::string, std::string> cache(kCapacity);
  auto foo = cache.InsertPinned("foo", "aa");
  EXPECT_NE(foo, nullptr);
  auto bar = cache.InsertPinned("bar", "bb");
  EXPECT_NE(bar, nullptr);
  cache.Insert("baz", "cc");
  cache.Insert("qux", "dd");
  EXPECT_EQ(cache.Size(), 4);

  // Unpinned cache entries are evicted to free up space.
  cache.SetCapacity(3);
  EXPECT_EQ(cache.Size(), 3);
  EXPECT_FALSE(cache.Lookup("baz"));

  // Pinned cache entries cannot be evicted, so the cache size could end up
  // greater than capacity.
  cache.SetCapacity(1);
  EXPECT_EQ(cache.Size(), 2);
  EXPECT_FALSE(cache.Lookup("qux"));

  // With cache size over capacity due to pinned cache entries, any cache
  // entry will be discard as soon as is released.
  cache.Release(bar);
  EXPECT_EQ(cache.Size(), 1);
  EXPECT_TRUE(cache.Lookup("foo"));
  EXPECT_FALSE(cache.Lookup("bar"));

  // Clean up.
  cache.Release(foo);
}

}  // namespace mtcache

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
