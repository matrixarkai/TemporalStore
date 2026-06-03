#include "map/cache_map.h"

#include "common/logging.h"

#include <gtest/gtest.h>

namespace mtcache {

TEST(CacheMapTest, MapTest) {
  ConcurrentHashMap<uint64_t, uint64_t> foomap(3);
  EXPECT_TRUE(foomap.empty());
  EXPECT_EQ(foomap.find(1), foomap.cend());
  auto r = foomap.insert(1, 0);
  EXPECT_TRUE(r.second);
  auto r2 = foomap.insert(1, 0);
  EXPECT_EQ(r.first->second, 0);
  EXPECT_EQ(r.first->first, 1);
  EXPECT_EQ(r2.first->second, 0);
  EXPECT_EQ(r2.first->first, 1);
  EXPECT_EQ(r.first, r2.first);
  EXPECT_TRUE(r.second);
  EXPECT_FALSE(r2.second);
  EXPECT_FALSE(foomap.empty());
  EXPECT_TRUE(static_cast<bool>(foomap.assign(1, 1)));
  auto r3 = foomap.find(1);
  EXPECT_NE(r3, foomap.cend());
  EXPECT_EQ(1, r3->second);
  EXPECT_TRUE(foomap.insert(std::make_pair(2, 0)).second);
  EXPECT_FALSE(foomap.insert_or_assign(2, 0).second);
  EXPECT_EQ(foomap.size(), 2);
  EXPECT_TRUE(foomap.assign_if_equal(2, 0, 3));
  auto r4 = foomap.find(2);
  EXPECT_NE(r4, foomap.cend());
  EXPECT_EQ(3, r4->second);
  EXPECT_TRUE(foomap.insert(3, 0).second);
  EXPECT_FALSE(foomap.erase_if_equal(3, 1));
  EXPECT_TRUE(foomap.erase_if_equal(3, 0));
  EXPECT_TRUE(foomap.insert(3, 0).second);
  EXPECT_NE(foomap.find(1), foomap.cend());
  EXPECT_NE(foomap.find(2), foomap.cend());
  EXPECT_EQ(foomap.find(2)->second, 3);
  EXPECT_EQ(foomap[2], 3);
  EXPECT_EQ(foomap[20], 0);
  EXPECT_EQ(foomap.at(20), 0);
  EXPECT_FALSE(foomap.insert(1, 0).second);
  auto l = foomap.find(1);
  foomap.erase(l);
  EXPECT_FALSE(foomap.erase(1));
  EXPECT_EQ(foomap.find(1), foomap.cend());
  auto res = foomap.find(2);
  EXPECT_NE(res, foomap.cend());
  EXPECT_EQ(3, res->second);
  EXPECT_FALSE(foomap.empty());
  foomap.clear();
  EXPECT_TRUE(foomap.empty());
}

TEST(CacheMapTest, MaxSizeTest) {
  ConcurrentHashMap<uint64_t, uint64_t> foomap(2, 16);
  bool insert_failed = false;
  for (int i = 0; i < 32; i++) {
    auto res = foomap.insert(0, 0);
    if (!res.second) {
      insert_failed = true;
    }
  }
  EXPECT_TRUE(insert_failed);
}

TEST(CacheMapTest, MoveTest) {
  ConcurrentHashMap<uint64_t, uint64_t> foomap(2, 16);
  auto other = std::move(foomap);
  auto other2 = std::move(other);
  other = std::move(other2);
}

struct foo {
  static int moved;
  static int copied;
  foo(foo&&) noexcept { moved++; }
  foo& operator=(foo&&) {
    moved++;
    return *this;
  }
  foo& operator=(const foo&) {
    copied++;
    return *this;
  }
  foo(const foo&) { copied++; }
  foo() {}
};
int foo::moved{0};
int foo::copied{0};

TEST(CacheMapTest, EmplaceTest) {
  ConcurrentHashMap<uint64_t, foo> foomap(200);
  foo bar;  // Make sure to test copy
  foomap.insert(1, bar);
  EXPECT_EQ(foo::moved, 0);
  EXPECT_EQ(foo::copied, 1);
  foo::copied = 0;
  // The difference between emplace and try_emplace:
  // If insertion fails, try_emplace does not move its argument
  foomap.try_emplace(1, foo());
  EXPECT_EQ(foo::moved, 0);
  EXPECT_EQ(foo::copied, 0);
  foomap.emplace(1, foo());
  EXPECT_EQ(foo::moved, 1);
  EXPECT_EQ(foo::copied, 0);
  // Reset the counters. Repeated tests are allowed
  foo::moved = 0;
  foo::copied = 0;
}

TEST(CacheMapTest, MapUpdateTest) {
  ConcurrentHashMap<uint64_t, uint64_t> foomap(2);
  EXPECT_TRUE(foomap.insert(1, 10).second);
  EXPECT_TRUE(static_cast<bool>(foomap.assign(1, 11)));
  auto res = foomap.find(1);
  EXPECT_NE(res, foomap.cend());
  EXPECT_EQ(11, res->second);
}

TEST(CacheMapTest, InsertAssignTest) {
  ConcurrentHashMap<uint64_t, uint64_t> foomap(2);
  // The first insert_or_assign should return true because an insert
  // OP is performed.
  auto pass1 = foomap.insert_or_assign(1, 10);
  EXPECT_TRUE(pass1.second);
  EXPECT_NE(pass1.first, foomap.cend());
  EXPECT_EQ(10, pass1.first->second);
  // The second insert_or_assign should return false because an assign
  // OP is performed.
  auto pass2 = foomap.insert_or_assign(1, 20);
  EXPECT_FALSE(pass2.second);
  EXPECT_NE(pass2.first, foomap.cend());
  // An iterator to the old value is returned, so the value should still
  // be 10.
  EXPECT_EQ(10, pass2.first->second);
  EXPECT_EQ(1, pass2.first->first);
  // The follow-up find should return the latest assigned value 20
  auto lookup_res = foomap.find(1);
  EXPECT_NE(lookup_res, foomap.cend());
  EXPECT_EQ(20, lookup_res->second);
}

TEST(CacheMapTest, MapInsertIteratorValueTest) {
  ConcurrentHashMap<uint64_t, uint64_t> foomap(2);
  for (uint64_t i = 0; i < 1 << 16; i++) {
    auto ret = foomap.insert(i, i + 1);
    EXPECT_TRUE(ret.second);
    EXPECT_EQ(ret.first->second, i + 1);
  }
}

TEST(CacheMapTest, MapResizeTest) {
  ConcurrentHashMap<uint64_t, uint64_t> foomap(2);
  EXPECT_EQ(foomap.find(1), foomap.cend());
  EXPECT_TRUE(foomap.insert(1, 0).second);
  EXPECT_TRUE(foomap.insert(2, 0).second);
  EXPECT_TRUE(foomap.insert(3, 0).second);
  EXPECT_TRUE(foomap.insert(4, 0).second);
  foomap.reserve(512);
  EXPECT_NE(foomap.find(1), foomap.cend());
  EXPECT_NE(foomap.find(2), foomap.cend());
  EXPECT_FALSE(foomap.insert(1, 0).second);
  EXPECT_TRUE(foomap.erase(1));
  EXPECT_EQ(foomap.find(1), foomap.cend());
  auto res = foomap.find(2);
  EXPECT_NE(res, foomap.cend());
  if (res != foomap.cend()) {
    EXPECT_EQ(0, res->second);
  }
  foomap.reserve(0);
}

TEST(CacheMapTest, ReserveTest) {
  ConcurrentHashMap<uint64_t, uint64_t> foomap;
  int64_t insert_count = 0;
  int64_t actual_count = 0;

  for (int64_t i = 0; i < 1000; i++) {
    if (i % 100 == 0) {
      foomap.reserve(i + 100);
    }
    foomap.insert(std::make_pair(i, i));
    insert_count++;
  }

  for (auto it = foomap.begin(); it != foomap.cend(); ++it) {
    actual_count++;
  }

  EXPECT_EQ(insert_count, actual_count);
}

TEST(CacheMapTest, MapIterateTest2) {
  ConcurrentHashMap<uint64_t, uint64_t> foomap(2);
  auto begin = foomap.cbegin();
  auto end = foomap.cend();
  EXPECT_EQ(begin, end);
}

TEST(CacheMapTest, MapIterateTest) {
  ConcurrentHashMap<uint64_t, uint64_t> foomap(2);
  EXPECT_EQ(foomap.cbegin(), foomap.cend());
  EXPECT_TRUE(foomap.insert(1, 1).second);
  EXPECT_TRUE(foomap.insert(2, 2).second);
  auto iter = foomap.cbegin();
  EXPECT_NE(iter, foomap.cend());
  EXPECT_EQ(iter->first, 1);
  EXPECT_EQ(iter->second, 1);
  ++iter;
  EXPECT_NE(iter, foomap.cend());
  EXPECT_EQ(iter->first, 2);
  EXPECT_EQ(iter->second, 2);
  ++iter;
  EXPECT_EQ(iter, foomap.cend());

  int count = 0;
  for (auto it = foomap.cbegin(); it != foomap.cend(); ++it) {
    count++;
  }
  EXPECT_EQ(count, 2);
}

TEST(CacheMapTest, MoveIterateAssignIterate) {
  using Map = ConcurrentHashMap<int, int>;
  Map tmp;
  Map map{std::move(tmp)};

  map.insert(0, 0);
  ++map.cbegin();
  ConcurrentHashMap<int, int> other;
  other.insert(0, 0);
  map = std::move(other);
  ++map.cbegin();
}

TEST(CacheMapTest, EraseTest) {
  ConcurrentHashMap<uint64_t, uint64_t> foomap(3);
  foomap.insert(1, 0);
  auto f1 = foomap.find(1);
  EXPECT_EQ(1, foomap.erase(1));
  foomap.erase(f1);
}

TEST(CacheMapTest, EraseIfEqualTest) {
  ConcurrentHashMap<uint64_t, uint64_t> foomap(3);
  foomap.insert(1, 0);
  EXPECT_FALSE(foomap.erase_if_equal(1, 1));
  auto f1 = foomap.find(1);
  EXPECT_EQ(0, f1->second);
  EXPECT_TRUE(foomap.erase_if_equal(1, 0));
  EXPECT_EQ(foomap.find(1), foomap.cend());
}

TEST(CacheMapTest, EraseIfTest) {
  ConcurrentHashMap<uint64_t, uint64_t> foomap(3);
  foomap.insert(1, 0);
  EXPECT_FALSE(
      foomap.erase_key_if(1, [](const uint64_t& value) { return value == 1; }));
  auto f1 = foomap.find(1);
  EXPECT_EQ(0, f1->second);
  EXPECT_TRUE(
      foomap.erase_key_if(1, [](const uint64_t& value) { return value == 0; }));
  EXPECT_EQ(foomap.find(1), foomap.cend());

  ConcurrentHashMap<std::string, std::weak_ptr<uint64_t>> barmap(3);
  auto shared = std::make_shared<uint64_t>(123);
  barmap.insert("test", shared);
  EXPECT_FALSE(barmap.erase_key_if(
      "test",
      [](const std::weak_ptr<uint64_t>& ptr) { return ptr.expired(); }));
  EXPECT_EQ(*barmap.find("test")->second.lock(), 123);
  shared.reset();
  EXPECT_TRUE(barmap.erase_key_if(
      "test",
      [](const std::weak_ptr<uint64_t>& ptr) { return ptr.expired(); }));
  EXPECT_EQ(barmap.find("test"), barmap.cend());
}

TEST(CacheMapTest, CopyIterator) {
  ConcurrentHashMap<int, int> map;
  map.insert(0, 0);
  for (auto cit = map.cbegin(); cit != map.cend(); ++cit) {
    std::pair<int const, int> const ckv{0, 0};
    EXPECT_EQ(*cit, ckv);
  }
}

TEST(CacheMapTest, EraseInIterateTest) {
  ConcurrentHashMap<uint64_t, uint64_t> foomap(3);
  for (uint64_t k = 0; k < 10; ++k) {
    foomap.insert(k, k);
  }
  EXPECT_EQ(10, foomap.size());
  for (auto it = foomap.cbegin(); it != foomap.cend();) {
    if (it->second > 3) {
      it = foomap.erase(it);
    } else {
      ++it;
    }
  }
  EXPECT_EQ(4, foomap.size());
  for (auto it = foomap.cbegin(); it != foomap.cend(); ++it) {
    EXPECT_GE(3, it->second);
  }
}

TEST(CacheMapTest, insertStressTest) {
  unsigned size = 4;
  unsigned iters = size * 64 * 4 * 100;
  ConcurrentHashMap<uint64_t, uint64_t, std::hash<uint64_t>,
                    std::equal_to<uint64_t>, std::allocator<uint8_t>, 8,
                    std::atomic, std::mutex>
      m(4);

  EXPECT_TRUE(m.insert(0, 0).second);
  EXPECT_FALSE(m.insert(0, 0).second);
  std::vector<std::thread> threads;
  unsigned int num_threads = 64;
  for (uint32_t t = 0; t < num_threads; t++) {
    threads.push_back(std::thread([&, t]() {
      int offset = (iters * t / num_threads);
      for (uint32_t i = 0; i < iters / num_threads; i++) {
        auto var = offset + i + 1;
        m.map_lock(var);
        EXPECT_TRUE(m.insert(var, var).second);
        m.map_unlock(var);
        m.map_lock(0);
        EXPECT_FALSE(m.insert(0, 0).second);
        m.map_unlock(0);
      }
    }));
  }
  for (auto& t : threads) {
    t.join();
  }
}

TEST(CacheMapTest, TryEmplaceEraseStressTest) {
  std::atomic<int> iterations{10000};
  std::vector<std::thread> threads;
  unsigned int num_threads = 32;
  threads.reserve(num_threads);
  ConcurrentHashMap<int, int> map;
  for (uint32_t t = 0; t < num_threads; t++) {
    threads.push_back(std::thread([&]() {
      while (--iterations >= 0) {
        map.map_lock(1);
        auto it = map.try_emplace(1, 101);
        map.map_unlock(1);
        map.map_lock(1);
        map.erase(1);
        map.map_unlock(1);
      }
    }));
  }
  for (auto& t : threads) {
    t.join();
  }
}

TEST(CacheMapTest, EraseStressTest) {
  unsigned size = 2;
  unsigned iters = size * 128 * 2;
  ConcurrentHashMap<uint64_t, uint64_t, std::hash<uint64_t>,
                    std::equal_to<uint64_t>, std::allocator<uint8_t>, 8,
                    std::atomic, std::mutex>
      m(2);

  for (uint32_t i = 0; i < size; i++) {
    uint64_t k = ::folly::hash::jenkins_rev_mix32(i);
    bool map_lock = false;
    while (!(map_lock = m.map_trylock(k))) {
      // retry
      usleep(10 * 1000);
    }
    m.insert(k, k);
    if (map_lock) {
      m.map_unlock(k);
    }
  }
  std::vector<std::thread> threads;
  unsigned int num_threads = 32;
  threads.reserve(num_threads);
  for (uint32_t t = 0; t < num_threads; t++) {
    threads.push_back(std::thread([&, t]() {
      int offset = (iters * t / num_threads);
      for (uint32_t i = 0; i < iters / num_threads; i++) {
        uint64_t k = ::folly::hash::jenkins_rev_mix32((i + offset));
        bool map_lock = false;
        while (!(map_lock = m.map_trylock(k))) {
          // retry
          usleep(10 * 1000);
        }
        auto res = m.insert(k, k).second;
        if (map_lock) {
          m.map_unlock(k);
        }
        map_lock = false;
        if (res) {
          if (i % 2 == 0) {
            m.map_lock(k);
            res = m.erase(k);
            m.map_unlock(k);
          } else {
            m.map_lock(k);
            res = m.erase_if_equal(k, k);
            m.map_unlock(k);
          }
          if (!res) {
            FAIL() << "Faulre to erase thread " << t << " val " << k;
          }
          EXPECT_TRUE(res);
        }
        map_lock = false;
        while (!(map_lock = m.map_trylock(k))) {
          // retry
          usleep(10 * 1000);
        }
        res = m.insert(k, k).second;
        if (map_lock) {
          m.map_unlock(k);
        }
        if (res) {
          map_lock = false;
          while (!(map_lock = m.map_trylock(k))) {
            // retry
            usleep(10 * 1000);
          }
          res = static_cast<bool>(m.assign(k, k));
          if (map_lock) {
            m.map_unlock(k);
          }
          if (!res) {
            FAIL() << "Thread " << t << " update fail " << k << " res" << res;
          }
          EXPECT_TRUE(res);
          auto result = m.find(k);
          if (result == m.cend()) {
            FAIL() << "Thread " << t << " lookup fail " << k;
          }
          EXPECT_EQ(k, result->second);
        }
      }
    }));
  }
  for (auto& t : threads) {
    t.join();
  }
}

TEST(CacheMapTest, UpdateStressTest) {
  // size must match iters for this test.
  unsigned size = 128 * 128;
  unsigned iters = size;
  ConcurrentHashMap<uint64_t, uint64_t, std::hash<uint64_t>,
                    std::equal_to<uint64_t>, std::allocator<uint8_t>, 8,
                    std::atomic, std::mutex>
      m(2);

  // single thread, no need to lock
  for (uint32_t i = 0; i < size; i++) {
    m.insert(i, i);
  }
  std::vector<std::thread> threads;
  unsigned int num_threads = 32;
  threads.reserve(num_threads);
  for (uint32_t t = 0; t < num_threads; t++) {
    threads.push_back(std::thread([&, t]() {
      int offset = (iters * t / num_threads);
      for (uint32_t i = 0; i < iters / num_threads; i++) {
        uint64_t k = ::folly::hash::jenkins_rev_mix32((i + offset));
        k = k % (iters / num_threads) + offset;
        uint64_t val = 3;
        {
          // find, no need to lock
          auto res = m.find(k);
          EXPECT_NE(res, m.cend());
          EXPECT_EQ(k, res->second);
          bool map_lock = false;
          while (!(map_lock = m.map_trylock(k))) {
            // retry
            usleep(10 * 1000);
          }
          auto r = m.assign(k, res->second);
          if (map_lock) {
            m.map_unlock(k);
          }
          EXPECT_TRUE(r);
        }
        {
          auto res = m.find(k);
          EXPECT_NE(res, m.cend());
          EXPECT_EQ(k, res->second);
        }
        // Another random insertion to force table resizes
        val = size + i + offset;
        bool map_lock = false;
        while (!(map_lock = m.map_trylock(val))) {
          // retry
          usleep(10 * 1000);
        }
        EXPECT_TRUE(m.insert(val, val).second);
        if (map_lock) {
          m.map_unlock(val);
        }
      }
    }));
  }
  for (auto& t : threads) {
    t.join();
  }
}

TEST(CacheMapTest, IterateStressTest) {
  unsigned size = 2;
  unsigned iters = size * 128 * 2;
  ConcurrentHashMap<uint64_t, uint64_t, std::hash<uint64_t>,
                    std::equal_to<uint64_t>, std::allocator<uint8_t>, 8,
                    std::atomic, std::mutex>
      m(2);

  for (uint32_t i = 0; i < size; i++) {
    uint64_t k = folly::hash::jenkins_rev_mix32(i);
    m.insert(k, k);
  }
  for (uint32_t i = 0; i < 10; i++) {
    m.insert(i, i);
  }
  std::vector<std::thread> threads;
  unsigned int num_threads = 32;
  for (uint32_t t = 0; t < num_threads; t++) {
    threads.push_back(std::thread([&, t]() {
      int offset = (iters * t / num_threads);
      for (uint32_t i = 0; i < iters / num_threads; i++) {
        uint64_t k = folly::hash::jenkins_rev_mix32((i + offset));

        bool map_lock = false;
        while (!(map_lock = m.map_trylock(k))) {
          // retry
          usleep(10 * 1000);
        }

        auto res = m.insert(k, k).second;
        if (map_lock) {
          m.map_unlock(k);
        }

        if (res) {
          if (i % 2 == 0) {
            m.map_lock(k);
            res = m.erase(k);
            m.map_unlock(k);
          } else {
            m.map_lock(k);
            res = m.erase_if_equal(k, k);
            m.map_unlock(k);
          }
          if (!res) {
            FAIL() << "Faulre to erase thread " << t << " val " << k;
          }
          EXPECT_TRUE(res);
        }
        int count = 0;
        for (auto it = m.cbegin(); it != m.cend(); ++it) {
          VLOG(1) << "Item is " << it->first;
          if (it->first < 10) {
            count++;
          }
        }
        EXPECT_EQ(count, 10);
      }
    }));
  }
  for (auto& t : threads) {
    t.join();
  }
}

TEST(CacheMapTest, assignStressTest) {
  unsigned size = 2;
  unsigned iters = size * 64 * 4;
  struct big_value {
    uint64_t v1;
    uint64_t v2;
    uint64_t v3;
    uint64_t v4;
    uint64_t v5;
    uint64_t v6;
    uint64_t v7;
    uint64_t v8;
    void set(uint64_t v) { v1 = v2 = v3 = v4 = v5 = v6 = v7 = v8 = v; }
    void check() const {
      auto v = v1;
      EXPECT_EQ(v, v8);
      EXPECT_EQ(v, v7);
      EXPECT_EQ(v, v6);
      EXPECT_EQ(v, v5);
      EXPECT_EQ(v, v4);
      EXPECT_EQ(v, v3);
      EXPECT_EQ(v, v2);
    }
  };
  ConcurrentHashMap<uint64_t, big_value, std::hash<uint64_t>,
                    std::equal_to<uint64_t>, std::allocator<uint8_t>, 8,
                    std::atomic, std::mutex>
      m(2);

  for (uint32_t i = 0; i < iters; i++) {
    big_value a;
    a.set(i);
    m.insert(i, a);
  }

  std::vector<std::thread> threads;
  unsigned int num_threads = 32;
  for (uint32_t t = 0; t < num_threads; t++) {
    threads.push_back(std::thread([&]() {
      for (uint32_t i = 0; i < iters; i++) {
        auto res = m.find(i);
        EXPECT_NE(res, m.cend());
        res->second.check();
        big_value b;
        b.set(res->second.v1 + 1);
        bool map_lock = false;
        while (!(map_lock = m.map_trylock(i))) {
          // retry
          usleep(10 * 1000);
        }
        m.assign(i, b);
        if (map_lock) {
          m.map_unlock(i);
        }
      }
    }));
  }
  for (auto& t : threads) {
    t.join();
  }
}

TEST(CacheMapTest, RefcountTest) {
  struct badhash {
    size_t operator()(uint64_t) const { return 0; }
  };
  ConcurrentHashMap<uint64_t, uint64_t, badhash, std::equal_to<uint64_t>,
                    std::allocator<uint8_t>, 0>
      foomap(3);
  foomap.insert(0, 0);
  foomap.insert(1, 1);
  foomap.insert(2, 2);
  for (int32_t i = 0; i < 300; ++i) {
    foomap.insert_or_assign(1, i);
  }
}

struct Wrapper {
  explicit Wrapper(bool& del_) : del(del_) {}
  ~Wrapper() { del = true; }

  bool& del;
};

TEST(CacheMapTest, Deletion) {
  bool del{false};

  {
    ConcurrentHashMap<int, std::shared_ptr<Wrapper>> map;

    map.insert(0, std::make_shared<Wrapper>(del));
  }

  folly::hazptr_cleanup();

  EXPECT_TRUE(del);
}

TEST(CacheMapTest, DeletionWithErase) {
  bool del{false};

  {
    ConcurrentHashMap<int, std::shared_ptr<Wrapper>> map;

    map.insert(0, std::make_shared<Wrapper>(del));
    map.erase(0);
  }

  folly::hazptr_cleanup();

  EXPECT_TRUE(del);
}

TEST(CacheMapTest, DeletionWithIterator) {
  bool del{false};

  {
    ConcurrentHashMap<int, std::shared_ptr<Wrapper>> map;

    map.insert(0, std::make_shared<Wrapper>(del));
    auto it = map.find(0);
    map.erase(it);
  }

  folly::hazptr_cleanup();

  EXPECT_TRUE(del);
}

TEST(CacheMapTest, DeletionWithForLoop) {
  bool del{false};

  {
    ConcurrentHashMap<int, std::shared_ptr<Wrapper>> map;

    map.insert(0, std::make_shared<Wrapper>(del));
    for (auto it = map.cbegin(); it != map.cend(); ++it) {
      EXPECT_EQ(it->first, 0);
    }
  }

  folly::hazptr_cleanup();

  EXPECT_TRUE(del);
}

TEST(CacheMapTest, DeletionMultiple) {
  bool del1{false}, del2{false};

  {
    ConcurrentHashMap<int, std::shared_ptr<Wrapper>> map;

    map.insert(0, std::make_shared<Wrapper>(del1));
    map.insert(1, std::make_shared<Wrapper>(del2));
  }

  folly::hazptr_cleanup();

  EXPECT_TRUE(del1);
  EXPECT_TRUE(del2);
}

TEST(CacheMapTest, DeletionAssigned) {
  bool del1{false}, del2{false};

  {
    ConcurrentHashMap<int, std::shared_ptr<Wrapper>> map;

    map.insert(0, std::make_shared<Wrapper>(del1));
    map.insert_or_assign(0, std::make_shared<Wrapper>(del2));
  }

  folly::hazptr_cleanup();

  EXPECT_TRUE(del1);
  EXPECT_TRUE(del2);
}

TEST(CacheMapTest, DeletionMultipleMaps) {
  bool del1{false}, del2{false};

  {
    ConcurrentHashMap<int, std::shared_ptr<Wrapper>> map1;
    ConcurrentHashMap<int, std::shared_ptr<Wrapper>> map2;

    map1.insert(0, std::make_shared<Wrapper>(del1));
    map2.insert(0, std::make_shared<Wrapper>(del2));
  }

  folly::hazptr_cleanup();

  EXPECT_TRUE(del1);
  EXPECT_TRUE(del2);
}

TEST(CacheMapTest, ForEachLoop) {
  ConcurrentHashMap<int, int> map;
  map.insert(1, 2);
  size_t iters = 0;
  for (const auto& kv : map) {
    EXPECT_EQ(kv.first, 1);
    EXPECT_EQ(kv.second, 2);
    ++iters;
  }
  EXPECT_EQ(iters, 1);
}

template <typename T>
struct FooBase {
  typename T::ConstIterator it;
  explicit FooBase(typename T::ConstIterator&& it_) : it(std::move(it_)) {}
  FooBase(FooBase&&) = default;
  FooBase& operator=(FooBase&&) = default;
};

TEST(CacheMapTest, IteratorMove) {
  using Foo = FooBase<ConcurrentHashMap<int, int>>;
  ConcurrentHashMap<int, int> map;
  int k = 111;
  int v = 999999;
  map.insert(k, v);
  Foo foo(map.find(k));
  ASSERT_EQ(foo.it->second, v);
  Foo foo2(map.find(0));
  foo2 = std::move(foo);
  ASSERT_EQ(foo2.it->second, v);
}

TEST(CacheMapTest, IteratorLoop) {
  ConcurrentHashMap<std::string, int> map;
  static constexpr size_t kNum = 4000;
  for (size_t i = 0; i < kNum; ++i) {
    map.insert(folly::to<std::string>(i), i);
  }

  size_t count = 0;
  for (auto it = map.begin(); it != map.end(); ++it) {
    ASSERT_LT(count, kNum);
    ++count;
  }

  EXPECT_EQ(count, kNum);
}

}  // namespace mtcache

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
