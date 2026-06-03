#pragma once

#include "common/logging.h"
#include "mtcache.h"

#include <list>
#include <memory>
#include <string>
#include <unordered_map>

namespace mtcache {

// A simple LRU cache implemented using a hash map and a double-linked list.
//
// NOTE: this implementation of the cache interface is mostly illustrative
// and not performance-oriented. It is intended for use mainly in unit tests
// to codify the cache API behavior, or as a stub Cache implementation to
// test an application-specific cache API (e.g., the file data cache).
template <class Key, class Value>
class SimpleLRUCache : public Cache<Key, Value> {
 public:
  explicit SimpleLRUCache(size_t capacity);

  // The Cache interface.
  bool Start() override { return true; }
  bool Stop() override { return true; }
  void Insert(const Key& key, Value value, size_t size = 1) override;
  std::optional<Value> Lookup(const Key& key) override;
  void Remove(const Key& key) override;
  void RemoveAll() override;
  size_t Capacity() const override { return capacity_; }
  void SetCapacity(size_t capacity) override;
  size_t Size() const override { return size_; };

 private:
  struct Elem {
    Elem(Value p_v, size_t p_size) : value(std::move(p_v)), size(p_size) {}

    Value value;
    size_t size;
  };
  using KVPair = std::pair<Key, Elem>;
  // NOTE: we store the values in the list instead of the map, because
  // iterators to the map may be invalidated if insertions triggers rehashing.
  using List = std::list<KVPair>;
  using Index = std::unordered_map<Key, typename List::const_iterator>;

  // The index of cache entries.
  Index index_;
  // The LRU list of cache entries (front is the MRU element).
  List list_;
  // The capacity of the cache.
  size_t capacity_;
  // The total size of all cached entries.
  size_t size_;

  // Evict some cache entries so that "Size() + headroom <= Capacity()".
  //
  // Requires: headroom >= 0.
  void Evict(size_t headroom = 0);
};

// A simple LRU cache with zero-copy support.
template <class Key, class Value>
class ZeroCopySimpleLRUCache : public ZeroCopyCache<Key, Value> {
 public:
  explicit ZeroCopySimpleLRUCache(size_t capacity)
      : capacity_(capacity), size_(0) {
    CHECK_GT(capacity_, 0);
    index_.reserve(capacity_);
  }

  // The Cache interface.
  bool Start() override { return true; }
  bool Stop() override { return true; }
  void Insert(const Key& key, Value value, size_t size = 1) override;
  std::optional<Value> Lookup(const Key& key) override;
  void Remove(const Key& key) override;
  void RemoveAll() override;
  size_t Capacity() const override { return capacity_; }
  void SetCapacity(size_t capacity) override;
  size_t Size() const override { return size_; }

  // The ZeroCopyCache interface.
  typename ZeroCopyCache<Key, Value>::Handle* Acquire(const Key& key) override;
  void Release(typename ZeroCopyCache<Key, Value>::Handle* handle) override;
  typename ZeroCopyCache<Key, Value>::Handle* InsertPinned(
      const Key& key, Value value, size_t size = 1) override;

 private:
  class HandleImpl final : public ZeroCopyCache<Key, Value>::Handle {
   public:
    HandleImpl(const Key& key, std::shared_ptr<Value> value)
        : key_(key), value_(std::move(CHECK_NOTNULL(value))) {}

    const Key& key() const override { return key_; }
    const Value& value() const override { return *value_; }

    typename ZeroCopyCache<Key, Value>::Handle* Clone() const override {
      return new HandleImpl(key_, value_);
    }

   private:
    const Key key_;
    const std::shared_ptr<Value> value_;
  };

  // We use shared pointers for reference counting, and setup custom deleter
  // to track the current cache size.
  using KVPair = std::pair<Key, std::shared_ptr<Value>>;
  // NOTE: we store the values in the list instead of the map, because
  // iterators to the map may be invalidated if insertions triggers rehashing.
  using List = std::list<KVPair>;
  using Index = std::unordered_map<Key, typename List::const_iterator>;

  // The index of cache entries.
  Index index_;
  // The LRU list of cache entries (front is the MRU element).
  List list_;
  // The capacity of this cache.
  size_t capacity_;
  // The current size of this cache, including removed but pinned cache
  // entries.
  size_t size_;

  // Try to evict some unpinned cache entries so that "Size() <= Capacity()".
  //
  // NOTE: if there are not enough unpinned cache entries to evict, the cache
  // size may still be greater than the capacity after calling this method.
  void Evict();
};

//------------------------------------------------------------------------------
// End of public interfaces.
// Implementation details follow.
//------------------------------------------------------------------------------

template <class Key, class Value>
SimpleLRUCache<Key, Value>::SimpleLRUCache(size_t capacity)
    : capacity_(capacity), size_(0) {
  CHECK_GT(capacity_, 0);
  index_.reserve(capacity_);
}

template <class Key, class Value>
void SimpleLRUCache<Key, Value>::Insert(const Key& key, Value value,
                                        size_t size) {
  CHECK_GT(size, 0);
  if (size > capacity_) {
    return;
  }

  Remove(key);

  list_.emplace_front(key, Elem(std::move(value), size));
  size_ += size;
  index_[key] = list_.begin();

  Evict();
}

template <class Key, class Value>
std::optional<Value> SimpleLRUCache<Key, Value>::Lookup(const Key& key) {
  typename Index::const_iterator iter = index_.find(key);
  if (iter == index_.end()) {
    return std::nullopt;
  }

  list_.splice(list_.begin(), list_, iter->second);
  return iter->second->second.value;
}

template <class Key, class Value>
void SimpleLRUCache<Key, Value>::Remove(const Key& key) {
  typename Index::iterator iter = index_.find(key);
  if (iter != index_.end()) {
    size_ -= iter->second->second.size;
    list_.erase(iter->second);
    index_.erase(iter);
  }
}

template <class Key, class Value>
void SimpleLRUCache<Key, Value>::RemoveAll() {
  index_.clear();
  list_.clear();
  size_ = 0;
}

template <class Key, class Value>
void SimpleLRUCache<Key, Value>::SetCapacity(size_t capacity) {
  CHECK_GT(capacity, 0);
  capacity_ = capacity;
  Evict();
}

template <class Key, class Value>
void SimpleLRUCache<Key, Value>::Evict(size_t headroom) {
  CHECK_GE(headroom, 0);
  while (!list_.empty() && size_ + headroom > capacity_) {
    typename List::iterator iter = list_.end();
    iter--;
    index_.erase(iter->first);
    size_ -= iter->second.size;
    list_.pop_back();
  }
}

template <class Key, class Value>
void ZeroCopySimpleLRUCache<Key, Value>::Insert(const Key& key, Value value,
                                                size_t size) {
  Release(InsertPinned(key, value, size));
}

template <class Key, class Value>
std::optional<Value> ZeroCopySimpleLRUCache<Key, Value>::Lookup(
    const Key& key) {
  typename ZeroCopyCache<Key, Value>::ScopedLookup lookup(this, key);
  if (lookup.Found()) {
    return lookup.value();
  } else {
    return std::nullopt;
  }
}

template <class Key, class Value>
void ZeroCopySimpleLRUCache<Key, Value>::Remove(const Key& key) {
  auto iter = index_.find(key);
  if (iter != index_.end()) {
    list_.erase(iter->second);
    index_.erase(iter);
  }
}

template <class Key, class Value>
void ZeroCopySimpleLRUCache<Key, Value>::RemoveAll() {
  index_.clear();
  list_.clear();
}

template <class Key, class Value>
typename ZeroCopyCache<Key, Value>::Handle*
ZeroCopySimpleLRUCache<Key, Value>::Acquire(const Key& key) {
  auto iter = index_.find(key);
  if (iter == index_.end()) {
    return nullptr;
  }

  list_.splice(list_.begin(), list_, iter->second);
  return new HandleImpl(key, iter->second->second);
}

template <class Key, class Value>
void ZeroCopySimpleLRUCache<Key, Value>::Release(
    typename ZeroCopyCache<Key, Value>::Handle* handle) {
  if (handle) {
    delete handle;
    Evict();
  }
}

template <class Key, class Value>
typename ZeroCopyCache<Key, Value>::Handle*
ZeroCopySimpleLRUCache<Key, Value>::InsertPinned(const Key& key, Value value,
                                                 size_t size) {
  CHECK_GT(size, 0);

  if (size > capacity_) {
    return nullptr;
  }

  // The previous value will be deleted when all outstanding pins are
  // released.
  Remove(key);

  // Use custom deleter so that the cache size gets updated when all
  // outstanding pins on removed cache entries are removed.
  std::shared_ptr<Value> pin(new Value(std::move(value)), [this, size](auto p) {
    CHECK_GE(size_, size);
    size_ -= size;
    delete p;
  });
  list_.emplace_front(key, pin);
  size_ += size;
  index_[key] = list_.begin();

  Evict();

  return new HandleImpl(key, std::move(pin));
}

template <class Key, class Value>
void ZeroCopySimpleLRUCache<Key, Value>::SetCapacity(size_t capacity) {
  CHECK_GT(capacity, 0);
  capacity_ = capacity;
  Evict();
}

template <class Key, class Value>
void ZeroCopySimpleLRUCache<Key, Value>::Evict() {
  if (size_ <= capacity_) return;

  // The cache size has grown over capacity, try to evict some unpinned cache
  // entries.
  auto iter = list_.end();
  while (size_ > capacity_ && iter != list_.begin()) {
    iter--;
    // A cache entry with no outstanding pin has only one reference by the
    // cache itself.
    if (iter->second.use_count() == 1) {
      index_.erase(iter->first);
      iter = list_.erase(iter);
    }
  }
}

}  // namespace mtcache
