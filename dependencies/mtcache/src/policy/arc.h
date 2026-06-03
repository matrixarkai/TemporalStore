#pragma once

#include "map/cache_map.h"
#include "replacement_policy.h"

#include <list>
#include <vector>

namespace mtcache {

using CacheKeyType = std::string;

class BaseLRUList {
 public:
  BaseLRUList() : capacity_(0) {}
  explicit BaseLRUList(size_t capacity) : capacity_(capacity) {}
  ~BaseLRUList() = default;

  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  size_t Capacity() { return capacity_; }

  size_t Size() { return index_.size(); }

  void Put(const CacheKeyType& key);

  bool Get(const CacheKeyType& key);

  bool Delete(const CacheKeyType& key);

  // Get tail items from LRU list tail
  std::vector<CacheKeyType> GetTail(size_t size);

  // Evict keys and return them.
  std::vector<CacheKeyType> Evict();

  std::vector<CacheKeyType> EvictOne();

  void Reset() {
    list_.clear();
    index_.clear();
  }

 private:
  struct Node {
    explicit Node(std::string p_item) : item(std::move(p_item)) {}

    std::string item;
  };

  using NodeList = std::list<Node>;

  struct IndexVal {
    explicit IndexVal(NodeList::iterator p_list_iter)
        : list_iter(p_list_iter) {}

    NodeList::iterator list_iter;
  };

  // capacity of data item
  size_t capacity_;

  NodeList list_;

  ConcurrentHashMap<std::string, IndexVal> index_;
};

// A LRU List with a ghost list, caller can choose to put cache item to ghost
// list. Both data list and ghost list use LRU to choose cache item to be
// evicted.
//
// The cache item in the data list keeps data in the cache instance, while the
// cache_item in the ghost list only has meta information.
//
// The meaning of ghost list is to adjust the size of the queue for the ARC
// algorithm. Every time the cache key in the ghost list is hit, it indicates
// that current data list is too small and needs to be expanded.
//
// single thread only
class GhostLRUList {
 public:
  explicit GhostLRUList(size_t capacity)
      : data_list_(capacity), ghost_list_(capacity) {}
  ~GhostLRUList() {}

  // Notice: Ghost list has no capacity state, and does not rely on capacity to
  // limit the number of item.
  void SetCapacity(size_t capacity) {
    data_list_.SetCapacity(capacity);
    ghost_list_.SetCapacity(capacity);
  }

  size_t Capacity() { return data_list_.Capacity(); }

  size_t GhostCapacity() { return ghost_list_.Capacity(); }

  size_t Size() { return data_list_.Size(); }

  size_t GhostSize() { return ghost_list_.Size(); }

  size_t TotalSize() { return Size() + GhostSize(); }

  void Put(const CacheKeyType& key);

  void PutGhost(const CacheKeyType& key);

  bool Get(const CacheKeyType& key);

  bool Delete(const CacheKeyType& key);

  // Delete LRU cache item in data list and move it to MRU in ghost list
  void Downgrade();

  // Delete LRU cache item in data list, do not move it to ghost list
  std::vector<CacheKeyType> EvictOneData();

  // Delete LRU cache item in ghost list
  std::vector<CacheKeyType> EvictOneGhost();

  // Get tail items from fetch and active list
  std::vector<CacheKeyType> GetDataTail(size_t size);

  std::vector<CacheKeyType> GetGhostTail(size_t size);

  // Evict data list and ghost list, but only return the items evicted from data
  // list.
  std::vector<CacheKeyType> Evict();

  struct PopResult {
    CacheKeyType item{""};
    bool is_ghost{false};
  };

  // If the key exists in any list, remove it from the list.
  // set item = "" if the key *does not exist* in any list
  // set is_ghost = true if the key exist in ghost list.
  // set is_ghost = false if the key exist in data list.
  PopResult Pop(const CacheKeyType& key);

  void Reset();

 private:
  BaseLRUList data_list_;

  BaseLRUList ghost_list_;
};

// Arc list is composed of two GhostLRULists, called fetch list and active list.
// The fetch list saves cache items that have been accessed only once, while the
// active list saves cache items that have been accessed multiple times
//
// The total size of the two lists is fixed, but the respective size will be
// adjusted according to the work load.
//
// The adjustment rule: that no adjustment will be made if the data list is
// hit, and the capacity will be expanded if the ghost list is hit.
// Correspondingly, the capacity of another list will be reduced.
class ArcList {
 public:
  explicit ArcList(size_t capacity)
      : capacity_(capacity),
        fetch_list_(capacity_ / 2),
        active_list_(capacity_ - capacity_ / 2) {}
  ~ArcList() {}

  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  size_t Size() { return fetch_list_.Size() + active_list_.Size(); }

  size_t GhostSize() {
    return fetch_list_.GhostSize() + active_list_.GhostSize();
  }

  size_t TotalSize() { return Size() + GhostSize(); }

  size_t Capacity() { return capacity_; }

  size_t FetchCapacity() { return fetch_list_.Capacity(); }

  size_t ActiveCapacity() { return active_list_.Capacity(); }

  // full of data item
  bool DataFull() { return Size() >= Capacity(); }

  void Reset() {
    fetch_list_.Reset();
    active_list_.Reset();
  }

  void Put(const CacheKeyType& key);

  bool Get(const CacheKeyType& key);

  // Evict items from fetch and active list.
  std::vector<CacheKeyType> Evict();

  // Remove the specified cache item.
  bool Delete(const CacheKeyType& key);

  // Get tail items from fetch/active lists' data/ghost list.
  std::vector<CacheKeyType> GetActiveDataTail(size_t size) {
    return active_list_.GetDataTail(size);
  }
  std::vector<CacheKeyType> GetFetchDataTail(size_t size) {
    return fetch_list_.GetDataTail(size);
  }
  std::vector<CacheKeyType> GetActiveGhostTail(size_t size) {
    return active_list_.GetGhostTail(size);
  }
  std::vector<CacheKeyType> GetFetchGhostTail(size_t size) {
    return fetch_list_.GetGhostTail(size);
  }

 private:
  // In the ARC algorithm, no matter it is a PUT/GET request, the processing
  // method is the same. So, we abstract the Access method to unify this
  // process.
  bool Access(const CacheKeyType& key);

  // the Replacement operation in ARC
  // If fetch list is overloaded or fetch list is exactly full and
  // key_in_active_ghost is true, move the LRU cache item in fetch list's data
  // list to fetch list's ghost list. otherwise, move the LRU cache item in
  // active list's data list to active list's ghost list
  void Replace(bool key_in_active_ghost);

  struct PopResult {
    CacheKeyType item;
    bool is_active{false};
    bool is_ghost{false};
  };

  PopResult Pop(const std::string& key);

  // Capacity represents the limit of the number of items, not the limit of the
  // amount of data.
  // It is equivalent to the c value in the paper.
  // In fact, data + ghost = 2*capacity
  size_t capacity_;

  GhostLRUList fetch_list_;
  GhostLRUList active_list_;
};

// ARC Replacement Policy, use for l2arc.
// The L2ARC sits in-between, extending the main memory cache using fast storage
// devices, such as flash memory based SSDs (solid state disks).
// Reference: http://www.brendangregg.com/blog/2008-07-22/zfs-l2arc.html
class ReplacementArc {
 public:
  explicit ReplacementArc(size_t item_capacity)
      : item_capacity_(item_capacity), arc_list_(item_capacity_) {}
  ~ReplacementArc() {}

  noodle::Result<void, CacheError> Init();

  noodle::Result<void, CacheError> Reset();

  size_t GetItemCapacity() { return item_capacity_; }

  void SetItemCapacity(size_t new_item_capacity) {
    std::lock_guard<std::mutex> lock(mutex_);

    item_capacity_ = new_item_capacity;
    arc_list_.SetCapacity(item_capacity_);
  }

  // Put cache item to ArcList
  void Put(const CacheKeyType& key);

  // Get cache item from ArcList
  // Return true if key exist, otherwise false
  bool Get(const CacheKeyType& key);

  // Delete cache item from ArcList
  // Return true if key exist, otherwise false
  bool Delete(const CacheKeyType& key);

  // Get tail items.
  std::vector<CacheKeyType> GetActiveTail(size_t size);
  std::vector<CacheKeyType> GetFetchTail(size_t size);

 private:
  std::vector<CacheKeyType> Evict();

  // capacity of item number, will dynamically adjust according to capacity
  size_t item_capacity_;

  bool initialized_{false};

  ArcList arc_list_;

 private:
  mutable std::mutex mutex_;
};

}  // namespace mtcache
