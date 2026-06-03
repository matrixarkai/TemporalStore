#include "arc.h"

namespace mtcache {

// === BaseLRUList ===

void BaseLRUList::Put(const CacheKeyType& key) {
  Delete(key);

  list_.emplace_front(Node(key));
  // It doesn't matter whether an insert or assignment OP
  // is performed, as the used space is not tracked in the
  // BaseLRUList.
  // There is no need to acquire the map_lock here either, as
  // there is a mutex acquired in the ReplacementArc::Put method
  // before this method is called.
  index_.insert_or_assign(key, IndexVal(list_.begin()));
}

bool BaseLRUList::Get(const CacheKeyType& key) {
  if (auto iter = index_.find(key); iter != index_.end()) {
    // move to first place
    auto list_iter = iter->second.list_iter;

    auto item = list_iter->item;
    list_.erase(list_iter);
    list_.emplace_front(Node(key));
    // Similar to Put method, there is no need to acquire the map_lock
    // here and it doesn't matter whether an assignment or insert OP is
    // performed.
    index_.insert_or_assign(key, IndexVal(list_.begin()));

    return true;
  }

  return false;
}

bool BaseLRUList::Delete(const CacheKeyType& key) {
  if (auto iter = index_.find(key); iter != index_.end()) {
    auto list_iter = iter->second.list_iter;

    list_.erase(list_iter);
    index_.erase(iter);

    return true;
  }

  return false;
}

std::vector<CacheKeyType> BaseLRUList::GetTail(size_t size) {
  std::vector<CacheKeyType> tail;
  for (auto iter = list_.rbegin(); iter != list_.rend() && tail.size() < size;
       ++iter) {
    tail.push_back(iter->item);
  }
  return tail;
}

std::vector<CacheKeyType> BaseLRUList::Evict() {
  std::vector<CacheKeyType> evicted;

  // evict ghost
  while (index_.size() > capacity_) {
    auto key = list_.back().item;
    evicted.emplace_back(key);
    index_.erase(key);
    list_.pop_back();
  }

  return evicted;
}

std::vector<CacheKeyType> BaseLRUList::EvictOne() {
  std::vector<CacheKeyType> evicted;

  if (!index_.empty()) {
    auto key = list_.back().item;
    evicted.emplace_back(key);
    index_.erase(key);
    list_.pop_back();
  }

  return evicted;
}

// === GhostLRUList ===

void GhostLRUList::Put(const CacheKeyType& key) { data_list_.Put(key); }

void GhostLRUList::PutGhost(const CacheKeyType& key) { ghost_list_.Put(key); }

bool GhostLRUList::Get(const CacheKeyType& key) {
  if (data_list_.Get(key)) {
    return true;
  }

  ghost_list_.Get(key);
  // Whether it exist in ghost list, return false.
  return false;
}

bool GhostLRUList::Delete(const CacheKeyType& key) {
  return data_list_.Delete(key) && ghost_list_.Delete(key);
}

std::vector<CacheKeyType> GhostLRUList::GetDataTail(size_t size) {
  return data_list_.GetTail(size);
}

std::vector<CacheKeyType> GhostLRUList::GetGhostTail(size_t size) {
  return ghost_list_.GetTail(size);
}
std::vector<CacheKeyType> GhostLRUList::Evict() {
  ghost_list_.Evict();
  return data_list_.Evict();
}

GhostLRUList::PopResult GhostLRUList::Pop(const std::string& key) {
  GhostLRUList::PopResult result;

  if (data_list_.Delete(key)) {
    result.item = key;
    result.is_ghost = false;
  } else if (ghost_list_.Delete(key)) {
    result.item = key;
    result.is_ghost = true;
  }

  return result;
}
void GhostLRUList::Reset() {
  data_list_.Reset();
  ghost_list_.Reset();
}

void GhostLRUList::Downgrade() {
  auto keys = data_list_.EvictOne();
  for (auto& key : keys) {
    ghost_list_.Put(key);
  }
}

std::vector<CacheKeyType> GhostLRUList::EvictOneData() {
  return data_list_.EvictOne();
}

std::vector<CacheKeyType> GhostLRUList::EvictOneGhost() {
  return ghost_list_.EvictOne();
}

// === ArcList ===

void ArcList::Put(const CacheKeyType& key) {
  VLOG(10) << "Put key=" << key;
  Access(key);
}

bool ArcList::Get(const CacheKeyType& key) {
  VLOG(10) << "Get key=" << key;
  return Access(key);
}

// ARC algorithm description
// Name
//   name in paper - variable in code
//   T1 - fetch_list_.data_list_
//   T2 - active_list_.data_list_
//   t1 - fetch_list_.data_list_.Size()
//   t2 - active_list_.data_list_.Size()
//   B1 - fetch_list_.ghost_list_
//   B2 - active_list_.ghost_list_
//   b1 - fetch_list_.ghost_list_.Size()
//   b2 - active_list_.ghost_list_.Size()
//   L1 - fetch_list_
//   L2 - active_list_
//   p  - fetch_list_.Capacity(), not a strict limit, size can exceed it.
//   c  - Capacity()
// Equation
//   b1 + b2 = c
//   t1 + t2 = c
// State Processing
//   access to key x occurred
//   Case I: x is in T1 or T2, cache hit.
//     move x to MRU in T2
//   Case II: x is in B1, cache miss. In addition, it means T1 is too small.
//     update p = min(p+max(1,b2/b1), c)
//     Replace(), release data space.
//     move x from B1 to T2
//   Case III: x is in B2, cache miss. In addition, it means T2 is to small.
//     update p = max(p-max(1,b1/b2), 0)
//     Replace(), release data space.
//     move x from B2 to T2
//   Case IV: x is not in T1/B1/T2/B2, totally cache miss.
//     Case A: L1 = T1 U B1 has exactly c items, means L1 is too full.
//       Case A.1: t1 < c, means B1 is too full
//         Delete LRU item in B1
//         Replace(), release data space.
//       Case A.2: t1 = c, means B1 is empty, need to evict item in T1 directly
//         Delete LRU item in T1
//     Case B: L1 = T1 U B1 has less than c items, and t1+t2+b1+b2 >= c, means
//     the load of the entire list is high.
//       If t1+t2+b1+b2 == 2*c, means the entire list is full.
//         Delete LRU item in B2, release the space for new item.
//       Replace(), release data space.
//     Finally, new key added, move x to T1.
//
// the Subroutine Replace(), will evict one cache item from data list to ghost
// list in L1 or L2, to release one data quota.
//   Replace()
//     If t1 > 0 and (t1 > p or (t1 == p and x hit B2)), means T1 is too full.
//       Delete LRU page in T1, move it to B1.
//     else
//       Delete LRU page in T2, move it to B2.
bool ArcList::Access(const CacheKeyType& key) {
  if (auto result = this->Pop(key); result.item.empty()) {
    // miss
    // Case IV: cache miss has occurred
    VLOG(10) << "  Case IV: cache miss has occurred";
    if (fetch_list_.TotalSize() >= Capacity()) {
      // Case A: L1 has exactly c items
      VLOG(10) << "  Case A: L1 has exactly c items";
      if (fetch_list_.Size() < Capacity()) {
        VLOG(10) << "  Case A: Delete LRU item in B1 & Replace";
        fetch_list_.EvictOneGhost();
        Replace(false /* key_in_active_ghost */);
      } else {
        VLOG(10) << "  Case A: Delete LRU item in T1";
        fetch_list_.EvictOneData();
      }
    } else {
      // Case B: L1 has less than c items
      VLOG(10) << "  Case B: L1 has less than c items";
      if (TotalSize() >= Capacity()) {
        VLOG(10) << "  Case B: half full";
        if (TotalSize() >= 2 * Capacity()) {
          VLOG(10) << "  Case B: full, Delete LRU item in B2";
          active_list_.EvictOneGhost();
        }
        VLOG(10) << "  Case B: half full, Replace";
        Replace(false);
      }
    }
    VLOG(10) << "  Finally, move it to MRU in T1";
    fetch_list_.Put(key);
    return false;
  } else {
    if (result.is_ghost) {
      int c = Capacity();
      int p = fetch_list_.Capacity();
      int b1 = fetch_list_.GhostSize();
      int b2 = active_list_.GhostSize();
      // if hit the key in ghost list, it means need to adjust queue size.
      VLOG(10) << "  Hit ghost list";
      if (result.is_active) {
        // This item has been popped out of the ghost list and the count must be
        // added
        b2 += 1;
        // Case III: hit active list ghost, p = max{p-delta, 0}
        VLOG(10) << "  Case III: hit active list ghost, p = max{p-delta, 0}";

        int d = std::max(1, b1 / b2);
        p = std::max(p - d, 0);
        VLOG(10) << "    "
                 << "b1=" << b1 << ", b2=" << b2 << ", d=" << d
                 << ", p=fetch_capacity=" << p
                 << ", active_capacity=" << capacity_ - p;

        fetch_list_.SetCapacity(p);
        active_list_.SetCapacity(capacity_ - p);

        VLOG(10) << "  Case III: Replace & move it to MRU in T2";
        Replace(true /* key_in_active_ghost */);
        active_list_.Put(key);
      } else {
        // This item has been popped out of the ghost list and the count must be
        // added
        b1 += 1;
        // Case II: hit fetch list ghost, p = min{p+delta, c}
        VLOG(10) << "  Case II: hit fetch list ghost, p = min{p+delta, c}";

        int d = std::max(1, b2 / b1);
        p = std::min(p + d, c);
        VLOG(10) << "    "
                 << "b1=" << b1 << ", b2=" << b2 << ", d=" << d
                 << ", p=fetch_capacity=" << p
                 << ", active_capacity=" << capacity_ - p;

        fetch_list_.SetCapacity(p);
        active_list_.SetCapacity(capacity_ - p);

        VLOG(10) << "  Case II: Replace & move it to MRU in T2";
        Replace(false /* key_in_active_ghost */);
        active_list_.Put(key);
      }

      return false;
    } else {
      // hit, insert it to the active list
      // Case I: move to MRU in T2 (active list)
      VLOG(10) << "  Case I: move to MRU in T2";
      active_list_.Put(key);
      return true;
    }
  }
  return false;
}

std::vector<CacheKeyType> ArcList::Evict() {
  std::vector<CacheKeyType> res;

  auto fetch_evicted = fetch_list_.Evict();
  res.insert(std::end(res), std::begin(fetch_evicted), std::end(fetch_evicted));

  auto active_evicted = active_list_.Evict();
  res.insert(std::end(res), std::begin(active_evicted),
             std::end(active_evicted));

  return res;
}

bool ArcList::Delete(const CacheKeyType& key) {
  return fetch_list_.Delete(key) || active_list_.Delete(key);
}

ArcList::PopResult ArcList::Pop(const std::string& key) {
  PopResult result;

  if (auto res = fetch_list_.Pop(key); !res.item.empty()) {
    result.item = res.item;
    result.is_ghost = res.is_ghost;
    result.is_active = false;
  } else if (auto res = active_list_.Pop(key); !res.item.empty()) {
    result.item = res.item;
    result.is_ghost = res.is_ghost;
    result.is_active = true;
  }

  return result;
}

void ArcList::Replace(bool key_in_active_ghost) {
  if (fetch_list_.Size() != 0 &&
      (fetch_list_.Size() > fetch_list_.Capacity() ||
       (key_in_active_ghost && fetch_list_.Size() == fetch_list_.Capacity()))) {
    // Replace 1
    VLOG(10) << "  Replace 1, fetch list do downgrade";
    fetch_list_.Downgrade();
  } else {
    // Replace 2
    VLOG(10) << "  Replace 2, active list do downgrade";
    active_list_.Downgrade();
  }
}

// === ReplacementArc ===

noodle::Result<void, CacheError> ReplacementArc::Init() {
  std::lock_guard<std::mutex> lock(mutex_);

  initialized_ = true;

  arc_list_.SetCapacity(item_capacity_);

  return {};
}

noodle::Result<void, CacheError> ReplacementArc::Reset() {
  std::lock_guard<std::mutex> lock(mutex_);

  initialized_ = false;

  arc_list_.Reset();

  return {};
}

void ReplacementArc::Put(const CacheKeyType& key) {
  if (key.empty()) {
    return;
  }
  std::lock_guard<std::mutex> lock(mutex_);

  arc_list_.Put(key);
}

bool ReplacementArc::Get(const CacheKeyType& key) {
  std::lock_guard<std::mutex> lock(mutex_);

  return arc_list_.Get(key);
}

bool ReplacementArc::Delete(const CacheKeyType& key) {
  std::lock_guard<std::mutex> lock(mutex_);

  return arc_list_.Delete(key);
}

std::vector<CacheKeyType> ReplacementArc::GetActiveTail(size_t size) {
  std::lock_guard<std::mutex> lock(mutex_);

  return arc_list_.GetActiveDataTail(size);
}

std::vector<CacheKeyType> ReplacementArc::GetFetchTail(size_t size) {
  std::lock_guard<std::mutex> lock(mutex_);

  return arc_list_.GetFetchDataTail(size);
}

}  // namespace mtcache
