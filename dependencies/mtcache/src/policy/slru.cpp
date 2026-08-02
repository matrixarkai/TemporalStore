#include "slru.h"

#include "common/logging.h"

// Ratio of hot buffers in cache
DECLARE_int32(hot_lru_pct);
// Ratio of warm buffers in cache
DECLARE_int32(warm_lru_pct);
// We use cache_slru_ut to indicate that we are testing SLRU UT
DECLARE_bool(cache_slru_ut);
// Number of segments in SLRU
DECLARE_int32(slru_num_segments);

namespace mtcache {

noodle::Result<void, CacheError> ReplacementSLRU::Init() {
  initialized_ = true;
  // initialize ConcurrentHashMap with a large size to reduce
  // rehash induced long latency, need to implement a heuristic way to get size
  // number. The current size number is estimated based on cache size, key and
  // value sizes in the microbenchmark.
  index_.reserve(33554432);

  num_segments_ = FLAGS_slru_num_segments;
  // if capacity is too small, then we only have one segment.
  if (capacity_ < num_segments_) {
    num_segments_ = 1;
  }
  bytes_each_segment_ = capacity_ / num_segments_;
  lru_lists_ = new SLRUList[FLAGS_slru_num_segments];

  SetEnableLRUMaintainer(false);
  StartLRUMaintainer();
  return {};
}

noodle::Result<void, CacheError> ReplacementSLRU::Reset() {
  // hashmap clear is an atomic op (contains a lock_guard lock)
  index_.clear();
  for (int i = 0; i < num_segments_; i++) {
    for (int j = 0; j < 3; j++) {
      lru_lists_[i].lists_[j].list_.clear();
      lru_lists_[i].used_.store(0, std::memory_order_release);
    }
  }
  used_ = 0;
  return {};
}

std::vector<CacheBufferSharedPtr> ReplacementSLRU::Put(
    CacheBufferSharedPtr buffer_ptr) {
  CHECK(initialized_);
  CHECK(buffer_ptr != nullptr);

  const std::string new_key = buffer_ptr->Key();
  size_t value_size = buffer_ptr->Size();
  auto segment_id = PickSegment(new_key);
  std::vector<CacheBufferSharedPtr> evicted;

  index_.map_lock(new_key);

  LRUList& lru_list = lru_lists_[segment_id].lists_[HOT_LRU];
  lru_list.list_lock.lock();

  lru_list.list_.emplace_front(new_key);
  // update used memory capacity
  lru_list.list_used_ += (value_size + new_key.size());
  lru_lists_[segment_id].used_ += (value_size + new_key.size());
  auto res = index_.insert_or_assign(
      new_key,
      Node(HOT_LRU, std::make_shared<std::atomic<uint16_t>>(BUFFER_INIT),
           buffer_ptr, lru_list.list_.begin()));
  // If an assignment OP is performed in the index_, the old buffer should be
  // removed from the LRU list
  const auto lru_clsid = res.first->second.lru_clsid;
  if (!res.second && lru_clsid == HOT_LRU) {
    // remove the updated iterator from the HOT LRU list
    std::list<std::string>::iterator list_iter = res.first->second.list_iter;
    lru_list.list_.erase(list_iter);
    // Remove the size of the old buffer from the LRU used spaces
    size_t old_val_size = res.first->second.buffer_->Size();
    lru_list.list_used_ -= (old_val_size + new_key.size());
    lru_lists_[segment_id].used_ -= (old_val_size + new_key.size());
  }
  lru_list.list_lock.unlock();

  // the original LRU list of `new_key` could be another LRU list rather than
  // the hot LRU list, therefore, let's get the original one and update it
  if (!res.second && lru_clsid != HOT_LRU) {
    std::list<std::string>::iterator list_iter = res.first->second.list_iter;
    LRUList& orig_lru_list = lru_lists_[segment_id].lists_[lru_clsid];
    orig_lru_list.list_lock.lock();
    orig_lru_list.list_.erase(list_iter);
    orig_lru_list.list_lock.unlock();
    // Remove the size of the old buffer from the LRU used spaces
    size_t old_val_size = res.first->second.buffer_->Size();
    orig_lru_list.list_used_ -= (old_val_size + new_key.size());
    lru_lists_[segment_id].used_ -= (old_val_size + new_key.size());
  }

  index_.map_unlock(new_key);

  // cache eviction if needed
  Evict(segment_id, &evicted);
  return evicted;
}

noodle::Result<void, CacheError> ReplacementSLRU::UpdateCacheBuffer(
    const std::string& key, const char* old_data_ptr,
    CacheBufferSharedPtr buffer_ptr) {
  CHECK(initialized_);
  CHECK(buffer_ptr != nullptr);
  // during the update, we will assign its original node info to
  // `key`, which has to be exactly correct, otherwise, further
  // modifications based on the node info could incur unexpected
  // results. If we don't hold a lock at the very beginning, after
  // we got an iterator, other threads could migrate `key` to
  // another LRU list and update its node info with new `lru_clsid`
  // and `list_iter`, while the `assign` operation in this function
  // will then overwrite its node info with an outdated node info.
  // To ensure we always get the correct node info, we acquire a
  // lock before the `find`. Moreover, the `assign` operation should
  // always success since `key` must in the map if `assign` getting
  // invoked.
  std::unique_lock lock(*index_.get_map_lock(key));
  auto iter = index_.find(key);
  if (UNLIKELY(iter == index_.end())) {
    return &Errors::kCacheBufferNotFound;
  } else {
    auto old_buffer = iter->second.buffer_;
    if (old_buffer->Data() != old_data_ptr) {
      return &Errors::kCacheReplaceMismatch;
    }
    auto node = iter->second;
    node.buffer_ = buffer_ptr;

    auto result = index_.assign(key, node);
    CHECK(result.has_value());
    return {};
  }
}

CacheBufferSharedPtr ReplacementSLRU::Get(const std::string& key) {
  CHECK(initialized_);

  auto iter = index_.find(key);

  // do not need to get the lock here
  // update buffer reference flag if neeeded
  if (iter != index_.end()) {
    if (GetNodeBufferFlag(iter->second) != BUFFER_ACTIVE) {
      if (GetNodeBufferFlag(iter->second) != BUFFER_FETCHED) {
        SetNodeBufferFlag(iter->second, BUFFER_FETCHED);
      } else {
        SetNodeBufferFlag(iter->second, BUFFER_ACTIVE);
      }
    }
    return iter->second.buffer_;
  } else {
    return std::shared_ptr<CacheBuffer>();
  }
}

CacheBufferSharedPtr ReplacementSLRU::Peek(const std::string& key) {
  auto iter = index_.find(key);
  if (iter != index_.end()) {
    return iter->second.buffer_;
  } else {
    return std::shared_ptr<CacheBuffer>();
  }
}

CacheBufferSharedPtr ReplacementSLRU::Delete(const std::string& key) {
  CHECK(initialized_);

  auto segment_id = PickSegment(key);
  auto map_iter = index_.find(key);
  if (map_iter == index_.end()) {
    return std::shared_ptr<CacheBuffer>();
  } else {
    index_.map_lock(key);
    // Other threads may have modified lru_clsid between index_.find()
    // and index_.map_lock(). In this case, we will lock the wrong
    // lru_list and the data race may happen.
    // So We have to update map_iter after we call index_.map_lock().
    // If we cannot find it from index_, then we just unlock index_
    // and return.
    if ((map_iter = index_.find(key)) == index_.end()) {
      index_.map_unlock(key);
      return std::shared_ptr<CacheBuffer>();
    }
    auto buffer = map_iter->second.buffer_;
    size_t val_size = buffer->Size();
    LRUList& lru_list =
        lru_lists_[segment_id].lists_[map_iter->second.lru_clsid];
    lru_list.list_lock.lock();
    std::list<std::string>::iterator list_iter = map_iter->second.list_iter;
    // remove it from lru list
    lru_list.list_.erase(list_iter);
    lru_list.list_used_ -= (val_size + key.size());
    lru_lists_[segment_id].used_ -= (val_size + key.size());
    // remove it from index hashmap
    index_.erase(map_iter);

    lru_list.list_lock.unlock();
    index_.map_unlock(key);
    return buffer;
  }
}

void ReplacementSLRU::Dump(uint64_t segment_id, int cur_lru,
                           std::vector<CacheBufferSharedPtr>* evicted) {
  LRUList& lru_list = lru_lists_[segment_id].lists_[cur_lru];

  while (GetSegmentUsedSize(segment_id) > GetSegmentByteLimit()) {
    lru_list.list_lock.lock();
    if (lru_list.list_.empty()) {
      lru_list.list_lock.unlock();
      break;
    }
    const std::string key = lru_list.list_.back();

    if (!index_.map_trylock(key)) {
      lru_list.list_lock.unlock();
      continue;
    }

    auto map_iter = index_.find(key);
    if (map_iter == index_.end()) {
      lru_list.list_.pop_back();
      index_.map_unlock(key);
      lru_list.list_lock.unlock();
      continue;
    }

    DCHECK(cur_lru == map_iter->second.lru_clsid);
    DCHECK(map_iter->second.list_iter == std::prev(lru_list.list_.end()));

    size_t val_size = map_iter->second.buffer_->Size();
    evicted->emplace_back(map_iter->second.buffer_);
    lru_list.list_.pop_back();
    lru_list.list_used_ -= (val_size + key.size());
    lru_lists_[segment_id].used_ -= (val_size + key.size());
    // If there exist a lower tier, we need to insert to its index first
    // to guarantee consistency.
    if (mem_eviction_func_) {
      mem_eviction_func_(map_iter->second.buffer_);
    }
    index_.erase(map_iter);

    index_.map_unlock(key);
    lru_list.list_lock.unlock();
  }
}

void ReplacementSLRU::Evict(uint64_t segment_id,
                            std::vector<CacheBufferSharedPtr>* evicted) {
  if (GetSegmentUsedSize(segment_id) < GetSegmentByteLimit()) {
    return;
  }

  // move active buffers from cold lru to warm lru
  // delete non-active buffers from cold lru

  // get the cold lru
  LRUList& cold_lru_list = lru_lists_[segment_id].lists_[COLD_LRU];
  LRUList& warm_lru_list = lru_lists_[segment_id].lists_[WARM_LRU];

  while (GetSegmentUsedSize(segment_id) > GetSegmentByteLimit()) {
    cold_lru_list.list_lock.lock();
    if (cold_lru_list.list_.empty()) {
      cold_lru_list.list_lock.unlock();
      break;
    }
    const std::string key = cold_lru_list.list_.back();
    if (!index_.map_trylock(key)) {
      cold_lru_list.list_lock.unlock();
      continue;
    }
    auto map_iter = index_.find(key);
    if (map_iter == index_.end()) {
      cold_lru_list.list_.pop_back();
      index_.map_unlock(key);
      cold_lru_list.list_lock.unlock();
      continue;
    }
    cold_lru_list.list_lock.unlock();

    DCHECK(COLD_LRU == map_iter->second.lru_clsid);
    DCHECK(map_iter->second.list_iter == std::prev(cold_lru_list.list_.end()));

    size_t val_size = map_iter->second.buffer_->Size();
    auto buffer_ptr = map_iter->second.buffer_;

    if (GetNodeBufferFlag(map_iter->second) == BUFFER_ACTIVE) {
      // move it to warm lru
      cold_lru_list.list_lock.lock();
      warm_lru_list.list_lock.lock();

      // key will still be used after being emplaced to the beginning of the
      // list, so should not be moved.
      warm_lru_list.list_.emplace_front(key);
      cold_lru_list.list_.pop_back();

      warm_lru_list.list_used_ += (val_size + key.size());
      cold_lru_list.list_used_ -= (val_size + key.size());
      auto res = index_.insert_or_assign(
          key,
          Node(WARM_LRU, std::make_shared<std::atomic<uint16_t>>(BUFFER_INIT),
               buffer_ptr, warm_lru_list.list_.begin()));
      // As the buffer is moved from cold list to warm list, an assignment
      // instead of insert OP should have been performed by insert_or_assign.
      if (res.second) {
        LOG(WARNING) << "ReplacementSLRU::Evict: insert instead of assignment "
                        "OP performed "
                     << "in policy index! key: " << key;
      }

      warm_lru_list.list_lock.unlock();
      cold_lru_list.list_lock.unlock();

    } else {
      // Remove from the cold list
      cold_lru_list.list_lock.lock();
      evicted->emplace_back(buffer_ptr);
      cold_lru_list.list_.pop_back();
      cold_lru_list.list_used_ -= (val_size + key.size());
      lru_lists_[segment_id].used_ -= (val_size + key.size());
      // If there exist a lower tier, we need to insert to its index first
      // to guarantee consistency.
      if (mem_eviction_func_) {
        mem_eviction_func_(map_iter->second.buffer_);
      }
      index_.erase(map_iter);
      cold_lru_list.list_lock.unlock();
    }
    index_.map_unlock(key);
  }
  // Haven't release enough space
  // force to kick buffers from warm/hot lru
  // kick buffers from warm lru
  Dump(segment_id, WARM_LRU, evicted);
  // kick buffers from hot lru
  Dump(segment_id, HOT_LRU, evicted);
}

void ReplacementSLRU::LRUMaintainerTask() {
  while (true) {
    for (int segment_id = 0; segment_id < num_segments_; segment_id++) {
      if (!CheckEnableLRUMaintainer()) break;
      // grab three lru lists
      LRUList& hot_lru_list = lru_lists_[segment_id].lists_[HOT_LRU];
      LRUList& warm_lru_list = lru_lists_[segment_id].lists_[WARM_LRU];
      LRUList& cold_lru_list = lru_lists_[segment_id].lists_[COLD_LRU];

      // If contention is found at any step, we will skip the remaining steps
      // and move to the next segment
      bool contention_find = false;
      // step 1 update hot lru
      // update the limit of hot lru
      auto hot_limit = GetSegmentByteLimit() * FLAGS_hot_lru_pct / 100;
      while (GetListUsedSize(segment_id, HOT_LRU) > hot_limit) {
        if (!hot_lru_list.list_lock.try_lock()) {
          contention_find = true;
          break;
        }

        if (hot_lru_list.list_.empty()) {
          hot_lru_list.list_lock.unlock();
          break;
        }

        const std::string key = hot_lru_list.list_.back();

        if (!index_.map_trylock(key)) {
          hot_lru_list.list_lock.unlock();
          contention_find = true;
          break;
        }
        auto map_iter = index_.find(key);
        if (map_iter == index_.end()) {
          hot_lru_list.list_.pop_back();
          index_.map_unlock(key);
          hot_lru_list.list_lock.unlock();
          break;
        }
        hot_lru_list.list_lock.unlock();

        CHECK_EQ(HOT_LRU, map_iter->second.lru_clsid);
        CHECK(map_iter->second.list_iter ==
              std::prev(hot_lru_list.list_.end()));

        size_t val_size = map_iter->second.buffer_->Size();
        auto buffer_ptr = map_iter->second.buffer_;

        if (GetNodeBufferFlag(map_iter->second) == BUFFER_ACTIVE) {
          // move to warm lru
          // if we cannot get the lock, then try it later
          if (!warm_lru_list.list_lock.try_lock()) {
            index_.map_unlock(key);
            contention_find = true;
            break;
          }
          if (!hot_lru_list.list_lock.try_lock()) {
            warm_lru_list.list_lock.unlock();
            index_.map_unlock(key);
            contention_find = true;
            break;
          }

          warm_lru_list.list_.emplace_front(key);
          hot_lru_list.list_.pop_back();
          auto res = index_.insert_or_assign(
              key, Node(WARM_LRU,
                        std::make_shared<std::atomic<uint16_t>>(BUFFER_INIT),
                        buffer_ptr, warm_lru_list.list_.begin()));
          hot_lru_list.list_used_ -= (val_size + key.size());
          warm_lru_list.list_used_ += (val_size + key.size());

          hot_lru_list.list_lock.unlock();
          warm_lru_list.list_lock.unlock();

          // As the buffer is moved from hot list to warm list, an assignment
          // instead of insert OP should have been performed by
          // insert_or_assign.
          if (res.second) {
            LOG(WARNING) << "ReplacementSLRU::LRUMaintainerTask: insert "
                            "instead of assignment "
                         << "OP performed in policy index! (hot -> warm). Key: "
                         << key;
          }
        } else {
          // move to cold lru
          if (!cold_lru_list.list_lock.try_lock()) {
            index_.map_unlock(key);
            contention_find = true;
            break;
          }
          if (!hot_lru_list.list_lock.try_lock()) {
            cold_lru_list.list_lock.unlock();
            index_.map_unlock(key);
            contention_find = true;
            break;
          }

          cold_lru_list.list_.emplace_front(key);
          hot_lru_list.list_.pop_back();
          auto res = index_.insert_or_assign(
              key, Node(COLD_LRU,
                        std::make_shared<std::atomic<uint16_t>>(BUFFER_INIT),
                        buffer_ptr, cold_lru_list.list_.begin()));
          hot_lru_list.list_used_ -= (val_size + key.size());
          cold_lru_list.list_used_ += (val_size + key.size());

          hot_lru_list.list_lock.unlock();
          cold_lru_list.list_lock.unlock();

          // As the buffer is moved from hot list to cold list, an assignment
          // instead of insert OP should have been performed by
          // insert_or_assign.
          if (res.second) {
            LOG(WARNING) << "ReplacementSLRU::LRUMaintainerTask: insert "
                            "instead of assignment "
                         << "OP performed in policy index! (cold -> hot). Key: "
                         << key;
          }
          if (FLAGS_cache_slru_ut) {
            TEST_NotifyMaintainerMoveComplete();
          }
        }
        index_.map_unlock(key);
      }

      if (contention_find) continue;
      // step 2 update warm lru
      auto warm_limit = GetSegmentByteLimit() * FLAGS_warm_lru_pct / 100;

      while (GetListUsedSize(segment_id, WARM_LRU) > warm_limit) {
        if (!warm_lru_list.list_lock.try_lock()) {
          contention_find = true;
          break;
        }

        if (warm_lru_list.list_.empty()) {
          warm_lru_list.list_lock.unlock();
          break;
        }

        const std::string key = warm_lru_list.list_.back();

        if (!index_.map_trylock(key)) {
          warm_lru_list.list_lock.unlock();
          contention_find = true;
          break;
        }
        auto map_iter = index_.find(key);
        if (map_iter == index_.end()) {
          warm_lru_list.list_.pop_back();
          index_.map_unlock(key);
          warm_lru_list.list_lock.unlock();
          break;
        }
        warm_lru_list.list_lock.unlock();

        CHECK_EQ(WARM_LRU, map_iter->second.lru_clsid);
        CHECK(map_iter->second.list_iter ==
              std::prev(warm_lru_list.list_.end()));

        size_t val_size = map_iter->second.buffer_->Size();
        auto buffer_ptr = map_iter->second.buffer_;

        if (GetNodeBufferFlag(map_iter->second) == BUFFER_ACTIVE) {
          // send to the head of warm lru
          // if we cannot get the lock, then try it later
          if (!warm_lru_list.list_lock.try_lock()) {
            index_.map_unlock(key);
            contention_find = true;
            break;
          }

          warm_lru_list.list_.emplace_front(key);
          warm_lru_list.list_.pop_back();
          auto res = index_.insert_or_assign(
              key, Node(WARM_LRU,
                        std::make_shared<std::atomic<uint16_t>>(BUFFER_INIT),
                        buffer_ptr, warm_lru_list.list_.begin()));

          warm_lru_list.list_lock.unlock();

          // As the buffer is moved within the warm list from tail to head, an
          // assignment instead of insert OP should have been performed by
          // insert_or_assign.
          if (res.second) {
            LOG(WARNING)
                << "ReplacementSLRU::LRUMaintainerTask: insert instead of "
                   "assignment "
                << "OP performed in policy index! (warm -> warm). Key: " << key;
          }
        } else {
          // move to cold lru
          // if we cannot get the lock, then try it later
          if (!cold_lru_list.list_lock.try_lock()) {
            index_.map_unlock(key);
            contention_find = true;
            break;
          }
          if (!warm_lru_list.list_lock.try_lock()) {
            cold_lru_list.list_lock.unlock();
            index_.map_unlock(key);
            contention_find = true;
            break;
          }

          cold_lru_list.list_.emplace_front(key);
          warm_lru_list.list_.pop_back();
          auto res = index_.insert_or_assign(
              key, Node(COLD_LRU,
                        std::make_shared<std::atomic<uint16_t>>(BUFFER_INIT),
                        buffer_ptr, cold_lru_list.list_.begin()));
          warm_lru_list.list_used_ -= (val_size + key.size());
          cold_lru_list.list_used_ += (val_size + key.size());

          warm_lru_list.list_lock.unlock();
          cold_lru_list.list_lock.unlock();

          // As the buffer is moved from the warm list to the cold list, an
          // assignment instead of insert OP should have been performed by
          // insert_or_assign.
          if (res.second) {
            LOG(WARNING)
                << "ReplacementSLRU::LRUMaintainerTask: insert instead of "
                   "assignment "
                << "OP performed in policy index! (warm -> cold ). Key: "
                << key;
          }
        }
        index_.map_unlock(key);
      }

      if (contention_find) continue;
      // step 3 update cold lru
      while (GetSegmentUsedSize(segment_id) > GetSegmentByteLimit()) {
        if (!cold_lru_list.list_lock.try_lock()) {
          break;
        }
        if (cold_lru_list.list_.empty()) {
          cold_lru_list.list_lock.unlock();
          break;
        }
        const std::string key = cold_lru_list.list_.back();

        if (!index_.map_trylock(key)) {
          cold_lru_list.list_lock.unlock();
          contention_find = true;
          break;
        }
        auto map_iter = index_.find(key);
        if (map_iter == index_.end()) {
          cold_lru_list.list_.pop_back();
          index_.map_unlock(key);
          cold_lru_list.list_lock.unlock();
          break;
        }
        cold_lru_list.list_lock.unlock();

        CHECK_EQ(COLD_LRU, map_iter->second.lru_clsid);
        CHECK(map_iter->second.list_iter ==
              std::prev(cold_lru_list.list_.end()));

        size_t val_size = map_iter->second.buffer_->Size();
        auto buffer_ptr = map_iter->second.buffer_;

        if (GetNodeBufferFlag(map_iter->second) == BUFFER_ACTIVE) {
          if (!cold_lru_list.list_lock.try_lock()) {
            index_.map_unlock(key);
            break;
          }
          if (!warm_lru_list.list_lock.try_lock()) {
            cold_lru_list.list_lock.unlock();
            index_.map_unlock(key);
            break;
          }

          // move it to warm lru
          warm_lru_list.list_.emplace_front(key);
          cold_lru_list.list_.pop_back();

          auto res = index_.insert_or_assign(
              key, Node(WARM_LRU,
                        std::make_shared<std::atomic<uint16_t>>(BUFFER_INIT),
                        buffer_ptr, warm_lru_list.list_.begin()));
          cold_lru_list.list_used_ -= (val_size + key.size());
          warm_lru_list.list_used_ += (val_size + key.size());

          warm_lru_list.list_lock.unlock();
          cold_lru_list.list_lock.unlock();

          // As the buffer is moved from the cold list to the warm list, an
          // assignment instead of insert OP should have been performed by
          // insert_or_assign.
          if (res.second) {
            LOG(WARNING)
                << "ReplacementSLRU::LRUMaintainerTask: insert instead of "
                   "assignment "
                << "OP performed in policy index! (cold -> warm). Key: " << key;
          }
        } else {
          if (!cold_lru_list.list_lock.try_lock()) {
            index_.map_unlock(key);
            break;
          }

          cold_lru_list.list_.pop_back();
          cold_lru_list.list_used_ -= (val_size + key.size());
          lru_lists_[segment_id].used_ -= (val_size + key.size());
          index_.erase(map_iter);

          cold_lru_list.list_lock.unlock();
        }
        index_.map_unlock(key);
      }
    }
    // Break the `while` loop here rather than using
    // `while (CheckEnableLRUMaintainer())` at the begining. Otherwise this
    // thread will still sleep for 2000ms before exit if the
    // `nofity_all` signal is sent during the while loop.
    if (!CheckEnableLRUMaintainer()) break;
    std::unique_lock<std::mutex> lk(enable_lru_maintainer_mtx_);
    if (enable_lru_maintainer_cv_.wait_for(
            lk, std::chrono::milliseconds(2000),
            [this] { return !CheckEnableLRUMaintainer(); })) {
      break;
    }
  }
}

int ReplacementSLRU::StartLRUMaintainer() {
  CHECK(initialized_);
  SetLRUMaintainerInit(true);
  SetEnableLRUMaintainer(true);
  if (CheckLRUMaintainerInit()) {
    lru_maintainer_ = std::thread([&]() { LRUMaintainerTask(); });
  }
  return 0;
}

int ReplacementSLRU::StopLRUMaintainer() {
  SetEnableLRUMaintainer(false);
  if (CheckLRUMaintainerInit()) {
    lru_maintainer_.join();
    delete[] lru_lists_;
    SetLRUMaintainerInit(false);
  }
  return 0;
}

void ReplacementSLRU::SetEnableLRUMaintainer(bool status) {
  std::lock_guard<std::mutex> lk(enable_lru_maintainer_mtx_);
  enable_lru_maintainer_.store(status, std::memory_order_release);
  if (!status) {
    enable_lru_maintainer_cv_.notify_all();
  }
}

uint16_t ReplacementSLRU::TEST_CheckLRUPos(std::string key) {
  auto iter = index_.find(key);
  // update buffer reference flag if needed
  if (iter != index_.end()) {
    // locate the list
    auto lru_id = 0;
    if (iter->second.lru_clsid == HOT_LRU) {
      lru_id = 0;
    } else if (iter->second.lru_clsid == WARM_LRU) {
      lru_id = 1;
    } else {
      lru_id = 2;
    }
    return lru_id;
  } else {
    return -1;
  }
}

uint16_t ReplacementSLRU::TEST_CheckBufferFlag(std::string key) {
  auto iter = index_.find(key);
  // update buffer reference flag if needed
  if (iter != index_.end()) {
    return GetNodeBufferFlag(iter->second);
  } else {
    return -1;
  }
}

void ReplacementSLRU::TEST_ConfigLRUMaintainer(bool status) {
  SetEnableLRUMaintainer(status);
}

void ReplacementSLRU::TEST_WaitForLRUMaintainer() {
  std::unique_lock<std::mutex> lck(TEST_slru_ut_mtx_);
  TEST_did_maintainer_move_.wait(lck);
}

void ReplacementSLRU::TEST_NotifyMaintainerMoveComplete() {
  std::unique_lock<std::mutex> lck(TEST_slru_ut_mtx_);
  TEST_did_maintainer_move_.notify_all();
}

}  // namespace mtcache
