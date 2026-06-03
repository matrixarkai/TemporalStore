#pragma once

#include "map/cache_map.h"
#include "replacement_policy.h"

#include <list>
#include <mutex>

#define BUFFER_INIT 4
// Buffer was fetched at least once in its lifetime
#define BUFFER_FETCHED 8
// Appended on fetch, removed on LRU shuffling
#define BUFFER_ACTIVE 16

#define HOT_LRU 0
#define WARM_LRU 1
#define COLD_LRU 2

namespace mtcache {

// ReplacementSLRU implements a segmented LRU policy.
// There are three LRU lists: hot lru, warm lru and cold lru
// new inserted buffers are always inserted into hot lru
// At the tail of hot lru, if an buffer has been touched twice,
// it will be sent to warm lru, otherwise, it will be sent to
// cold lru.
// At the tail of warm lru, if an buffer has been touched twice,
// it will be sent to the head of warm lru. otherwise, it will be
// sent to cold lru.
// At the tail of cold lru, if an buffer has been touched twice,
// it will be sent to the warm lru, otherwise, it will be deleted.
class ReplacementSLRU : public ReplacementPolicy {
 public:
  // Constructor instantiate a SLRU replacement policy with the specified
  // capacity.
  explicit ReplacementSLRU(size_t capacity) : ReplacementPolicy(capacity) {}

  // destructor
  ~ReplacementSLRU() {
    // destory maintainer_thread
    StopLRUMaintainer();
  }

  // Detailed comments for the overriding functions below could be found in
  // the parent class in cache_replacement_policy.h
  noodle::Result<void, CacheError> Init() override;

  noodle::Result<void, CacheError> Reset() override;

  std::vector<CacheBufferSharedPtr> Put(
      CacheBufferSharedPtr buffer_ptr) override;

  noodle::Result<void, CacheError> UpdateCacheBuffer(
      const std::string& key, const char* old_data_ptr,
      CacheBufferSharedPtr buffer_ptr) override;

  CacheBufferSharedPtr Get(const std::string& key) override;

  CacheBufferSharedPtr Peek(const std::string& key) override;

  CacheBufferSharedPtr Delete(const std::string& key) override;

  void SetCapacity(size_t new_capacity) override {
    CHECK_GT(new_capacity, 0);
    capacity_ = new_capacity;
    UpdateSegmentByteLimit();
  }

  size_t GetUsedSpace() override {
    size_t cur_used_ = 0;
    for (int i = 0; i < num_segments_; i++) {
      cur_used_ += GetSegmentUsedSize(i);
    }
    used_ = cur_used_;
    return used_;
  }

  // GetFreeSpace returns how much space is left for the replacement policy to
  // install new cache buffers.
  size_t GetFreeSpace() override {
    GetUsedSpace();
    return capacity_ >= used_ ? (capacity_ - used_) : 0;
  }

  size_t GetItemNum() override { return index_.size(); }

  // UT interfaces
  uint16_t TEST_CheckLRUPos(std::string key);

  uint16_t TEST_CheckBufferFlag(std::string key);

  void TEST_ConfigLRUMaintainer(bool status);

  void TEST_WaitForLRUMaintainer();

  void TEST_NotifyMaintainerMoveComplete();

 private:
  struct Node;
  // Evict cache buffers
  void Evict(uint64_t segment_id, std::vector<CacheBufferSharedPtr>* evicted);
  // Dump cache buffer from the target lru list
  // use it when no cache buffer in the cold lru list
  void Dump(uint64_t segment_id, int cur_lru,
            std::vector<CacheBufferSharedPtr>* evicted);

  // The number of LRU lists does not have to be equal to the number of segments
  // in the index hashmap
  uint64_t PickSegment(const std::string& key) const {
    auto h = std::hash<std::string>()(key);
    return h & (num_segments_ - 1);
  }

  int StartLRUMaintainer();

  int StopLRUMaintainer();

  void LRUMaintainerTask();

  size_t GetSegmentUsedSize(int segment_id) {
    return lru_lists_[segment_id].used_.load(std::memory_order_acquire);
  }

  size_t GetSegmentByteLimit() {
    return bytes_each_segment_.load(std::memory_order_acquire);
  }

  void UpdateSegmentByteLimit() {
    auto new_bytes_each_segment_ = capacity_ / num_segments_;
    bytes_each_segment_.store(new_bytes_each_segment_,
                              std::memory_order_release);
  }

  bool CheckLRUMaintainerInit() {
    return lru_maintainer_initialized_.load(std::memory_order_acquire);
  }

  void SetLRUMaintainerInit(bool status) {
    lru_maintainer_initialized_.store(status, std::memory_order_release);
  }

  bool CheckEnableLRUMaintainer() {
    return enable_lru_maintainer_.load(std::memory_order_acquire);
  }

  void SetEnableLRUMaintainer(bool status);

  size_t GetListUsedSize(int segment_id, int lru_id) {
    return lru_lists_[segment_id].lists_[lru_id].list_used_.load(
        std::memory_order_acquire);
  }

  void SetNodeBufferFlag(Node target_node, uint16_t new_flag) {
    target_node.it_flag.get()->store(new_flag, std::memory_order_release);
  }

  uint16_t GetNodeBufferFlag(Node target_node) {
    return target_node.it_flag.get()->load(std::memory_order_acquire);
  }

 private:
  struct Node {
    Node(uint16_t p_lru_clsid, std::shared_ptr<std::atomic<uint16_t>> p_it_flag,
         CacheBufferSharedPtr p_buffer, std::list<std::string>::iterator p_iter)
        : lru_clsid(p_lru_clsid),
          it_flag(p_it_flag),
          buffer_(p_buffer),
          list_iter(p_iter) {}

    uint16_t lru_clsid{HOT_LRU};
    std::shared_ptr<std::atomic<uint16_t>>
        it_flag;  // Fetched: access 1 time; Active: access 2 times
    CacheBufferSharedPtr buffer_{nullptr};
    std::list<std::string>::iterator list_iter;
  };

  struct LRUList {
    // TODO(kaiwu.kw) Storing key is not space-efficient. Instead, we can store
    // a pointer to the Node.
    std::list<std::string> list_;
    std::atomic<size_t> list_used_{0};
    mutable std::mutex list_lock;
  };

  struct SLRUList {
    // Each SLRU list contains three sub-lists: hot/warm/cold lru lists
    LRUList lists_[3];
    std::atomic<size_t> used_{0};
  };

  // index map
  ConcurrentHashMap<std::string, Node> index_;
  int num_segments_;
  std::atomic<uint64_t> bytes_each_segment_;
  SLRUList* lru_lists_;

  // SLRU background maintainer variables
  std::thread lru_maintainer_;
  std::atomic<bool> lru_maintainer_initialized_;
  std::atomic<bool> enable_lru_maintainer_;

  // The mutex for `gc_monitor_cv_`
  std::mutex enable_lru_maintainer_mtx_;

  // The condition variable that controls whether to exit lru maintainer thread.
  // Without this cv, the `lru_maintainer_` thread can also check the
  // status of `enable_lru_maintainer_` at some interval and then decide
  // whether to exit. But in this way it will wait for at most
  // 'interval' time after calling StopLRUMaintainer() and it will cost
  // much time when running UT.
  std::condition_variable enable_lru_maintainer_cv_;

  // UT variables
  std::mutex TEST_slru_ut_mtx_;
  std::condition_variable TEST_did_maintainer_move_;
};

}  // namespace mtcache
