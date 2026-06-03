#pragma once

#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/io/IOBuf.h>

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <variant>

namespace mtcache {

class Index {
 public:
  // Index's value doesn't store the memory or lba address directly
  // Value should first be resolved as described below:
  // [1]. Memory: `shared_ptr` => Value of user record
  // [2]. SSD:
  //    64  =    43     +    12    +     7     +     2
  //            lba         size      reserved     state
  // Note: `shared_ptr` is used to prevent user from visiting corrupted
  // memory address of `IOBuf` after `IOBuf` is flushed.
  enum RecordStateType : uint8_t {
    kSoftDel = 0x0,
    kNormal = 0x1,
    kPinned = 0x2,
    kMaxCode = 0xf
  };
  using SSDColoredPtr = uint64_t;
  using ValueMemoryType = std::shared_ptr<folly::IOBuf>;
  using MemoryColoredPtr = std::pair<ValueMemoryType, RecordStateType>;
  using ValueType = std::variant<SSDColoredPtr, MemoryColoredPtr>;
  // Other class uses `Index` in the form of callback funtion(except
  // `StorageEngineZonedStore`).
  // `CondDeleteCallback` is used by `GCWorker`, `GCEntryCallback`
  // contains logic to determine if entry to which key corresponds
  // should be deleted.
  // `OnGetEntry` is used to get entry to which key corresponds.
  // `UpdateEntryCallback` is used to update certain entry, however it
  // won't insert new entry if key doesn't exist.
  using GCEntryCallback = std::function<bool(RecordStateType)>;
  using CondDeleteCallback =
      std::function<bool(const std::string&, GCEntryCallback)>;
  using OnGetEntry = std::function<ValueType(const std::string&)>;
  using UpdateEntryCallback =
      std::function<bool(const std::string&, ValueType)>;

 private:
  ::folly::ConcurrentHashMap<std::string, ValueType> hash_map_;

 public:
  virtual ~Index() = default;

  // After support **Record State** feature,
  // only value pointer is updated, state is determinated by `Index`'s
  // current state.
  virtual bool UpdateIndex(const std::string& key, ValueType value);

  // Only for inserting new record.
  // 1. StorageEngineZonedStore::Put
  // 2. Recovery.
  virtual void Put(const std::string& key, Index::ValueType value);

  // If entry's state is `kSoftDel`, change to `kNormal`.
  virtual ValueType Get(const std::string& key);

  // Change state from `kPinned` to `kNormal`.
  // Otherwise no operation is performed.
  virtual void UnPin(const std::string& key);

  // Change entry's state from `kSoftDel` or `kNormal`
  // to `kPinned`.
  virtual bool Pin(const std::string& key);

  // Don't actually delete entry, just mark it
  // as `kSoftDel`.
  // `kPinned` entry can't be deleted directly.
  virtual void SoftDelete(const std::string& key);

  // Atomically check entry's state and delete it if `pred`
  // returns true.
  // `pred` use recycling mode and entry's state to determine whether
  // deletes it or not.
  // Used in `GCWorker`.
  virtual bool DeleteIf(const std::string& key, GCEntryCallback pred);

  template<typename Func>
  void ScanIndexForRecover(const Func& func) {
    for (auto it = hash_map_.cbegin(); it != hash_map_.end(); ++it) {
      func(it->first, it->second);
    }
  }
};

class IndexUpdater {
 public:
  explicit IndexUpdater(std::shared_ptr<Index> index)
      : index_(std::move(index)) {}
  virtual ~IndexUpdater() = default;

  // Below methods are just wrapper of `Index`'s methods.
  virtual bool DeleteIf(const std::string& key, Index::GCEntryCallback pred) {
    return index_->DeleteIf(key, pred);
  }
  virtual Index::ValueType Get(const std::string& key) {
    return index_->Get(key);
  }
  virtual bool UpdateIndex(const std::string& key, Index::ValueType value) {
    return index_->UpdateIndex(key, value);
  }

 private:
  std::shared_ptr<Index> index_;
};

}  // namespace mtcache
