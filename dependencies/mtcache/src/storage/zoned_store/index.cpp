#include "storage/zoned_store/index.h"

#include "common/logging.h"
#include "storage/zoned_store/util.h"
#include "storage/zoned_store/zoned_store.h"

#include <utility>
#include <variant>

namespace mtcache {

// FIXME(fangliming) : Now all entries' state is `Index::kSoftDel`,
bool Index::UpdateIndex(const std::string& key, ValueType value) {
  DCHECK_GT(key.size(), 0) << "key size must > 0";
  DCHECK(hash_map_.find(key) != hash_map_.end());
  auto option = hash_map_.assign(key, std::move(value));
  return option.hasValue();
}

void Index::Put(const std::string& key, Index::ValueType value) {
  DCHECK_GT(key.size(), 0) << "key size must > 0";
  hash_map_.emplace(key, value);
}

Index::ValueType Index::Get(const std::string& key) {
  DCHECK_GT(key.size(), 0) << "key size must > 0";
  auto result = hash_map_.find(key);
  if (result == hash_map_.end()) {
    return StorageEngineZonedStore::kNotExist;
  }
  return (*result).second;
}

// FIXME(fanglimnig) : use pred.
bool Index::DeleteIf(const std::string& key, GCEntryCallback pred) {
  DCHECK_GT(key.size(), 0) << "key size must > 0";
  hash_map_.erase(key);
  return true;
}

// TODO(fangliming) : implement it.
void Index::UnPin(const std::string& key) {}

// TODO(fangliming) : implement it.
bool Index::Pin(const std::string& key) { return true; }

// TODO(fangliming) : implement it.
void Index::SoftDelete(const std::string& key) {
  // Do nothing, because now all state is`Index::kSoftDel`.
}

}  // namespace mtcache
