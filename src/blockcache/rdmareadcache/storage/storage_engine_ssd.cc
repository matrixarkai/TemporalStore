// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "blockcache/rdmareadcache/storage/storage_engine_ssd.h"

#include <memory>
#include <string>

namespace bcache2 {

template <typename K, typename V>
StorageEngineSSD<K, V>::StorageEngineSSD(AllocatorType type, size_t cap) {
    // TODO(mingzhe.du): initiate storage engine constructor
}

template <typename K, typename V>
StorageEngineSSD<K, V>::~StorageEngineSSD() {
    // TODO(mingzhe.du): initiate storage engine destructor
}

template <typename K, typename V>
int StorageEngineSSD<K, V>::Get(const K& key, size_t sz, RDMAResponse* resp, char* addr) {
    // TODO(mingzhe.du): get data from SSD storage engine
    return OP_SUCCESS;
}

template <typename K, typename V>
char* StorageEngineSSD<K, V>::Put(const K& key, const V& val) {
    // TODO(mingzhe.du): put data into SSD storage engine
    return nullptr;
}

template <typename K, typename V>
int StorageEngineSSD<K, V>::Del(char* addr, size_t len) {
    // TODO(mingzhe.du): delete data from SSD storage engine
    return OP_SUCCESS;
}

template <typename K, typename V>
void StorageEngineSSD<K, V>::Stats() {
    // TODO(mingzhe.du): implement Stats()
}

template class StorageEngineSSD<std::string, std::string>;
template class StorageEngineSSD<int, int>;

}  // namespace bcache2
