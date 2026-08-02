// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "blockcache/rdmareadcache/storage/storage_engine_dram.h"

#include <memory>
#include <string>

namespace bcache2 {

template <typename K, typename V>
StorageEngineDram<K, V>::StorageEngineDram(AllocatorType type, size_t cap) {
    alloc_type_ = type;
    switch (type) {
    case AllocatorType::Standard:
        allocator_ = new StdAllocator();
        break;
    default:
        perror("Allocator unimplemented\n");
        return;
    }
}

template <typename K, typename V>
StorageEngineDram<K, V>::~StorageEngineDram() {
    delete (allocator_);
}

template <typename K, typename V>
int StorageEngineDram<K, V>::Get(const K& key, size_t sz, RDMAResponse* resp, char* addr) {
    // | KEY_LEN | VAL_LEN | KEY | VAL | CRC |
    // |   8 B       8 B   |           | 8 B |
    // Dereference the first two 8 bytes in the data block
    // to get the key length and value length
    uint64_t k_len = *reinterpret_cast<uint64_t*>(addr);
    uint64_t v_len = *reinterpret_cast<uint64_t*>(addr + KEY_LEN);
    // When sz == 0, data block size is too large to fit into the address field
    // in the index entry, so read its size from the data block.
    if (sz == 0) {
        sz = k_len + v_len + DATA_HEADER + CRC_LEN;
    }
    // load the value into the resp
    resp->Fill(addr + DATA_HEADER + k_len, v_len);

    // verify again in case the data block is reclaimed
    int eq = VerifyKey(addr + DATA_HEADER, reinterpret_cast<const char*>(&key), k_len);
    if (!eq) {
        return NOT_FOUND;
    }
    // verify the CRC
    uint64_t crc = DataCRC(addr, sz - CRC_LEN);
    eq = VerifyCRC(addr, sz, crc);
    // return if the CRC does not match
    if (!eq) {
        return CRC_MISMATCH;
    }
    return OP_SUCCESS;
}

template <typename K, typename V>
char* StorageEngineDram<K, V>::Put(const K& key, const V& val) {
    uint64_t k_len = sizeof(key);
    uint64_t v_len = sizeof(val);
    char* addr =
        reinterpret_cast<char*>(allocator_->allocate(DATA_HEADER + k_len + v_len + CRC_LEN));
    if (addr == nullptr) {
        return nullptr;
    }
    // set the header field
    *reinterpret_cast<uint64_t*>(addr) = k_len;
    *reinterpret_cast<uint64_t*>(addr + KEY_LEN) = v_len;
    // store the key value
    memcpy(addr + DATA_HEADER, &key, k_len);
    memcpy(addr + DATA_HEADER + k_len, &val, v_len);
    // compute and set the CRC
    uint64_t crc = DataCRC(addr, DATA_HEADER + k_len + v_len);
    memcpy(addr + DATA_HEADER + k_len + v_len, &crc, sizeof(crc));
    return addr;
}

template <typename K, typename V>
int StorageEngineDram<K, V>::Del(char* addr, size_t len) {
    // the index region has already match the full key
    // we do not need to compare the full key agian
    // delete operation is protected by a lock, no data race
    allocator_->free(addr, len);
    return OP_SUCCESS;
}

template <typename K, typename V>
void StorageEngineDram<K, V>::Stats() {
    // TODO(mingzhe.du): implement Stats()
}

template class StorageEngineDram<int, int>;
template class StorageEngineDram<std::string, std::string>;
}  // namespace bcache2
