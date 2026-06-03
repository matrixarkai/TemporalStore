// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "blockcache/rdmareadcache/rdma_cache.h"

#include <string>
namespace bcache2 {

template <typename K, typename V>
int RDMACache<K, V>::Lookup(const K& key, RDMAResponse* resp) {
    size_t sz;
    uint8_t type;
    char* addr = cache_index_->Get(key, &sz, &type);
    if (addr == nullptr) {
        return NOT_FOUND;
    }
    int status = 0;
    switch (static_cast<StorageEngineType>(type)) {
    case StorageEngineType::DRAM: {
        status = dram_engine_->Get(key, sz, resp, addr);
        break;
    }
    case StorageEngineType::PMEM: {
        status = pmem_engine_->Get(key, sz, resp, addr);
        break;
    }
    case StorageEngineType::SSD: {
        status = ssd_engine_->Get(key, sz, resp, addr);
        break;
    }
    default:
        perror("Invalid storage type");
    }
    return status;
}

template <typename K, typename V>
int RDMACache<K, V>::Insert(const K& key, const V& val) {
    // by default, data is cached in the DRAM buffer, therefore, we use dram_engine_
    char* addr = dram_engine_->Put(key, val);
    if (addr == nullptr) {
        return FAIL_ALLOC;
    }
    char* old_addr = nullptr;
    size_t old_sz = 0;
    uint8_t old_type = 3;  // 3 represents invalid storage engine type
    int status = cache_index_->Put(key, addr, sizeof(key) + sizeof(val), StorageEngineType::DRAM,
                                   &old_addr, &old_sz, &old_type);
    // if fail to update the index, delete kv pair from the storage engine
    if (status != OP_SUCCESS) {
        dram_engine_->Del(addr, DATA_HEADER + sizeof(key) + sizeof(val) + CRC_LEN);
    }
    // put operation replaces the old data block associated with the key with a new data block
    // old data block address and its size are stored in old_addr and old_sz, respectively
    // reclaim the old data block to avoid memory leak
    if (old_addr != nullptr) {
        if (old_sz > 0) {
            switch (static_cast<StorageEngineType>(old_type)) {
            case StorageEngineType::PMEM: {
                pmem_engine_->Del(old_addr, old_sz);
                break;
            }
            case StorageEngineType::SSD: {
                ssd_engine_->Del(old_addr, old_sz);
                break;
            }
            case StorageEngineType::DRAM: {
                dram_engine_->Del(old_addr, old_sz);
                break;
            }
            default:
                perror("Fail to deallocate old data block, invalid storage engine\n");
                return OP_FAIL;
            }
        }
    }
    return status;
}

template <typename K, typename V>
int RDMACache<K, V>::Insert(StorageEngineType type, const K& key, const V& val) {
    char* addr;
    switch (type) {
    case StorageEngineType::PMEM: {
        addr = pmem_engine_->Put(key, val);
        break;
    }
    case StorageEngineType::SSD: {
        addr = ssd_engine_->Put(key, val);
        break;
    }
    default:
        addr = dram_engine_->Put(key, val);
        break;
    }
    if (addr == nullptr) {
        return FAIL_ALLOC;
    }
    char* old_addr;
    size_t old_sz;
    uint8_t old_type;
    int status = cache_index_->Put(key, addr, sizeof(key) + sizeof(val), type, &old_addr, &old_sz,
                                   &old_type);
    if (status != OP_SUCCESS) {
        switch (type) {
        case StorageEngineType::PMEM: {
            pmem_engine_->Del(addr, DATA_HEADER + sizeof(key) + sizeof(val) + CRC_LEN);
            break;
        }
        case StorageEngineType::SSD: {
            ssd_engine_->Del(addr, DATA_HEADER + sizeof(key) + sizeof(val) + CRC_LEN);
            break;
        }
        default:
            dram_engine_->Del(addr, DATA_HEADER + sizeof(key) + sizeof(val) + CRC_LEN);
            break;
        }
    }
    if (old_addr != nullptr) {
        if (old_sz > 0) {
            switch (static_cast<StorageEngineType>(old_type)) {
            case StorageEngineType::PMEM: {
                pmem_engine_->Del(old_addr, old_sz);
                break;
            }
            case StorageEngineType::SSD: {
                ssd_engine_->Del(old_addr, old_sz);
                break;
            }
            case StorageEngineType::DRAM: {
                dram_engine_->Del(old_addr, old_sz);
                break;
            }
            default:
                perror("Fail to deallocate old data block, invalid storage engine\n");
                return OP_FAIL;
            }
        }
    }
    return status;
}

template <typename K, typename V>
int RDMACache<K, V>::Remove(const K& key) {
    char* addr;
    size_t sz;
    uint8_t type;
    int status = cache_index_->Del(key, &addr, &sz, &type);
    if (status != OP_SUCCESS) {
        return status;
    }
    switch (static_cast<StorageEngineType>(type)) {
    case StorageEngineType::PMEM: {
        status = pmem_engine_->Del(addr, sz);
        break;
    }
    case StorageEngineType::SSD: {
        status = ssd_engine_->Del(addr, sz);
        break;
    }
    default:
        status = dram_engine_->Del(addr, sz);
        break;
    }
    return status;
}

template <typename K, typename V>
size_t RDMACache<K, V>::GetCapacity(StorageEngineType type) {
    switch (type) {
    case StorageEngineType::DRAM: {
        return dram_capacity_;
    }
    case StorageEngineType::PMEM: {
        return pmem_capacity_;
    }
    case StorageEngineType::SSD: {
        return ssd_capacity_;
    }
    default:
        perror("Unsupported type\n");
        return -1;
    }
}

template <typename K, typename V>
void RDMACache<K, V>::InitStorageEngine(StorageEngineType type, size_t cap,
                                        AllocatorType alloc_type) {
    switch (type) {
    case StorageEngineType::DRAM: {
        dram_engine_ = new StorageEngineDram<K, V>(alloc_type, cap);
        dram_capacity_ = cap;
        dram_alloc_type_ = alloc_type;
        break;
    }
    case StorageEngineType::PMEM: {
        pmem_engine_ = new StorageEnginePmem<K, V>(alloc_type, cap);
        pmem_capacity_ = cap;
        pmem_alloc_type_ = alloc_type;
        break;
    }
    case StorageEngineType::SSD: {
        ssd_engine_ = new StorageEngineSSD<K, V>(alloc_type, cap);
        ssd_capacity_ = cap;
        ssd_alloc_type_ = alloc_type;
        break;
    }
    default:
        perror("Unsupported type\n");
        return;
    }
}

template <typename K, typename V>
ReplacementPolicyType RDMACache<K, V>::GetReplacementPolicyType() {
    return policy_type_;
}

template <typename K, typename V>
void RDMACache<K, V>::SetReplacementPolicy(ReplacementPolicyType policy) {
    switch (policy) {
    case ReplacementPolicyType::FIFO:
        replacement_policy_ = new ReplacementPolicyFIFO();
        break;
    case ReplacementPolicyType::LRU:
        replacement_policy_ = new ReplacementPolicyLRU();
        break;
    default:
        perror("Replacement Policy Unimplemented\n");
        return;
    }
    policy_type_ = policy;
}

template class RDMACache<int, int>;
template class RDMACache<std::string, std::string>;

}  // namespace bcache2
