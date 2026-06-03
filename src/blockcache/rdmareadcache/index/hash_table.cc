// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "blockcache/rdmareadcache/index/hash_table.h"

#include <string>

namespace bcache2 {

template <typename K, typename V>
char* HashTable<K, V>::Get(const K& key, size_t* len, uint8_t* type) {
    char* addr = nullptr;
    // get the bucket
    size_t n = std::hash<K>{}(key);
    uint64_t bucket_pos = n % BUCKET_NUM;
    Bucket<K, V>* bucket = &Buckets[n % BUCKET_NUM];

    std::bitset<96> k96;
    std::bitset<128> k128;
    Signature_96(key, bucket_pos, &k96);
    Signature_128(key, bucket_pos, &k128);

    int pos = bucket->GetIndex(key, &k96, &k128);
    if (pos != -1) {
        addr = reinterpret_cast<char*>(bucket->entries[pos].GetPtr());
        if (bucket->entries[pos].GetOverflowFlag() == true) {
            *len = 0;
        } else {
            *len = bucket->entries[pos].GetLength();
            *type = bucket->entries[pos].GetType();
        }
        return addr;
    }
    // key not found
    *type = static_cast<uint8_t>(bcache2::StorageEngineType::INVALID);
    return nullptr;
}

template <typename K, typename V>
int HashTable<K, V>::Put(const K& key, V val, size_t block_size, StorageEngineType type,
                         char** old_addr, size_t* old_sz, uint8_t* old_type) {
    // get the bucket
    size_t n = std::hash<K>{}(key);
    uint64_t bucket_pos = n % BUCKET_NUM;
    Bucket<K, V>* bucket = &Buckets[bucket_pos];

    if (bucket->LockBucket() == false) {
        return BUCKET_LOCKED;
    }

    // check if the key already existed
    std::bitset<96> k96;
    std::bitset<128> k128;
    Signature_96(key, bucket_pos, &k96);
    Signature_128(key, bucket_pos, &k128);
    int idx_pos = bucket->GetIndex(key, &k96, &k128);

    block_size += (DATA_HEADER + CRC_LEN);

    if (idx_pos != -1) {
        /* key already exists, update the index in place */
        // record the old address for memory reclamation
        *old_addr = bucket->entries[idx_pos].GetPtr();
        *old_type = bucket->entries[idx_pos].GetType();
        if (bucket->entries[idx_pos].GetOverflowFlag()) {
            // data block size overflows, get the data size from the
            // data block by de-referencing its content
            size_t k_sz = *reinterpret_cast<uint64_t*>(*old_addr);
            size_t v_sz = *reinterpret_cast<uint64_t*>(*old_addr + 8);
            *old_sz = k_sz + v_sz;
        } else {
            *old_sz = bucket->entries[idx_pos].GetLength();
        }
        bucket->entries[idx_pos].SetDataLength(block_size);
        bucket->entries[idx_pos].SetVersion();
        int overflow = block_size > MAX_BLOCK_SIZE ? 1 : 0;
        uint64_t new_addr = 0;
        // cannot use static_cast/reinterpret_cast for char*->uint64_t conversion
        new_addr = (uintptr_t)val << 16 | static_cast<int>(type) << 6 | overflow << 5;
        // set the address which does not contain CRC
        bucket->entries[idx_pos].SetAddr(new_addr);
        char crc = bcache2::EntryCRC(&(bucket->entries[idx_pos]));
        new_addr = new_addr | crc << 8;
        // set the CRC field in the new address
        bucket->entries[idx_pos].SetAddr(new_addr);
    } else {
        // key does not exist, use a new entry to store the information
        // get an empty slot
        int pos = bucket->GetEmptyEntry();
        if (pos == -1) {
            // if no empty slot available, evict one entry from the bucket and return the slot
            // number
            pos = bucket->EvictEntry();
        }
        // flip the bit corresponding to the entry in the bitmap
        bucket->OccupyEntry(pos);

        // init the corresponding entry in the bucket
        if (type == StorageEngineType::SSD) {
            bucket->entries[pos].SetSignature(k128);
        } else {
            bucket->entries[pos].SetSignature(k96);
        }
        bucket->entries[pos].SetDataLength(block_size);
        bucket->entries[pos].SetVersion();
        int overflow = block_size > MAX_BLOCK_SIZE ? 1 : 0;
        // cannot use static_cast/reinterpret_cast for char*->uint64_t conversion
        uint64_t addr = (uintptr_t)val << 16 | static_cast<int>(type) << 6 | overflow << 5;
        unsigned char crc = bcache2::EntryCRC(&(bucket->entries[pos]));
        addr = addr | (crc << 8);
        bucket->entries[pos].SetAddr(addr);

        /* insert the finger print of the key into the header */
        unsigned char fp = bcache2::HashCode1B(key);
        bucket->metadata.finger_print[pos] = fp;
    }
    bucket->UnlockBucket();
    return OP_SUCCESS;
}

template <typename K, typename V>
int HashTable<K, V>::Del(const K& key, char** addr, uint64_t* data_size, uint8_t* type) {
    // check if the key exists
    size_t n = std::hash<K>{}(key);
    uint64_t bucket_pos = n % BUCKET_NUM;
    Bucket<K, V>* bucket = &Buckets[bucket_pos];

    if (bucket->LockBucket() == false) {
        return BUCKET_LOCKED;
    }
    std::bitset<96> k96;
    std::bitset<128> k128;
    Signature_96(key, bucket_pos, &k96);
    Signature_128(key, bucket_pos, &k128);
    int idx_pos = bucket->GetIndex(key, &k96, &k128);
    if (idx_pos != -1) {
        *addr = bucket->entries[idx_pos].GetPtr();
        *data_size = bucket->entries[idx_pos].GetLength();
        *type = bucket->entries[idx_pos].GetType();
        bucket->ClearEntry(idx_pos);
        bucket->UnlockBucket();
        return OP_SUCCESS;
    }
    bucket->UnlockBucket();
    return NOT_FOUND;
}

template <typename K, typename V>
char* HashTable<K, V>::GetStartingAddr() {
    return reinterpret_cast<char*>(Buckets);
}

template <typename K, typename V>
size_t HashTable<K, V>::GetSize() {
    return BUCKET_NUM * BUCKET_SIZE;
}

template <typename K, typename V>
Bucket<K, V>* HashTable<K, V>::GetBucket(uint64_t i) {
    return &Buckets[i];
}

template <typename K, typename V>
uint64_t HashTable<K, V>::GetNumEntries() {
    uint64_t sum = 0;
    for (uint64_t i = 0; i < BUCKET_NUM; ++i) {
        sum += Buckets[i].GetOccupiedEntryNum();
    }
    return sum;
}

template <typename K, typename V>
bool HashTable<K, V>::AllBucketsUnlocked() {
    bool ret = true;
    for (uint64_t i = 0; i < BUCKET_NUM; ++i) {
        if (Buckets[i].IsLocked() == true) {
            ret = false;
        }
    }
    return ret;
}

template class HashTable<int, char*>;
template class HashTable<std::string, char*>;
}  // namespace bcache2
