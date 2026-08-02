// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <emmintrin.h>
#include <immintrin.h>
#include <x86intrin.h>

#include <atomic>
#include <bitset>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>

#include "blockcache/rdmareadcache/index/utils.h"
#include "blockcache/rdmareadcache/storage/storage_engine.h"

namespace bcache2 {

template <typename K, typename V>
class Bucket;
template <typename K, typename V>
class HashTable;
template <typename K, typename V>
class Entry;

template <typename K, typename V>
class BucketHeader {
    char finger_print[BUCKET_CAP];  // store 8-bit fingerprints for keys
    char reserved[BUCKET_CAP];      // reserved for future usage
    std::atomic<uint16_t> bitmap;   // 15 bits bitmap | 1 bit for locking the entire bucket
    friend class HashTable<K, V>;
    friend class Bucket<K, V>;
    friend class Entry<K, V>;

 public:
    BucketHeader() {
        bitmap.store(0);
        for (int i = 0; i < BUCKET_CAP; ++i) {
            finger_print[i] = 0;
            reserved[i] = 0;
        }
    }
    BucketHeader(const BucketHeader& h) {
        bitmap.store(h.bitmap.load());
        for (int i = 0; i < BUCKET_CAP; ++i) {
            finger_print[i] = h.finger_print[i];
            reserved[i] = h.reserved[i];
        }
    }
    BucketHeader& operator=(const BucketHeader& h) {
        bitmap.store(h.bitmap.load());
        for (int i = 0; i < BUCKET_CAP; ++i) {
            finger_print[i] = h.finger_print[i];
            reserved[i] = h.reserved[i];
        }
        return *this;
    }
    /**
     * @brief get the bitmap information which represents the entry occupation status in a bucket
     *
     * @return uint16_t
     */
    uint16_t GetBitmap();
};

template <typename K, typename V>
class Entry {
    std::bitset<128> signature;  // if data is on PM/DRAM, signature uses 96 bits, another 32
                                 // bits are used for rkey
    uint64_t addr;   // 48-bit ptr | 8-bit CRC | 2-bit type | 1-bit overflow flag | 5-bit reserved
    int32_t length;  // total length of key and value, if kv size exceeds 2^32, overflow flag is set
    int32_t version;
    friend class Bucket<K, V>;
    friend class HashTable<K, V>;

 public:
    Entry() : signature(0x0), addr(0), length(0), version(-1) {}
    Entry(const Entry& e) {
        signature = e.signature;
        addr = e.addr;
        length = e.length;
        version = e.version;
    }
    Entry& operator=(const Entry& e) {
        signature = e.signature;
        addr = e.addr;
        length = e.length;
        version = e.version;
        return *this;
    }

    // helper functions
    /**
     * @brief get the data block address stored in the entry
     *
     * @return data block address in the storage engine
     */
    char* GetPtr();
    /**
     * @brief get the data block CRC stored in the entry
     *
     * @return data block crc
     */
    unsigned char GetCRC();
    /**
     * @brief get the storage engine type where the data block resides in
     *
     * @return storage engine type
     */
    uint8_t GetType();
    /**
     * @brief get the overflow flag
     *
     * @return overflow flag
     */
    int GetOverflowFlag();
    /**
     * @brief get the key signature
     *
     * @return key signature
     */
    std::bitset<128> GetSignature128b();
    std::bitset<96> GetSignature96b();
    /**
     * @brief set the key signature field
     *
     * @param sig is the pre-computed key signature
     */
    void SetSignature(const std::bitset<96>& sig);
    void SetSignature(const std::bitset<128>& sig);
    /**
     * @brief get the data block size in the storage engine
     *
     * @return the total size of both key and value
     */
    int GetLength();
    /**
     * @brief set the length field to be the data block size
     *
     * @param l the key valye size stored in the storage engine
     */
    void SetDataLength(int l);
    /**
     * @brief get the data block version number
     *
     * @return version number
     */
    int GetVersion();
    /**
     * @brief set the data block version number field
     *
     */
    void SetVersion();
    /**
     * @brief get the address field in the entry (different from data block pointer), which includes
     * the data block pointer, overflow flag, storage engine type, etc.,
     *
     * @return the address field
     */
    uint64_t GetAddr();
    /**
     * @brief set the entry address field
     *
     * @param a
     */
    void SetAddr(uint64_t a);
    /* TODO(mingzhe.du): unimplemented yet */
    int GetRkey(char* str);
    void SetRkey(const std::bitset<128>& signature) {}
};

template <typename K, typename V>
class Bucket {
    BucketHeader<K, V> metadata;
    Entry<K, V> entries[BUCKET_CAP];
    friend class HashTable<K, V>;

 public:
    Bucket() {
        metadata = BucketHeader<K, V>();
        for (int i = 0; i < BUCKET_CAP; ++i) {
            entries[i] = Entry<K, V>();
        }
    }

    Bucket& operator=(const Bucket& b) {
        metadata = b.metadata;
        for (int i = 0; i < BUCKET_CAP; ++i) {
            entries[i] = b.entries[i];
        }
        return *this;
    }

    Bucket(const Bucket& b) {
        metadata = b.metadata;
        for (int i = 0; i < BUCKET_CAP; ++i) {
            entries[i] = b.entries[i];
        }
    }

    /**
     * @brief lock a target bucket
     *
     * @return true
     * @return false
     */
    bool LockBucket();
    /**
     * @brief unlock a locked bucket
     *
     */
    void UnlockBucket();
    /**
     * @brief get an empty entry from the bucket
     *
     * @return the index of the empty entry
     */
    int GetEmptyEntry();
    /**
     * @brief evict one entry from the bucket
     *
     * @return the index of the evicted entry
     */
    int EvictEntry();
    /**
     * @brief occupy one entry in the bucket
     *
     * @param pos is the index of the occupied entry
     */
    void OccupyEntry(int pos);
    /**
     * @brief remove an entry from the bucket
     *
     * @param pos is the index of the cleared entry
     */
    void ClearEntry(int pos);
    /**
     * @brief get the index position in a bucket
     *
     * @param key is the target key
     * @param sig1 is the 96-bit key signature
     * @param sig2 is the 128-bit key signature
     * @return the position of the entry in the bucket
     */
    int GetIndex(const K& key, std::bitset<96>* sig1, std::bitset<128>* sig2);
    /**
     * @brief get the metadata of a bucket
     *
     * @return metadata of a bucket
     */
    BucketHeader<K, V>* GetMetadata();
    /**
     * @brief get an entry at position i from the bucket
     *
     * @param i is the index of the entry
     * @return a target entry
     */
    Entry<K, V>* GetEntry(int i);
    /**
     * @brief get number of occupied entries in a bucket
     *
     * @return the number of occupied entries in a bucket
     */
    uint64_t GetOccupiedEntryNum();
    /**
     * @brief check if a bucket is locked
     *
     * @return true if locked
     * @return false if not
     */
    bool IsLocked();
};
}  // namespace bcache2
