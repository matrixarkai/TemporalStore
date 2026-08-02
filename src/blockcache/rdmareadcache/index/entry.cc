// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "blockcache/rdmareadcache/index/entry.h"

#include <ctime>
#include <string>

namespace bcache2 {

#if defined(__CYGWIN__)
static int rand_r(unsigned int* seed) {
    *seed = *seed * 1103515245u + 12345u;
    return static_cast<int>((*seed / 65536u) % 32768u);
}
#endif

// -----------------------class BucketHeader----------------------
template <typename K, typename V>
uint16_t BucketHeader<K, V>::GetBitmap() {
    return bitmap.load();
}

// -----------------------class Entry----------------------
template <typename K, typename V>
char* Entry<K, V>::GetPtr() {
    return reinterpret_cast<char*>((addr & 0xFFFFFFFFFFFF0000) >> 16);
}

template <typename K, typename V>
unsigned char Entry<K, V>::GetCRC() {
    return static_cast<char>((addr & 0xFF00) >> 8);
}

template <typename K, typename V>
uint8_t Entry<K, V>::GetType() {
    return static_cast<uint8_t>((addr & 0xC0) >> 6);  // 0:defualt(DRAM), 1:DRAM, 2:PM, 3:SSD
}

template <typename K, typename V>
int Entry<K, V>::GetOverflowFlag() {
    return static_cast<int>((addr & 0x20) >> 5);
}

template <typename K, typename V>
std::bitset<128> Entry<K, V>::GetSignature128b() {
    return signature;
}

template <typename K, typename V>
std::bitset<96> Entry<K, V>::GetSignature96b() {
    std::bitset<96> sig = 0;
    memcpy(&sig, &signature, 12);
    return sig;
}

template <typename K, typename V>
void Entry<K, V>::SetSignature(const std::bitset<96>& sig) {
    memcpy(&signature, &sig, 12);
}

template <typename K, typename V>
void Entry<K, V>::SetSignature(const std::bitset<128>& sig) {
    memcpy(&signature, &sig, 16);
}

template <typename K, typename V>
int Entry<K, V>::GetLength() {
    return length;
}

template <typename K, typename V>
void Entry<K, V>::SetDataLength(int l) {
    length = l;
}

template <typename K, typename V>
int Entry<K, V>::GetVersion() {
    return version;
}

template <typename K, typename V>
void Entry<K, V>::SetVersion() {
    version++;
}

template <typename K, typename V>
uint64_t Entry<K, V>::GetAddr() {
    return addr;
}

template <typename K, typename V>
void Entry<K, V>::SetAddr(uint64_t a) {
    addr = a;
}

template <typename K, typename V>
int Entry<K, V>::GetRkey(char* str) {
    // TODO(mingzhe.du)
    return 0;
}

// -----------------------class Entry----------------------
template <typename K, typename V>
BucketHeader<K, V>* Bucket<K, V>::GetMetadata() {
    return &metadata;
}

template <typename K, typename V>
Entry<K, V>* Bucket<K, V>::GetEntry(int i) {
    return &entries[i];
}

template <typename K, typename V>
uint64_t Bucket<K, V>::GetOccupiedEntryNum() {
    return CountBit(metadata.bitmap.load() >> 1);
}

template <typename K, typename V>
bool Bucket<K, V>::IsLocked() {
    if (metadata.bitmap.load() % 2 == 1) {
        return true;
    }
    return false;
}

template <typename K, typename V>
bool Bucket<K, V>::LockBucket() {
    uint16_t is_locked = metadata.bitmap.load();
    if (is_locked % 2 == 1) {
        // the bucket is already locked
        return false;
    }
    uint16_t lock = is_locked | 1;
    if (metadata.bitmap.compare_exchange_strong(is_locked, lock) == true) {
        return true;
    }

    return false;
}

template <typename K, typename V>
void Bucket<K, V>::UnlockBucket() {
    // we do not need to use CAS since no other writers can change the lock status
    uint16_t is_lock = metadata.bitmap.load();
    if (is_lock % 2 == 1) {
        metadata.bitmap.store(is_lock - 1);
    }
}

template <typename K, typename V>
int Bucket<K, V>::GetEmptyEntry() {
    int pos = 0;
    uint16_t bm = metadata.bitmap.load() >> 1;
    if (bm == 0) {
        return pos;
    }
    if (bm == 0x7fff) {
        return -1;
    }
    while (bm != 0) {
        if (bm % 2 == 0) {
            return pos;
        }
        bm = bm >> 1;
        pos++;
    }
    return pos;
}

template <typename K, typename V>
void Bucket<K, V>::OccupyEntry(int pos) {
    // thread-safe because the bucket has been locked
    uint16_t bm = metadata.bitmap.load();
    bm |= (1 << (pos + 1));
    metadata.bitmap.store(bm);
}

template <typename K, typename V>
void Bucket<K, V>::ClearEntry(int pos) {
    // clear the bitmap
    uint16_t bm = metadata.bitmap.load();
    bm &= ~(1 << (pos + 1));
    metadata.bitmap.store(bm);
    // clear the ptr that points to the KV pair in the data region (unnecessary)
    entries[pos].SetAddr(0);
}

template <typename K, typename V>
int Bucket<K, V>::EvictEntry() {
    /*
        TODO(mingzhe.du):
        implement an eviction policy
        now a random eviction policy is used
    */
    unsigned int seed = time(NULL);
    int evict_pos = rand_r(&seed) % 16;
    uint16_t bm = metadata.bitmap.load();
    bm &= ~(1 << (evict_pos + 1));
    metadata.bitmap.store(bm);
    return evict_pos;
}

template <typename K, typename V>
int Bucket<K, V>::GetIndex(const K& key, std::bitset<96>* sig_96, std::bitset<128>* sig_128) {
    /* do a quick fingerprint comparison, only compare entries whose fingerprints match */
    uint8_t fp = bcache2::HashCode1B(key);
    __m128i s = _mm_set1_epi8(fp);
    __m128i finger_prints = _mm_load_si128((__m128i const*)metadata.finger_print);
    __m128i results = _mm_cmpeq_epi8(finger_prints, s);
    uint16_t mask = static_cast<uint16_t>(_mm_movemask_epi8(results));
    mask &= static_cast<uint16_t>(metadata.bitmap.load() >> 1);

    while (mask) {
        // bitScan returns one plus the index of the least significant 1-bit of mask
        int pos = BitScan(mask) - 1;
        // check the key signatures of potential matches
        Entry<K, V> e = entries[pos];
        if (static_cast<StorageEngineType>(e.GetType()) == StorageEngineType::DRAM ||
            static_cast<StorageEngineType>(e.GetType()) == StorageEngineType::PMEM) {
            // if the data is on DRAM/PMEM, use 96-bit signature
            if (e.GetSignature96b() == *sig_96) {
                // compare the search key with the full key in the data region
                char* addr = reinterpret_cast<char*>(e.GetPtr());
                uint64_t k_len = *(reinterpret_cast<uint64_t*>(addr));
                if (IsEqual(reinterpret_cast<const char*>(&key),
                            const_cast<const char*>(addr + DATA_HEADER), k_len) == false) {
                    return -1;
                }
                return pos;
            }
        } else {
            // data is on SSD, use 128-bit signature
            if (e.GetSignature128b() == *sig_128) {
                // TODO(mingzhe.du): deference SSD address
                return pos;
            }
        }
        mask &= ~(0x1 << pos);
    }
    return -1;
}

template class Entry<int, char*>;
template class Entry<std::string, char*>;
template class Bucket<int, char*>;
template class Bucket<std::string, char*>;
template class BucketHeader<int, char*>;
template class BucketHeader<std::string, char*>;

}  // namespace bcache2
