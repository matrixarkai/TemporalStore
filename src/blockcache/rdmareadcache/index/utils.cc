// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "blockcache/rdmareadcache/index/utils.h"

#include <string>
namespace bcache2 {

unsigned char HashCode1B(const int& x) {
    int y = x;
    y ^= y >> 16;
    y ^= y >> 8;
    return (unsigned char)(y & 0x0ffULL);
}

unsigned char HashCode1B(const std::string& x) {
    size_t fp = std::hash<std::string>{}((std::string)x);
    return (unsigned char)(fp & 0x0ffULL);
}

void Signature_96(const int& k, uint64_t bucket_pos, std::bitset<96>* sig) {
    size_t hash_1 = std::hash<int>{}(k);
    size_t hash_2 = std::hash<uint64_t>{}(bucket_pos);
    memcpy(sig, &hash_1, 8);
    memcpy(reinterpret_cast<uint64_t*>(sig) + 1, &hash_2, 4);
}

void Signature_96(const std::string& k, uint64_t bucket_pos, std::bitset<96>* sig) {
    size_t hash_1 = std::hash<std::string>{}(k);
    size_t hash_2 = std::hash<uint64_t>{}(bucket_pos);
    memcpy(sig, &hash_1, 8);
    memcpy(reinterpret_cast<uint64_t*>(sig) + 1, &hash_2, 4);
}

void Signature_128(const int& k, uint64_t bucket_pos, std::bitset<128>* sig) {
    size_t hash_1 = std::hash<int>{}(k);
    size_t hash_2 = std::hash<uint64_t>{}(bucket_pos);
    memcpy(sig, &hash_1, 8);
    memcpy(reinterpret_cast<uint64_t*>(sig) + 1, &hash_2, 8);
}

void Signature_128(const std::string& k, uint64_t bucket_pos, std::bitset<128>* sig) {
    size_t hash_1 = std::hash<std::string>{}(k);
    size_t hash_2 = std::hash<uint64_t>{}(bucket_pos);
    memcpy(sig, &hash_1, 8);
    memcpy(reinterpret_cast<uint64_t*>(sig) + 1, &hash_2, 8);
}

unsigned char EntryCRC(Entry<int, char*>* e) {
    uint64_t x = std::hash<Entry<int, char*>*>{}(e);
    return (unsigned char)(x & 0x0ffULL);
}

unsigned char EntryCRC(Entry<std::string, char*>* e) {
    uint64_t x = std::hash<Entry<std::string, char*>*>{}(e);
    return (unsigned char)(x & 0x0ffULL);
}

uint64_t DataCRC(char* data, uint64_t len) {
    std::string val(data, len);
    uint64_t crc = std::hash<std::string>{}(val);
    return crc;
}

bool VerifyCRC(char* addr, uint64_t sz, uint64_t crc) {
    if (*reinterpret_cast<uint64_t*>(addr + sz - CRC_LEN) == crc) {
        return true;
    }
    return false;
}

bool VerifyKey(const char* str1, const char* str2, uint64_t len) {
    return memcmp(str1, str2, len) == 0 ? true : false;
}

bool IsEqual(const char* str1, const char* str2, uint64_t len) {
    return memcmp(str1, str2, len) == 0 ? true : false;
}

}  // namespace bcache2
