// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once
#include <string>

#include "blockcache/rdmareadcache/index/entry.h"

namespace bcache2 {

template <typename K, typename V>
class Bucket;
template <typename K, typename V>
class HashTable;
template <typename K, typename V>
class Entry;

// Returns one plus the index of the least significant 1-bit of x, if x is zero, returns zero.
#define BitScan(x) __builtin_ffs(x)
// Returns the number of 1-bits in x.
#define CountBit(x) __builtin_popcount(x)

typedef __uint128_t uint128_t;

/**
 * @brief compute a 96-bit signature of a given key and its bucket position
 * @param k is the target key
 * @param sig is the pointer that points to the signature
 */
void Signature_96(const int& k, uint64_t bucket_pos, std::bitset<96>* sig);
void Signature_96(const std::string& k, uint64_t bucket_pos, std::bitset<96>* sig);

/**
 * @brief compute a 128-bit signature of a given key and its bucket position
 * @param k is the target key
 * @param sig is the pointer that points to the signature
 */
void Signature_128(const int& k, uint64_t bucket_pos, std::bitset<128>* sig);
void Signature_128(const std::string& k, uint64_t bucket_pos, std::bitset<128>* sig);

/**
 * @brief compute a 8-bit fingerprint of a given key
 * @param x is the given key
 * @return the 8-bit signature of a given key
 */
unsigned char HashCode1B(const int& x);
unsigned char HashCode1B(const std::string& x);

/**
 * @brief compute the 8-bit checksum of an index entry
 * @param e is the pointer that points to the target index
 * @return the checksum of an index e
 */
unsigned char EntryCRC(Entry<int, char*>* e);
unsigned char EntryCRC(Entry<std::string, char*>* e);

/**
 * @brief compute the 64-bit checksum of an data block
 * @param data is the pointer that points to the target data block
 * @return the checksum of the data block
 */
uint64_t DataCRC(char* data, uint64_t len);

/**
 * @brief verify the CRC correctness of a data block
 *
 * @param addr if the address of the data block
 * @param sz is the data block size
 * @param crc is the expected crc which will be compared with the data block crc
 * @return true
 * @return false
 */
bool VerifyCRC(char* addr, uint64_t sz, uint64_t crc);

/**
 * @brief check if the target key matches with the full key in the data block
 *
 * @param str1 is the address of the target key
 * @param str2 is the address of the full key
 * @param len is the length of the key
 * @return true
 * @return false
 */
bool VerifyKey(const char* str1, const char* str2, uint64_t len);

/**
 * @brief compare if the first len bytes of str1 and str2 are equal
 *
 * @param str1 is the pointer that points to string 1
 * @param str2 is the pointer that points to string 2
 * @param len is the length of str1 and str2 to be compared
 * @return true if the len-byte of str1 and str2 are the same
 * @return false if not equal
 */
bool IsEqual(const char* str1, const char* str2, uint64_t len);

}  // namespace bcache2
