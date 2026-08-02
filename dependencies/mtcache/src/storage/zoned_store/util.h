#pragma once

#include "storage/zoned_store/zoned_store.h"

#include <xxhash.h>
namespace mtcache {
// Put n bytes num in buf.
// Return buf + #num.
char* PutFixedUint8(char* buf, uint8_t num);

char* PutFixedUint32(char* buf, uint32_t num);

char* PutFixedUint64(char* buf, uint64_t num);

char* PutFixedHash64(char* buf, XXH64_hash_t* num);

char* PutFixedHash128(char* buf, void* num);

// Retrive number stored in buf.
// Return buf + #num.
const char* GetFixedHash128(const char* buf, void* hash_val);

const char* GetFixedHash64(const char* buf, XXH64_hash_t* num);

const char* GetFixedUint64(const char* buf, uint64_t* num);

const char* GetFixedUint32(const char* buf, uint32_t* num);

const char* GetFixedUint8(const char* buf, uint8_t* num);

// (size+ @return value) % padding = 0.
// 4KB -> 4KB; 5KB -> 8KB(if padding = 4kb).
int AlignedTo(uint32_t size, int align_size);

// Copy @len bytes from src to dst,
// and @return value % padding = 0.
// @return dst + len
char* CopyBytesTo(char* dst, const char* src, int len);

// Copy @len bytes from src to dst,
// @return src + len
const char* CopyBytesFrom(const char* src, std::string* dst, int len);
const char* CopyBytesFrom(const char* src, char* dst, int len);

// - Memory: <0,48bits memory address>
// - SSD: <size, 43bits lba>
std::pair<uint32_t, uint64_t> DecodeColoredPtr(
    Index::SSDColoredPtr colored_ptr);

// ColoredPtr's form on SSD is `Index::SSDColoredPtr`(uint64_t).
// See `Index.h` for bit pattern meaning.
// MaskXXX(old,field) will get the old pointer's certain field to be equal to
// the new value.
uint64_t MaskColoredPtrMemoryAddress(Index::SSDColoredPtr old_colored_ptr,
                                     uint64_t address);
uint64_t MaskColoredPtrLBA(Index::SSDColoredPtr old_colored_ptr, uint64_t lba);
uint64_t MaskColoredPtrSize(Index::SSDColoredPtr old_colored_ptr,
                            uint32_t size);
uint64_t MaskColoredPtrRecordState(Index::SSDColoredPtr old_colored_ptr,
                                   Index::RecordStateType st);
};  // namespace mtcache
