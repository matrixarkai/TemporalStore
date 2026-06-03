#pragma once

#include <cstdint>

namespace mtcache {

uint64_t hash_uint64(uint64_t block_id);

uint32_t mur_mur_hash2(const void* key, int len, unsigned int seed);

inline uint32_t mur_mur_hash2(const void* key, int len) {
  return mur_mur_hash2(key, len, 97);
}

}  // namespace mtcache
