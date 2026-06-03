#include "hash.h"

namespace mtcache {

static const uint64_t kHashSeed = 0x20170730L;

uint64_t hash_uint64(uint64_t block_id) {
  const uint64_t m = 0xc6a4a7935bd1e995ULL;
  const int r = 47;
  uint64_t k = block_id;
  k *= m;
  k ^= k >> r;
  k *= m;
  uint64_t h = kHashSeed ^ (sizeof(uint64_t) * m);
  h ^= k;
  h *= m;
  h ^= h >> r;
  h *= m;
  h ^= h >> r;
  return h;
}

unsigned int mur_mur_hash2(const void* key, int len, unsigned int seed) {
  // 'm' and 'r' are mixing constants generated offline.
  // They're not really 'magic', they just happen to work well.

  const unsigned int m = 0x5bd1e995;
  const int r = 24;

  // Initialize the hash to a 'random' value

  unsigned int h = seed ^ len;

  // Mix 4 bytes at a time into the hash

  const unsigned char* data = (const unsigned char*)key;

  while (len >= 4) {
    unsigned int k = *(unsigned int*)data;

    k *= m;
    k ^= k >> r;
    k *= m;

    h *= m;
    h ^= k;

    data += 4;
    len -= 4;
  }

  // Handle the last few bytes of the input array

  const int8_t* idata = (const int8_t*)data;
  switch (len) {
    case 3:
      h ^= idata[2] << 16;
    case 2:
      h ^= idata[1] << 8;
    case 1:
      h ^= idata[0];
      h *= m;
  };

  // Do a few final mixes of the hash to ensure the last few
  // bytes are well-incorporated.

  h ^= h >> 13;
  h *= m;
  h ^= h >> 15;

  return h;
}

}  // namespace mtcache
