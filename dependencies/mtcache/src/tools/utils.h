#pragma once
#include <atomic>
#include <chrono>
#include <map>
#include <random>
#include <vector>
#include <xxhash.h>

namespace mtcache {

// Get random uint16 integer
inline int fast_rand16() {
  // Compute a pseudorandom integer, Output value in range [0, 32767]
  static unsigned int g_seed = 1988;
  g_seed = (214013 * g_seed + 2531011);
  return (g_seed >> 16) & 0x7FFF;
}

// Get random uint64 integer
uint64_t fast_rand64() {
  static thread_local std::random_device rd;
  static thread_local std::mt19937_64 gen(rd());
  static thread_local std::uniform_int_distribution<uint64_t> dist;
  return dist(gen);
}

inline std::string get_hashed_key(uint64_t id, int len = 4) {
  int xxh32 = XXH32(reinterpret_cast<char*>(&id), 8, 0);
  return std::string(reinterpret_cast<char*>(&xxh32), len);
}

inline std::string get_rand_str(int len) {
  static thread_local std::random_device rd;
  static thread_local std::mt19937_64 gen(rd());
  static thread_local std::uniform_int_distribution<uint32_t> dist;

  char buf[len];
  for (auto i = 0; i < len; ++i) {
    buf[i] = 'a' + dist(gen) % 26;
  }
  return std::string(buf, len);
}

// Generate random
class RandomStringGenerator {
 public:
  // Generate a random string on init, then select substring randomly.
  RandomStringGenerator() {
    posix_memalign((void**)&buf_, 512, size_);
    std::default_random_engine rng_;
    std::uniform_int_distribution<> dist_(0, 25);

    for (auto i = 0; i < size_; ++i) {
      buf_[i] = 'a' + dist_(rng_);
    }
  }

  ~RandomStringGenerator() { free(buf_); }

  // Generate a random string, if (size + 2<<16) is larger than size_, we use
  // (size_ - 2<<16) as the new size.
  int rand_value(char** value, int size, bool copy = false) {
    size = (size_ >= size + (2 << 16)) ? size : size_ - (2 << 16);
    int offset = fast_rand16();
    if (copy) {
      memcpy(*value, buf_ + offset, size);
    } else {
      *value = buf_ + offset;
    }
    return size;
  }

 private:
  int size_ = 10 << 20;
  char* buf_;
};

static RandomStringGenerator rand_generator;

// Generate random string, maximum size is 10MB
// @see RandomStringGenerator
inline void rand_string(char** out, int size, bool copy = false) {
  rand_generator.rand_value(out, size, copy);
}

}  // namespace mtcache
