#pragma once

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <numeric>
#include <string>
#include <vector>

/*
 * The histogram of each operations, note that this class is not thread-safe.
 * Simple Usage:
 * HistStats stats;
 * for (int i = 0; i < 100; ++i) {
 *   auto begin_time = Now();
 *   ...
 *   auto end_time = Now();
 *   stats.append(end_time - begin_time);
 * }
 * std::cout << stats.result_string();
 */
class HistStats {
 public:
  // Init the bucket on initialization.
  HistStats() : buckets_(bucket_size_, 0) {}

  // Append execution time of a record
  void append(size_t unit) {
    if (unit < buckets_.size()) {
      ++buckets_[unit];
    } else {
      large_nums_.emplace_back(unit);
    }
    total_count_++;
  }

  // Get latency results(with avg and max result).
  // e.g. get_result({0.5, 0.9, 0.99}) will return [p50, p90, p99, avg, max]
  template <size_t N>
  auto get_result(const double (&percentiles)[N]) -> std::array<size_t, N + 2> {
    assert(std::is_sorted(std::begin(percentiles), std::end(percentiles)));
    double reciprocal_total =
        1.0 / (std::accumulate(buckets_.cbegin(), buckets_.cend(), 0) +
               large_nums_.size());

    std::array<size_t, N + 2> result{};
    size_t idx = 0;
    size_t accum = 0;
    size_t total_tm = 0;
    size_t& avg = result[N];
    size_t& max = result[N + 1];

    for (size_t d = 0; d < buckets_.size(); ++d) {
      size_t c = buckets_[d];
      if (c) {
        accum += c;
        total_tm += d * c;
        while (idx < N && accum * reciprocal_total >= percentiles[idx]) {
          result[idx++] = d;
        }
        max = d;
      }
    }

    if (!large_nums_.empty()) {
      std::sort(large_nums_.begin(), large_nums_.end());
      for (size_t i = 0; i < large_nums_.size(); ++i) {
        ++accum;
        total_tm += large_nums_[i];
        while (idx < N && accum * reciprocal_total >= percentiles[idx]) {
          result[idx++] = large_nums_[i];
        }
      }
      max = large_nums_.back();
    }
    avg = total_tm / std::max<size_t>(1, accum);
    return result;
  }

  // Merge two Histgram.
  void merge(const HistStats& another) {
    large_nums_.insert(large_nums_.end(), another.large_nums_.begin(),
                       another.large_nums_.end());
    for (size_t d = 0; d < another.buckets_.size(); ++d) {
      size_t c = another.buckets_[d];
      buckets_[d] += c;
    }
    total_count_ += another.count();
  }

  uint64_t count() const { return total_count_; }

  void reset() {
    std::fill(buckets_.begin(), buckets_.end(), 0);
    large_nums_.resize(0);
    if (large_nums_.capacity() > (10 << 10)) {
      large_nums_.shrink_to_fit();
    }
    total_count_ = 0;
  }

  std::string result_string(const char* unit_suffix = "us") {
    auto result = get_result({0.50, 0.90, 0.95, 0.99, 0.999});
    char buf[1024];
    int len = snprintf(buf, sizeof(buf),
                       "P50:%zu%s P90:%zu%s P95:%zu%s P99:%zu%s P999:%zu%s "
                       "Avg:%zu%s Max:%zu%s",
                       result[0], unit_suffix, result[1], unit_suffix,
                       result[2], unit_suffix, result[3], unit_suffix,
                       result[4], unit_suffix, result[result.size() - 2],
                       unit_suffix, result[result.size() - 1], unit_suffix);
    return {buf, static_cast<size_t>(len)};
  }

 private:
  // Total operation count
  uint64_t total_count_ = 0;
  // Total bucket size, each bucket slot contains a
  // exact nanoseconds, so the range is [0ns, 10ms]
  uint64_t bucket_size_ = (10 << 20);

  // Space for each histgram (duration from nanoseconds to microseconds)
  // Preallocated on initialization.
  std::vector<uint64_t> buckets_ = {};

  // If the latency exceeds `buckets_.size()` we don't need to expand the bucket
  // size, here we just append all large latencies to `large_nums`.
  // e.g. sometimes we get 1 seconds max latency, we have no reason to make the
  // bucket size `1*1000*1000*1000`(unit).
  std::vector<size_t> large_nums_;
};
