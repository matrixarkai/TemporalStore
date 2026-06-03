#pragma once

#include <noodle/base/time.h>
#include <noodle/metric/counter.h>
#include <noodle/metric/gauge.h>
#include <noodle/metric/metric_registry.h>
#include <noodle/metric/summary.h>

#include <cstdint>
#include <variant>

namespace mtcache {
#define ZONED_STORE_METRICS(M)                           \
  M(zoned_store_get_qps, AtomicCounter)                  \
  M(zoned_store_get_latency, SampleSetTimeSummary)       \
  M(zoned_store_put_qps, AtomicCounter)                  \
  M(zoned_store_put_latency, SampleSetTimeSummary)       \
  M(zoned_store_put_throughput, AtomicCounter)           \
  M(codec_serializedata_latency, SampleSetTimeSummary)   \
  M(codec_deserializedata_latency, SampleSetTimeSummary) \
  M(zone_manager_append_qps, AtomicCounter)              \
  M(zone_manager_append_latency, SampleSetTimeSummary)   \
  M(zone_manager_append_throughput, AtomicCounter)       \
  M(zone_manager_read_qps, AtomicCounter)                \
  M(zone_manager_read_latency, SampleSetTimeSummary)     \
  M(zone_manager_read_throughput, AtomicCounter)         \
  M(zone_manager_append_batch_size, SampleSetSummary)    \
  M(zoned_store_write_amplification, AtomicCounter)      \
  M(zoned_store_used, AtomicCounter)

class ZonedStoreMetrics {
 public:
  using Value = int64_t;
  using Time = noodle::Time;
  using CounterPtr = noodle::Counter*;
  using GaugePtr = noodle::AtomicGauge*;
  using TimerPtr = noodle::SampleSetTimeSummary*;
  using SummaryPtr = noodle::SampleSetSummary*;
  using MetricEntity = std::variant<CounterPtr, GaugePtr, TimerPtr, SummaryPtr>;

#define M(NAME, TYPE) NAME,
  enum Metric { ZONED_STORE_METRICS(M) END };
#undef M

  static ZonedStoreMetrics* instance() {
    static ZonedStoreMetrics instance;
    return &instance;
  }

  void Start(std::shared_ptr<noodle::MetricRegistry> zoned_store_registry_);
  void Stop();

  static void counterAdd(Metric metric, Value value = 1);
  static void counterSet(Metric metric, Value value);
  static int64_t counterGet(Metric metric);
  static void timerAdd(Metric metric, Time time);
  static void summaryAdd(Metric metric, Value value);
  template <typename T>
  static std::unique_ptr<noodle::SummarySnapshot> TEST_GetSnapShot(
      Metric metric) {
    return std::get<T>(*getMetricEntities()[metric])->GetSnapshot();
  }
  class ScopedLatency {
   public:
    explicit ScopedLatency(Metric metric)
        : what_(metric), start_time_(Time::Now()) {}

    ~ScopedLatency() {
      ZonedStoreMetrics::timerAdd(what_, Time::Now() - start_time_);
    }

   private:
    Metric what_;
    Time start_time_;
  };

 private:
  // Don't allow constructor
  ZonedStoreMetrics() = default;
  ~ZonedStoreMetrics() = default;

  static MetricEntity** getMetricEntities();

  bool init_{false};
};

}  // namespace mtcache
