#include "storage/zoned_store/metrics.h"

#include "metrics.h"

namespace mtcache {

namespace zoned_store_metric_internal {

#define M(NAME, TYPE) auto METRIC_##NAME = std::make_shared<noodle::TYPE>();
ZONED_STORE_METRICS(M)
#undef M

#define M(NAME, TYPE) \
  ZonedStoreMetrics::MetricEntity METRIC_ENTITY_##NAME{METRIC_##NAME.get()};
ZONED_STORE_METRICS(M)
#undef M

/// Metric identifier -> metric entity.
#define M(NAME, TYPE) &METRIC_ENTITY_##NAME,
ZonedStoreMetrics::MetricEntity* metric_entities[] = {ZONED_STORE_METRICS(M)};
#undef M

}  // namespace zoned_store_metric_internal

void ZonedStoreMetrics::Start(
    std::shared_ptr<noodle::MetricRegistry> zoned_store_registry_) {
  if (init_) return;
#define M(NAME, TYPE)                                \
  zoned_store_registry_->MustRegister<noodle::TYPE>( \
      noodle::MetricId(#NAME), zoned_store_metric_internal::METRIC_##NAME);
  ZONED_STORE_METRICS(M)
#undef M
  init_ = true;
}

void ZonedStoreMetrics::Stop() { init_ = false; }

ZonedStoreMetrics::MetricEntity** ZonedStoreMetrics::getMetricEntities() {
  return zoned_store_metric_internal::metric_entities;
}

void ZonedStoreMetrics::counterAdd(Metric metric, Value value) {
  std::get<CounterPtr>(*getMetricEntities()[metric])->Increase(value);
}

void ZonedStoreMetrics::counterSet(Metric metric, Value value) {
  std::get<CounterPtr>(*getMetricEntities()[metric])->SetValue(value);
}

int64_t ZonedStoreMetrics::counterGet(Metric metric) {
  return std::get<CounterPtr>(*getMetricEntities()[metric])->GetValue();
}

void ZonedStoreMetrics::timerAdd(Metric metric, Time time) {
  std::get<TimerPtr>(*getMetricEntities()[metric])
      ->Add(time, noodle::Time::Now());
}

void ZonedStoreMetrics::summaryAdd(Metric metric, Value value) {
  std::get<SummaryPtr>(*getMetricEntities()[metric])
      ->Add(value, noodle::Time::Now());
}

}  // namespace mtcache
