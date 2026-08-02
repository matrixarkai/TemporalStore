#include "common/logging.h"
#include "storage/zoned_store/metrics.h"

#include <gtest/gtest.h>
#include <noodle/metric/bytedance_metric_report_buidler.h>

#include <chrono>
#include <cstdint>
#include <thread>

namespace mtcache {

class MetricTest : public testing::Test {
 protected:
  void SetUp() override {
    auto zoned_store_registry =
        noodle::GetMetricRegistry("ti.mtcache.zoned_store_test");
    ZonedStoreMetrics::instance()->Start(zoned_store_registry);
  }

  void TearDown() override {
    noodle::GetGlobalMetricRegistry()->Deregister(
        "ti.mtcache.zoned_store_test");
    ZonedStoreMetrics::instance()->Stop();
  }
};

TEST_F(MetricTest, QPSTest) {
  for (int i = 0; i < 120; i++) {
    ZonedStoreMetrics::counterAdd(ZonedStoreMetrics::zoned_store_get_qps);
  }
  // std::this_thread::sleep_for(std::chrono::seconds(30));
  ASSERT_EQ(
      ZonedStoreMetrics::counterGet(ZonedStoreMetrics::zoned_store_get_qps),
      120);
}

TEST_F(MetricTest, LatencyTest) {
  for (int i = 0; i < 10; i++) {
    ZonedStoreMetrics::ScopedLatency latency(
        ZonedStoreMetrics::zoned_store_get_latency);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  auto snapshot =
      ZonedStoreMetrics::TEST_GetSnapShot<ZonedStoreMetrics::TimerPtr>(
          ZonedStoreMetrics::zoned_store_get_latency);
  ASSERT_EQ(snapshot->GetCount(), 10);
  VLOG(1) << snapshot->GetMax();
}

TEST_F(MetricTest, AvgTest) {
  for (int i = 0; i < 10; i++) {
    ZonedStoreMetrics::summaryAdd(
        ZonedStoreMetrics::zone_manager_append_batch_size, i);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  auto snapshot =
      ZonedStoreMetrics::TEST_GetSnapShot<ZonedStoreMetrics::SummaryPtr>(
          ZonedStoreMetrics::zone_manager_append_batch_size);
  ASSERT_EQ(10, snapshot->GetCount());
  ASSERT_EQ(9, snapshot->GetMax());
}

}  // namespace mtcache

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
