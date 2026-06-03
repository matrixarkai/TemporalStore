#include "util/glog_advanced.h"

#include <gtest/gtest.h>

namespace mtcache {

struct LogTimeRecorder {
  static const size_t kMaxCalls = 10;
  size_t record_idx = 0;
  size_t fn_ncalls = 0;
  std::chrono::steady_clock::time_point calltimes[kMaxCalls];
  size_t ncalls[kMaxCalls];
  size_t expected_ncalls[kMaxCalls];
};

// The stream operator is called by LOG_EVERY_T every time a logging event
// occurs. Make sure to save the times for each call as they will be used later
// to verify the time delta between each call.
std::ostream& operator<<(std::ostream& stream, LogTimeRecorder& t) {
  auto log = static_cast<google::LogMessage::LogStream*>(&stream);
  t.ncalls[t.record_idx] = log->ctr();
  t.expected_ncalls[t.record_idx] = t.fn_ncalls;
  t.calltimes[t.record_idx++] = std::chrono::steady_clock::now();
  return stream;
}

// get elapsed time in nanoseconds
int64_t elapsed_time_ns(const std::chrono::steady_clock::time_point& begin,
                        const std::chrono::steady_clock::time_point& end) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>((end - begin))
      .count();
}

#define TEST_TIMED_LOGGING(log)                                              \
  do {                                                                       \
    LogTimeRecorder recorder;                                                \
                                                                             \
    constexpr auto expected_sec = 0.3; /* 300ms */                           \
                                                                             \
    while (recorder.record_idx < LogTimeRecorder::kMaxCalls) {               \
      recorder.fn_ncalls++;                                                  \
      log(INFO, expected_sec)                                                \
          << recorder << "Timed Log #" << recorder.record_idx << "/"         \
          << google::COUNTER << " calls";                                    \
    }                                                                        \
                                                                             \
    int64_t durations[LogTimeRecorder::kMaxCalls - 1];                       \
    for (int i = 1; i < LogTimeRecorder::kMaxCalls; i++) {                   \
      durations[i - 1] =                                                     \
          elapsed_time_ns(recorder.calltimes[i - 1], recorder.calltimes[i]); \
    }                                                                        \
    /* check counter */                                                      \
    for (int i = 0; i < LogTimeRecorder::kMaxCalls; i++) {                   \
      EXPECT_EQ(recorder.expected_ncalls[i], recorder.ncalls[i]);            \
    }                                                                        \
    /* check time, err = 10ms */                                             \
    for (int i = 0; i < LogTimeRecorder::kMaxCalls - 1; i++) {               \
      EXPECT_NEAR(durations[i], 1e9 * expected_sec, 1e7);                    \
    }                                                                        \
  } while (0)

TEST(GlogAdvancedTest, TimedLogging) {
    TEST_TIMED_LOGGING(LOG_EVERY_T);
    TEST_TIMED_LOGGING(PLOG_EVERY_T);
    TEST_TIMED_LOGGING(SYSLOG_EVERY_T);
}

}  // namespace mtcache

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
