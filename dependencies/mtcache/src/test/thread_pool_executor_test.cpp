#include "common/thread_pool/cpu_numa_thread_pool_executor.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <gtest/gtest.h>

namespace mtcache {

static void burnMs(uint64_t ms) {
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

TEST(ThreadPoolExecutorTest, ThreadpoolCreate) {
  ::folly::CPUThreadPoolExecutor tpe(10);
}

TEST(ThreadPoolExecutorTest, ThreadpoolCPUResize) {
  ::folly::CPUThreadPoolExecutor tpe(100);
  EXPECT_EQ(100, tpe.numThreads());
  tpe.setNumThreads(50);
  EXPECT_EQ(50, tpe.numThreads());
  tpe.setNumThreads(150);
  EXPECT_EQ(150, tpe.numThreads());
}

TEST(ThreadPoolExecutorTest, ThreadpoolCPUStop) {
  ::folly::CPUThreadPoolExecutor tpe(1);
  std::atomic<int> completed(0);
  auto f = [&]() {
    burnMs(10);
    completed++;
  };
  for (int i = 0; i < 1000; i++) {
    tpe.add(f);
  }
  tpe.stop();
  EXPECT_GT(1000, completed);
}

TEST(ThreadPoolExecutorTest, ThreadpoolCPUJoin) {
  ::folly::CPUThreadPoolExecutor tpe(10);
  std::atomic<int> completed(0);
  auto f = [&]() {
    burnMs(1);
    completed++;
  };
  for (int i = 0; i < 1000; i++) {
    tpe.add(f);
  }
  tpe.join();
  EXPECT_EQ(1000, completed);
}

TEST(ThreadPoolExecutorTest, ThreadpoolCPUDestroy) {
  ::folly::CPUThreadPoolExecutor tpe(1);
  std::atomic<int> completed(0);
  auto f = [&]() {
    burnMs(10);
    completed++;
  };
  for (int i = 0; i < 1000; i++) {
    tpe.add(f);
  }
  tpe.stop();
  EXPECT_GT(1000, completed);
}

TEST(ThreadPoolExecutorTest, ThreadpoolCPUResizeUnderLoad) {
  ::folly::CPUThreadPoolExecutor tpe(10);
  std::atomic<int> completed(0);
  auto f = [&]() {
    burnMs(1);
    completed++;
  };
  for (int i = 0; i < 1000; i++) {
    tpe.add(f);
  }
  tpe.setNumThreads(5);
  tpe.setNumThreads(15);
  tpe.join();
  EXPECT_EQ(1000, completed);
}

TEST(ThreadPoolExecutorTest, ThreadpoolCPUPoolStats) {
  folly::Baton<> startBaton, endBaton;
  ::folly::CPUThreadPoolExecutor tpe(1);
  auto stats = tpe.getPoolStats();
  EXPECT_GE(1, stats.threadCount);
  EXPECT_GE(1, stats.idleThreadCount);
  EXPECT_EQ(0, stats.activeThreadCount);
  EXPECT_EQ(0, stats.pendingTaskCount);
  EXPECT_EQ(0, tpe.getPendingTaskCount());
  EXPECT_EQ(0, stats.totalTaskCount);
  tpe.add([&]() {
    startBaton.post();
    endBaton.wait();
  });
  tpe.add([&]() {});
  startBaton.wait();
  stats = tpe.getPoolStats();
  EXPECT_EQ(1, stats.threadCount);
  EXPECT_EQ(0, stats.idleThreadCount);
  EXPECT_EQ(1, stats.activeThreadCount);
  EXPECT_EQ(1, stats.pendingTaskCount);
  EXPECT_EQ(1, tpe.getPendingTaskCount());
  EXPECT_EQ(2, stats.totalTaskCount);
  endBaton.post();
}

// Because folly has tested all the other functions of the thread pool in its
// UT, we only need to test the cpuAffinity-related functions here.
template <class TPE>
static void cpuAffinityTest() {
  TPE tpe(10, false);
  EXPECT_EQ(10, tpe.numThreads());

  int current_cpu = sched_getcpu();
  ASSERT_GE(current_cpu, 0);
  std::vector<int32_t> cpu_mask;
  cpu_mask.push_back(current_cpu);
  auto f = [current_cpu]() {
    burnMs(10);
    int cpu_id = sched_getcpu();
    EXPECT_EQ(cpu_id, current_cpu);
  };

  int32_t res1 = tpe.setCpuAffinity(cpu_mask);
  EXPECT_EQ(res1, 0);

  for (int i = 0; i < 100; i++) {
    tpe.add(f);
  }

  tpe.join();
}

TEST(ThreadPoolExecutorTest, cpuAffinityTestCPU) {
  cpuAffinityTest<CPUNumaThreadPoolExecutor>();
}

}  // namespace mtcache
