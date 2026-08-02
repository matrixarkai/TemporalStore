// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "butil/fast_rand.h"
#include "butil/logging.h"
#include "gtest/gtest.h"

#include "common/logging.h"
#include "metaserver_v2/metrics.h"
#include "metaserver_v2/scheduler/task_scheduler.h"

namespace bcache2::metaserver::test {

const int pri_factor = 100'000;
std::vector<int> empty_task_order;
struct EmptyTask : public Task {
    explicit EmptyTask(int pri, int n) : Task(pri), num(pri * pri_factor + n) {}
    Status Process() override {
        empty_task_order.push_back(num);
        return Status::OK();
    }

    const int num;
};

bool endless = true;
std::vector<int> endless_postponed_task_order;
struct EndlessPostponedTask : public Task {
    explicit EndlessPostponedTask(int pri, int n) : Task(pri), num(pri * pri_factor + n) {}
    Status Process() override {
        if (endless) {
            return Status::RetryLater("");
        }
        endless_postponed_task_order.push_back(num);
        return Status::OK();
    }

    const int num;
};

TEST(TaskSchedulerTest, MultiPriorityTaskTest) {
    InitMetrics("dev.bcache2.ut", {});
    BYTE_DEFER({ QuitMetrics(); });

    auto schd = std::make_unique<TaskScheduler>("x");
    ASSERT_TRUE(schd->Start().ok());
    std::vector<Task*> tasks;
    FLAGS_metaserver_task_scheduler_max_postpone_time_ms = 1'000;
    size_t postponed_count = 0;
    const int pcount = 10;
    const int count = 100;
    for (int pri = pcount; pri > 0; pri--) {
        for (int i = 0; i < count; i++) {
            tasks.push_back(new EmptyTask(pri, i));
            tasks.push_back(new EndlessPostponedTask(pri, i));
            ++postponed_count;
        }
    }
    for (auto task : tasks) {
        Status status = schd->Submit(task);
        ASSERT_TRUE(status.ok());
    }
    for (int i = 0; i < 15; i++) {
        bthread_usleep(100 * 1000);
        if (schd->q_.Size() <= postponed_count) {
            break;
        }
    }
    ASSERT_EQ(schd->q_.Size(), postponed_count);
    endless = false;
    for (int i = 0; i < 15; i++) {
        bthread_usleep(100 * 1000);
        if (schd->q_.Empty()) {
            break;
        }
    }
    ASSERT_TRUE(schd->q_.Empty());
    ASSERT_EQ(empty_task_order.size(), pcount * count);
    for (int i = 1; i < static_cast<int>(empty_task_order.size()); i++) {
        ASSERT_GE(empty_task_order[i] / pri_factor, empty_task_order[i - 1] / pri_factor);
    }
    ASSERT_EQ(endless_postponed_task_order.size(), pcount * count);
    for (int i = 1; i < static_cast<int>(endless_postponed_task_order.size()); i++) {
        ASSERT_GE(endless_postponed_task_order[i] / pri_factor,
                  endless_postponed_task_order[i - 1] / pri_factor);
    }
}

}  // namespace bcache2::metaserver::test
