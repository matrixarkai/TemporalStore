// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <algorithm>
#include <atomic>
#include <map>
#include <memory>
#include <ostream>
#include <queue>
#include <string>
#include <vector>

#include "bthread/bthread.h"
#include "bthread/mutex.h"
#include "butil/time.h"

#include "common/macros.h"
#include "common/status.h"
#include "metaserver_v2/flags.h"

namespace bcache2 {
namespace metaserver {

struct Task {
 public:
    explicit Task(int pri)  // lower number higher priority
        : priority_(pri),
          create_time_ms_(butil::gettimeofday_ms()),
          next_run_time_ms_(create_time_ms_) {}
    virtual ~Task() {}

    // helpers
    int GetPriority() const { return priority_; }
    int64_t GetCreateTimeMs() const { return create_time_ms_; }
    int64_t GetLastRunTimeMs() const { return last_run_time_ms_; }
    int64_t GetNextRunTimeMs() const { return next_run_time_ms_; }
    int64_t GetRetryTimes() const { return retry_times_; }

    virtual Status Process() = 0;
    virtual uint64_t GetPostponeTimeMs() {
        return std::min(GetRetryTimes() * FLAGS_metaserver_task_scheduler_base_postpone_time_ms,
                        FLAGS_metaserver_task_scheduler_max_postpone_time_ms);
    }

    virtual std::ostream& ToString(std::ostream& os) const {
        return os << "$" << priority_ << "|" << create_time_ms_ << "|" << next_run_time_ms_ << "|"
                  << last_run_time_ms_ << "|#" << retry_times_;
    }

 private:
    void Touch() { last_run_time_ms_ = butil::gettimeofday_ms(); }
    void SetNextRunMs(int64_t t) { next_run_time_ms_ = t; }
    void IncrRetryTimes() { ++retry_times_; }

 private:
    friend class TaskScheduler;

    const int priority_;
    int64_t create_time_ms_;
    int64_t next_run_time_ms_;
    int64_t last_run_time_ms_{0};
    int64_t retry_times_{0};
};

inline std::ostream& operator<<(std::ostream& os, const Task& obj) {
    obj.ToString(os);
    return os;
}

class PriorityTaskQueue {
 public:
    PriorityTaskQueue() = default;
    ~PriorityTaskQueue() = default;

    size_t Size() const { return size_; }
    bool Empty() const { return size_ == 0; }

    void Push(Task* task) {
        queue_map_[task->GetPriority()].push(task);
        ++size_;
    }

    Task* PopFirstAvailable() {
        int64_t now = butil::gettimeofday_ms();
        for (auto& iter : queue_map_) {
            auto& q = iter.second;
            if (q.empty()) {
                continue;
            }
            Task* t = q.top();
            if (t->GetNextRunTimeMs() > now) {
                continue;
            }
            --size_;
            q.pop();
            return t;
        }
        return nullptr;
    }

 private:
    struct Comparator {
        bool operator()(const Task* lhs, const Task* rhs) const {
            return lhs->GetNextRunTimeMs() > rhs->GetNextRunTimeMs();
        }
    };
    using Container = std::priority_queue<Task*, std::vector<Task*>, Comparator>;

 private:
    size_t size_{0};
    std::map<int, Container> queue_map_;
};

class TaskScheduler {
 public:
    explicit TaskScheduler(const std::string& name);
    ~TaskScheduler();

    Status Start();
    void Stop();
    Status Submit(Task* task);

 private:
    static void* RunLoop(void*);
    void Loop();

 private:
    const std::string name_;
    std::atomic<bool> running_{false};
    bthread_t thread_;

    std::atomic<size_t> inflight_task_cnt_{0};
    bthread::Mutex mu_;
    PriorityTaskQueue q_;
};

}  // namespace metaserver
}  // namespace bcache2
