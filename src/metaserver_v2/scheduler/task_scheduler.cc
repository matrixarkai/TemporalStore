// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/scheduler/task_scheduler.h"

#include <chrono>
#include <thread>

#include "butil/logging.h"
#include "common/logging.h"
#include "common/macros.h"
#include "metaserver_v2/flags.h"
#include "metaserver_v2/metrics.h"

namespace bcache2 {
namespace metaserver {

TaskScheduler::TaskScheduler(const std::string& name) : name_(name) {}

TaskScheduler::~TaskScheduler() { Stop(); }

Status TaskScheduler::Start() {
    if (running_) {
        return Status::Internal("already running");
    }
    running_ = true;
    int rc = bthread_start_background(&thread_, nullptr, RunLoop, this);
    if (rc != 0) {
        running_ = false;
        LOG_ERROR("failed to start background bthread").put("schd", name_);
        return Status::Internal("bthread start failed");
    }
    return Status::OK();
}

void TaskScheduler::Stop() {
    if (!running_) {
        return;
    }
    LOG_INFO("stopping").put("schd", name_);
    running_ = false;
    bthread_stop(thread_);

    LOG_INFO("joining background bthread").put("schd", name_);
    bthread_join(thread_, nullptr);
}

Status TaskScheduler::Submit(Task* task) {
    if (!running_) {
        return Status::Internal("TaskScheduler is not running");
    }
    std::lock_guard<bthread::Mutex> lock(mu_);
    q_.Push(task);
    return Status::OK();
}

void* TaskScheduler::RunLoop(void* arg) {
    TaskScheduler* schd = static_cast<TaskScheduler*>(arg);
    schd->Loop();
    return nullptr;
}

void TaskScheduler::Loop() {
    LOG_INFO("enter routine loop").put("schd", name_);
    bool delay_flag = false;
    while (running_) {
        if (delay_flag) {
            bthread_usleep(FLAGS_metaserver_task_scheduler_interval_ms * 1'000);
            delay_flag = false;
        }
        if (inflight_task_cnt_ >= FLAGS_metaserver_task_scheduler_max_inflight) {
            delay_flag = true;
            continue;
        }
        Task* task = nullptr;
        size_t qsize = 0;
        {
            std::lock_guard<bthread::Mutex> lock(mu_);
            if (q_.Empty() || (task = q_.PopFirstAvailable()) == nullptr) {
                delay_flag = true;
                continue;
            }
            qsize = q_.Size();
        }

        g_metrics->EmitStore("schd_task_queue_length", qsize, {{"schd", name_}});
        std::unique_ptr<Task> guard(task);
        // in-flight here make no means currently
        inflight_task_cnt_++;
        BYTE_DEFER({ inflight_task_cnt_--; });

        task->Touch();

        // TODO(wuzhenyu) refactor to async task to avoid head blocking
        // due to high cost RPC call
        int64_t start_time = butil::cpuwide_time_us();
        Status result = task->Process();
        int64_t elapse = butil::cpuwide_time_us() - start_time;

        g_metrics->EmitTimer("schd_task_latency_us", elapse, {{"schd", name_}});
        LOG_INFO("process task")
            .put("task", *task)
            .put("elapse_us", elapse)
            .put("result", result)
            .put("remain", qsize)
            .put("schd", name_);
        switch (result.errorcode()) {
        case kOK:
            break;

        case kRetryLater: {
            LOG_WARNING("task will retry later")
                .put("detail", result)
                .put("task", *task)
                .put("schd", name_);
            task->IncrRetryTimes();
            task->SetNextRunMs(butil::gettimeofday_ms() + task->GetPostponeTimeMs());
            guard.release();
            std::lock_guard<bthread::Mutex> lock(mu_);
            q_.Push(task);
            break;
        }

        case kAborted:
            LOG_WARNING("failed to process task and now we abort it")
                .put("detail", result)
                .put("schd", name_);
            break;

        default:
            LOG_ERROR("unkonwn result").put("result", result).put("schd", name_);
            CHECK(false);
            break;
        }  // switch
    }
    LOG_INFO("exiting routine loop...").put("schd", name_);
}

}  // namespace metaserver
}  // namespace bcache2
