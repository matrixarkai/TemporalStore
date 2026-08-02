// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/event_harbor.h"

#include <algorithm>
#include <deque>
#include <limits>
#include <mutex>
#include <set>
#include <utility>

#include "butil/time.h"

#include "common/logging.h"
#include "metaserver_v2/metrics.h"

namespace bcache2 {
namespace metaserver {

Status EventHarbor::Start() { return Status::OK(); }

void EventHarbor::Stop() {
    {
        std::unique_lock<bthread::Mutex> _(mu_);
        for (auto iter : subers_) {
            iter.second->running = false;
        }
        cv_.notify_all();
    }

    for (auto iter : subers_) {
        auto& suber = iter.second;
        LOG_INFO("stoping routine").put("id", suber->id);
        bthread_stop(suber->routine);
        bthread_join(suber->routine, nullptr);
    }
}

void EventHarbor::Publish(Event* event) {
    event->publish_timepoint_us_ = butil::cpuwide_time_us();
    std::unique_lock<bthread::Mutex> _(mu_);
    if (subers_.empty()) {
        delete event;  // TODO(wuzhenyu) think again
        return;
    }
    CHECK_LT(issue_cursor_, std::numeric_limits<uint64_t>::max() - 1);
    event->cursor_ = issue_cursor_++;
    events_.emplace(std::make_pair(event->cursor_, event));
    cv_.notify_all();
}

struct ListenerRoutineArgs {
    EventHarbor* harbor{nullptr};
    uint64_t id{0};
};

void* EventHarbor::RunSuberRoutine(void* arg) {
    ListenerRoutineArgs* pack = static_cast<ListenerRoutineArgs*>(arg);
    pack->harbor->SuberRoutine(pack->id);
    delete pack;
    return nullptr;
}

void EventHarbor::SuberRoutine(uint64_t id) {
    static constexpr size_t kBatchSize = 64;
    std::shared_ptr<Subscriber> self;
    {
        std::unique_lock<bthread::Mutex> _(mu_);
        auto iter = subers_.find(id);
        CHECK(iter != subers_.end()) << this;
        self = iter->second;
    }
    CHECK(self) << this;
    CHECK(bthread_equal(self->routine, bthread_self())) << this;
    std::set<topic_t> subscribed = self->listener->Subscribed();

    std::deque<butil::intrusive_ptr<Event>> batch;
    while (self->running) {
        std::unique_lock<bthread::Mutex> lock(mu_);
        while (events_.empty() && self->running) {
            cv_.wait_for(lock, 100'000);
        }

        auto iter = events_.upper_bound(self->ack_cursor);
        if (iter == events_.end()) {
            continue;
        }
        while (iter != events_.end() && batch.size() < kBatchSize) {
            batch.push_back(iter->second);
            iter++;
        }
        const uint64_t gap = issue_cursor_ - batch.back()->cursor_;
        lock.unlock();

        for (auto& e : batch) {
            if (subscribed.count(e->Topic()) > 0) {
                self->listener->Consume(e.get());
            }
            // self->ack_cursor = std::max(e->cursor_, self->ack_cursor.load());
        }
        self->ack_cursor = batch.back()->cursor_;
        batch.clear();

        MaybeSweepLegacyEvents(id, gap);
    }  // main loop
    LOG_INFO("exit consume routine").put("id", id);
}

void EventHarbor::MaybeSweepLegacyEvents(uint64_t suber_id, size_t gap) {
    static constexpr size_t kSweepThreshold = 200;
    std::unique_lock<bthread::Mutex> lock(mu_, std::defer_lock_t{});
    if (gap > kSweepThreshold) {
        LOG_INFO("gap too large").put("id", suber_id).put("gap", gap);
        lock.lock();
    } else {
        lock.try_lock();
    }

    if (!lock.owns_lock()) {
        return;
    }
    uint64_t min_ack = issue_cursor_ + 1;
    for (auto iter : subers_) {
        min_ack = std::min(min_ack, iter.second->ack_cursor.load());
    }
    size_t cnt = 0;
    auto iter = events_.begin();
    while (iter != events_.end() && min_ack >= iter->first) {
        iter = events_.erase(iter);
        cnt++;
    }
    MS_METRIC(event_harbor_queue_length).get()->Set(events_.size());
    LOG_DEBUG("sweep legacy events").put("id", suber_id).put("count", cnt).put("got_min", min_ack);
}

Status EventHarbor::RegisterListener(EventHarbor::Listener* listener) {
    CHECK(listener != nullptr);
    bthread_t thd;
    std::unique_lock<bthread::Mutex> _(mu_);
    const uint64_t id = suber_id_++;
    ListenerRoutineArgs* args = new ListenerRoutineArgs({this, id});
    int rc = bthread_start_background(&thd, nullptr, RunSuberRoutine, static_cast<void*>(args));
    if (rc != 0) {
        LOG_ERROR("failed to start background bthread");
        return Status::Internal("bthread start failed");
    }
    auto suber = std::make_shared<Subscriber>(id, thd);
    suber->listener = listener;
    suber->ack_cursor = issue_cursor_;
    subers_.emplace(std::make_pair(id, suber));
    return Status::OK();
}

void EventHarbor::UnregisterListener(Listener* listener) {
    std::shared_ptr<Subscriber> suber;
    {
        std::unique_lock<bthread::Mutex> _(mu_);
        for (auto iter : subers_) {
            auto& c = iter.second;
            if (c->listener == listener) {
                suber = c;
                subers_.erase(c->id);
                break;
            }
        }
    }
    if (suber) {
        LOG_INFO("stoping routine").put("id", suber->id);
        suber->running = false;
        bthread_stop(suber->routine);
        bthread_join(suber->routine, nullptr);
    }
}

}  // namespace metaserver
}  // namespace bcache2

