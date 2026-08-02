// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <atomic>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <unordered_map>

#include "brpc/shared_object.h"
#include "bthread/bthread.h"
#include "bthread/condition_variable.h"
#include "bthread/mutex.h"

#include "common/status.h"

namespace bcache2 {
namespace metaserver {

class EventHarbor {
 public:
    using topic_t = int;

    class Event : public brpc::SharedObject {
     public:
        virtual ~Event() {}

        virtual topic_t Topic() const = 0;

        int64_t GetPublishTimepointUs() const { return publish_timepoint_us_; }

     private:
        friend EventHarbor;
        int64_t publish_timepoint_us_{0};
        uint64_t cursor_{0};
    };

    class Listener {
     public:
        virtual ~Listener() {}

        virtual std::set<topic_t> Subscribed() = 0;
        virtual void Consume(const Event* event) = 0;
    };

 public:
    EventHarbor() = default;
    ~EventHarbor() { Stop(); }

    Status Start();
    void Stop();

    // Note:
    //  1. event lifecycle would be delivered to harbor, do not use after Publish()
    //  2. event would be discarded if no listener registered
    void Publish(Event* event);

    // Note: suber will consume event from the latest
    Status RegisterListener(Listener* listener);
    void UnregisterListener(Listener* listener);

    static void* RunSuberRoutine(void*);

 private:
    void SuberRoutine(uint64_t id);
    void MaybeSweepLegacyEvents(uint64_t suber_id, size_t gap);

 private:
    // using routine to avoid message block
    struct Subscriber {
        const uint64_t id;
        const bthread_t routine;

        std::atomic<bool> running{false};
        Listener* listener{nullptr};
        std::atomic<uint64_t> ack_cursor{0};
        Subscriber(uint64_t id, bthread_t bthd) : id(id), routine(bthd), running(true) {}
    };

 private:
    std::atomic<bool> running_{false};
    bthread::Mutex mu_;
    bthread::ConditionVariable cv_;
    uint64_t issue_cursor_{0};
    std::map<uint64_t /* cursor */, butil::intrusive_ptr<Event>> events_;

    uint64_t suber_id_{0};
    std::unordered_map<uint64_t /* id */, std::shared_ptr<Subscriber>> subers_;
};
}  // namespace metaserver
}  // namespace bcache2

