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
#include "metaserver_v2/event_harbor.h"
#include "metaserver_v2/metrics.h"

namespace bcache2::metaserver::test {

/// make me happy
class EventHarborTest : public testing::Test {};

struct MockEvent : public EventHarbor::Event {
    explicit MockEvent(EventHarbor::topic_t t) : topic(t) {}
    EventHarbor::topic_t Topic() const override { return topic; }
    EventHarbor::topic_t topic;
};

struct MockListener : public EventHarbor::Listener {
    std::set<EventHarbor::topic_t> Subscribed() override { return {1, 2, 3}; }
    void Consume(const EventHarbor::Event* event) override { counter[event->Topic()]++; }

    std::map<EventHarbor::topic_t, size_t> counter;
};

TEST_F(EventHarborTest, PubSubTest) {
    InitMetrics("dev.bcache2.ut", {});
    BYTE_DEFER({ QuitMetrics(); });
    Status status;
    auto q = std::make_unique<EventHarbor>();
    status = q->Start();
    ASSERT_TRUE(status.ok()) << status;
    std::list<std::unique_ptr<MockListener>> listeners;
    for (int i = 0; i < 10; i++) {
        listeners.emplace_back(new MockListener());
        Status status = q->RegisterListener(listeners.back().get());
        ASSERT_TRUE(status.ok()) << status;
    }
    BYTE_DEFER({ q.reset(); });

    size_t round = butil::fast_rand() % 1000 + 100;
    size_t cnt = 5;
    for (size_t i = 0; i < round; i++) {
        for (size_t t = 0; t < cnt; t++) {
            q->Publish(new MockEvent(t));
        }
    }

    ASSERT_EQ(q->issue_cursor_, round * cnt);
    bool y = true;
    do {
        bthread_usleep(1'000'000);
        for (auto& l : listeners) {
            auto m = l->counter;
            for (auto iter : m) {
                if (iter.second < round) {
                    y = false;
                    break;
                }
            }
        }
        LOG_INFO("look, another waiting round....");
    } while (!y);

    for (auto& l : listeners) {
        auto m = l->counter;
        ASSERT_EQ(m.size(), l->Subscribed().size());
        for (auto iter : m) {
            ASSERT_EQ(iter.second, round);
        }
    }
    // ASSERT_EQ(q->events_.size(), 0);
}

}  // namespace bcache2::metaserver::test
