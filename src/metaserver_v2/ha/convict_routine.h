// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <atomic>
#include <memory>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "bthread/bthread.h"

#include "common/proto_enhance.h"
#include "metaserver_v2/event_harbor.h"
#include "metaserver_v2/events.h"
#include "metaserver_v2/ha/failure_detector.h"
#include "metaserver_v2/meta/metabase.h"
#include "metaserver_v2/metrics.h"
#include "metaserver_v2/raft_connector.h"

namespace bcache2 {
namespace metaserver {

class ConvictRoutine : public EventHarbor::Listener {
 public:
    struct Options {
        RaftConnector* raft_connector{nullptr};
        EventHarbor* event_harbor{nullptr};
    };

 public:
    explicit ConvictRoutine(Metabase* metabase);
    ~ConvictRoutine();

    Status Start(const Options& opts);
    void Stop();

    static void* RunRoutine(void* arg);

    void Consume(const EventHarbor::Event* e) override;

    std::set<EventHarbor::topic_t> Subscribed() override {
        static std::set<EventHarbor::topic_t> v{
            kTopicServerHeartbeat, kTopicServerDrop, kTopicServerStop,  //
            kTopicProxyHeartbeat,  kTopicProxyDrop,  kTopicProxyStop,
        };
        return v;
    }

 private:
    void Routine();
    void HandleServerHeartbeat(const ServerHeartbeatEvent* e);
    void HandleProxyHeartbeat(const ProxyHeartbeatEvent* e);

    void ColdBoot();
    void RefreshCacheIfNeeded();
    void InterpretServerForOneRound();
    void InterpretProxyForOneRound();
    void Convict(const ServerPtr& server);
    Status FreezeServer(const ServerPtr& server, FreezeServerReason reason);
    Status FreezeProxy(const ProxyPtr& proxy);
    Status DropProxy(const ProxyPtr& proxy);

 private:
    class DamageEstimator {
     public:
        DamageSeverity Estimate(const std::vector<Endpoint>& failure_endpoints);

     private:
        static constexpr size_t kDamageWindows = 10;  // min
        DamageSeverity last_severity_{DamageSeverity::kNormal};
        int64_t curr_timepoint_{butil::cpuwide_time_s() / 60};
        uint64_t cursor_{0};
        bool filled_{false};
        std::vector<std::unordered_set<Endpoint, EndpointHash>> failure_window_{kDamageWindows};
    };

 private:
    std::atomic<bool> running_{false};
    Metabase* metabase_{nullptr};
    std::unique_ptr<FailureDetector> fd_;
    RaftConnector* raft_connector_{nullptr};  // to propose conviction
    bthread_t routine_thd_;

    DamageEstimator server_damage_estimator_;

    int64_t last_cache_time_sec_{0};
    std::vector<ServerPtr> server_list_cache_;
    std::vector<ProxyPtr> proxy_list_cache_;
};

}  // namespace metaserver
}  // namespace bcache2
