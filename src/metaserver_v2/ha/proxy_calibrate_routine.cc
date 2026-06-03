// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/ha/proxy_calibrate_routine.h"

#include <map>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "butil/time.h"

#include "common/logging.h"
#include "common/status.h"
#include "metaserver_v2/flags.h"
#include "metaserver_v2/meta/proxy.h"

namespace bcache2 {
namespace metaserver {

std::random_device t_random_device;
std::mt19937 t_mt19937(t_random_device());

ProxyCalibrateRoutine::~ProxyCalibrateRoutine() { Stop(); }

Status ProxyCalibrateRoutine::Start(const Options& opts) {
    if (running_) {
        return Status::Internal("already running");
    }
    running_ = true;
    metabase_ = opts.metabase;
    raft_connector_ = opts.raft_connector;

    int rc = bthread_start_background(&routine_thd_, nullptr, RunRoutine, this);
    if (rc != 0) {
        running_ = false;
        return Status::Internal("failed to start background bthread");
    }
    return Status::OK();
}

void ProxyCalibrateRoutine::Stop() {
    if (!running_) {
        return;
    }
    running_ = false;
    bthread_stop(routine_thd_);
    bthread_join(routine_thd_, nullptr);
}

void* ProxyCalibrateRoutine::RunRoutine(void* arg) {
    auto cr = static_cast<ProxyCalibrateRoutine*>(arg);
    cr->Routine();
    return nullptr;
}

void ProxyCalibrateRoutine::Routine() {
    LOG_INFO("enter routine");
    while (running_) {
        const uint64_t interval_us = FLAGS_metaserver_proxy_calibrate_interval_ms * 1'000;
        bthread_usleep(interval_us);

        Calibrate();
    }
    LOG_INFO("exiting routine");
}

static int SafeCount(int curr_count, int delta) {
    // TODO(wuzhenyu) refactor hard code
    if (delta < 0) {
        int abs_gap = -delta;
        if (abs_gap > curr_count * 0.5) {
            return -(curr_count * 0.3 + 1);
        }
        return delta;
    }

    return delta > 100 ? 100 : delta;
}

void ProxyCalibrateRoutine::Calibrate() {
    NamespaceManager* ns_mgr = metabase_->GetNamespaceManager();
    std::vector<NamespacePtr> ns_list = ns_mgr->List();
    for (auto ns : ns_list) {
        ProxyClusterPtr proxy_cluster = ns->GetProxyCluster();
        std::vector<ProxyGroupPtr> groups = proxy_cluster->ListAllProxyGroups();
        for (auto& group : groups) {
            ProxyGroupInfo info = group->GetInfo();
            int curr_count = group->GetProxyCount();
            int delta = static_cast<int>(info.instance_num()) - curr_count;
            if (delta == 0) {
                continue;
            }
            int exp_delta = SafeCount(curr_count, delta);
            size_t act_count = 0;
            if (exp_delta > 0) {
                act_count = AcquireInstance(group, exp_delta);
            } else {
                act_count = ReleaseInstance(group, -exp_delta);
            }
            LOG_INFO("try to calibrate proxy instance")
                .put("namespace", info.namespace_name())
                .put("loc", info.placement())
                .put("current", curr_count)
                .put("delta", delta)
                .put("exp_delta", exp_delta)
                .put("act_count", act_count);
        }
    }
}

static void InitRequestId(RequestId* id) {
    id->set_timestamp(butil::gettimeofday_s());
    id->set_cluster_name(FLAGS_metaserver_cluster_name);
    id->set_operator_name("calibrate_routine");
}

size_t ProxyCalibrateRoutine::AcquireInstance(const ProxyGroupPtr& group, int delta) {
    CHECK_GT(delta, 0);
    ProxyGroupInfo info = group->GetInfo();
    size_t attach_count = 0;
    LocationManager<Proxy>* loc_mgr = metabase_->GetProxyLocationManager();

    const Location& loc_expect = info.placement();
    auto proxies = loc_mgr->List(loc_expect, [&loc_expect](const auto& proxy) -> bool {
        return proxy->GetState() == ProxyState::PROXY_IDLE &&
               proxy->RealtimeStats().GetLastHeartbeatTimeUs() > 0 &&
               loc_expect.tag() == proxy->GetLocation().tag();
    });
    if (proxies.empty()) {
        LOG_INFO("no proxy found")
            .put("ns", info.namespace_name())
            .put("location", info.placement());
        // TODO(wuzhenyu) metrics
        return 0;
    }
    int right = proxies.size() - 1;
    for (int i = 0; i < delta && right >= 0; i++, right--) {
        size_t luck = right == 0 ? 0 : t_mt19937() % right;
        const ProxyPtr proxy = proxies[luck];
        Status status = AttachProxy(proxy, info);
        if (status.ok()) {
            attach_count++;
        }
        std::swap(proxies[luck], proxies[right]);
    }
    return attach_count;
}

size_t ProxyCalibrateRoutine::ReleaseInstance(const ProxyGroupPtr& group, int delta) {
    CHECK_GT(delta, 0);
    size_t detach_count = 0;
    std::vector<ProxyPtr> proxies = group->ListAllProxies();
    int right = proxies.size() - 1;
    for (int i = 0; i < delta && right >= 0; i++, right--) {
        size_t luck = right == 0 ? 0 : t_mt19937() % right;
        const ProxyPtr proxy = proxies[luck];
        if (proxy->GetState() == ProxyState::PROXY_NORMAL) {
            Status status = DetachProxy(proxy);
            if (status.ok()) {
                detach_count++;
            }
        }
        std::swap(proxies[luck], proxies[right]);
    }

    return detach_count;
}

Status ProxyCalibrateRoutine::DetachProxy(const ProxyPtr& proxy) {
    DetachProxyRequest request;
    InitRequestId(request.mutable_id());
    request.set_proxy_id(proxy->GetId());
    uint64_t log_id = butil::fast_rand();
    LOG_INFO("propose to raft to detach proxy")
        .put("log_id", log_id)
        .put("endpoint", proxy->GetEndpoint());
    Status status = raft_connector_->Propose(log_id, MS_LOG_PROXY_DETACH, &request);
    if (!status.ok()) {
        LOG_WARNING("propose to raft failed").put("log_id", log_id).put("status", status);
    }
    return status;
}

Status ProxyCalibrateRoutine::AttachProxy(const ProxyPtr& proxy, const ProxyGroupInfo& info) {
    AttachProxyRequest request;
    InitRequestId(request.mutable_id());
    request.set_proxy_id(proxy->GetId());
    request.set_namespace_name(info.namespace_name());
    *request.mutable_placement() = info.placement();
    uint64_t log_id = butil::fast_rand();
    LOG_INFO("propose to raft to attach proxy")
        .put("log_id", log_id)
        .put("endpoint", proxy->GetEndpoint());
    Status status = raft_connector_->Propose(log_id, MS_LOG_PROXY_ATTACH, &request);
    if (!status.ok()) {
        LOG_WARNING("propose to raft failed").put("log_id", log_id).put("status", status);
    }
    return status;
}

}  // namespace metaserver
}  // namespace bcache2
