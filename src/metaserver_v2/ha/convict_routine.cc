// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/ha/convict_routine.h"

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "butil/time.h"

#include "common/logging.h"
#include "common/status.h"
#include "metaserver_v2/flags.h"
#include "metaserver_v2/meta/server.h"

namespace bcache2 {
namespace metaserver {

ConvictRoutine::ConvictRoutine(Metabase* metabase)
    : metabase_(metabase), fd_(new FailureDetector()) {}

ConvictRoutine::~ConvictRoutine() { Stop(); }

Status ConvictRoutine::Start(const Options& opts) {
    if (running_) {
        return Status::Internal("already running");
    }
    running_ = true;
    raft_connector_ = opts.raft_connector;

    Status status = opts.event_harbor->RegisterListener(this);
    if (!status.ok()) {
        running_ = false;
        return status;
    }

    int rc = bthread_start_background(&routine_thd_, nullptr, RunRoutine, this);
    if (rc != 0) {
        running_ = false;
        return Status::Internal("failed to start background bthread");
    }
    return Status::OK();
}

void ConvictRoutine::Stop() {
    if (!running_) {
        return;
    }
    running_ = false;
    bthread_stop(routine_thd_);
    bthread_join(routine_thd_, nullptr);
}

void* ConvictRoutine::RunRoutine(void* arg) {
    auto cr = static_cast<ConvictRoutine*>(arg);
    cr->Routine();
    return nullptr;
}

void ConvictRoutine::Consume(const EventHarbor::Event* e) {
    if (e->Topic() == kTopicServerHeartbeat) {
        HandleServerHeartbeat(static_cast<const ServerHeartbeatEvent*>(e));
    } else if (e->Topic() == kTopicServerDrop) {
        fd_->Remove(static_cast<const ServerDropEvent*>(e)->endpoint);
    } else if (e->Topic() == kTopicServerStop) {
        ServerPtr server = static_cast<const ServerStopEvent*>(e)->server;
        if (server && server->GetState() == ServerState::SERVER_NORMAL) {
            LOG_INFO("detected server stop signal, freeze it")
                .put("endpoint", server->GetEndpoint());
            FreezeServer(server, FreezeServerReason::MAINTAIN);
        }
    } else if (e->Topic() == kTopicProxyHeartbeat) {
        HandleProxyHeartbeat(static_cast<const ProxyHeartbeatEvent*>(e));
    } else if (e->Topic() == kTopicProxyDrop) {
        fd_->Remove(static_cast<const ProxyDropEvent*>(e)->endpoint);
    } else if (e->Topic() == kTopicProxyStop) {
        ProxyPtr proxy = static_cast<const ProxyStopEvent*>(e)->proxy;
        if (proxy) {
            LOG_INFO("detected proxy stop signal, drop it").put("endpoint", proxy->GetEndpoint());
            DropProxy(proxy);
        }
    }
}

void ConvictRoutine::HandleServerHeartbeat(const ServerHeartbeatEvent* e) {
    const Endpoint& ep = e->request.endpoint();
    ServerPtr server = metabase_->GetServerLocationManager()->Get(ep);
    if (!server) {
        return;
    }
    if (server->GetState() != ServerState::SERVER_FROZEN) {
        fd_->Report(ep, e->GetPublishTimepointUs());
    }

    // Here we don't care about whether server is NORMAL state
    ServerStats* stats = server->MutableRealtimeStats();
    stats->SetLastHeartbeatTimeUs(butil::gettimeofday_us());
    stats->SetBinaryVersion(e->request.binary_version());

    const int64_t reported_boot_time_us = stats->GetReportedBootTimeUs();
    if (reported_boot_time_us == 0) {
        stats->SetReportedBootTimeUs(e->request.boot_time_us());
    } else if (reported_boot_time_us != e->request.boot_time_us() && !stats->IsRebootDetected()) {
        LOG_INFO("detected server rebooted!")
            .put("endpoint", ep)
            .put("record", reported_boot_time_us)
            .put("report", e->request.boot_time_us());
        stats->MarkRebootDetected();
        MS_METRIC(reboot_server_count).get()->Add(1);
    }
}

void ConvictRoutine::Routine() {
    LOG_INFO("cold booting");
    ColdBoot();

    LOG_INFO("enter routine");
    while (running_) {
        uint64_t start = butil::cpuwide_time_ms();
        RefreshCacheIfNeeded();
        InterpretServerForOneRound();
        InterpretProxyForOneRound();
        uint64_t elapse = butil::cpuwide_time_ms() - start;

        const uint64_t interval_ms = FLAGS_metaserver_convict_routine_interval_ms;
        if (elapse >= interval_ms) {
            LOG_WARNING("routine task took too long").put("elapse_ms", elapse);
            continue;
        }
        bthread_usleep((interval_ms - elapse) * 1'000);
    }
    LOG_INFO("exiting routine");
}

void ConvictRoutine::RefreshCacheIfNeeded() {
    int64_t now = butil::cpuwide_time_s();
    static constexpr int64_t kMaxCacheTimeSec = 10;
    if (now - last_cache_time_sec_ > kMaxCacheTimeSec || server_list_cache_.empty() ||
        proxy_list_cache_.empty()) {
        server_list_cache_ = metabase_->GetServerLocationManager()->ListAll();
        proxy_list_cache_ = metabase_->GetProxyLocationManager()->ListAll();
        last_cache_time_sec_ = now;
    }
}

void ConvictRoutine::InterpretServerForOneRound() {
    std::map<std::string, std::vector<ServerPtr>> abnormal_servers;
    std::map<std::string, std::vector<ServerPtr>> failure_servers;
    std::map<std::string, size_t> total_servers;
    size_t total_count = 0;
    size_t abnormal_count = 0;
    int64_t now = butil::cpuwide_time_us();
    for (const auto& server : server_list_cache_) {
        const std::string tag = server->GetLocation().vdc() + "_" + server->GetLocation().vau() +
                                "_" + server->GetLocation().tag();
        if (total_servers.count(tag) == 0) {
            total_servers[tag] = 1;
        } else {
            total_servers[tag]++;
        }
        if ((++total_count) % 100 == 0) {
            now = butil::cpuwide_time_us();
        }
        if (server->GetState() != ServerState::SERVER_NORMAL) {
            abnormal_count++;
            abnormal_servers[tag].push_back(server);
            continue;
        }
        const Endpoint& ep = server->GetEndpoint();
        if (server->MutableRealtimeStats()->IsRebootDetected()) {
            LOG_INFO("mark server as failure due to reboot detecting").put("endpoint", ep);
            abnormal_count++;
            abnormal_servers[tag].push_back(server);
            failure_servers[tag].push_back(server);
            continue;
        }

        FailureDetector::Diagnose result = fd_->Interpret(ep, now);
        switch (result) {
        case FailureDetector::Diagnose::kUnknown:
        case FailureDetector::Diagnose::kNormal:
            break;
        case FailureDetector::Diagnose::kNotExists:
            // TODO(wuzhenyu) refactor me
            fd_->Report(ep, now);
            LOG_INFO("no heartbeat reported, fall back").put("endpoint", ep);
            break;
        case FailureDetector::Diagnose::kFailure:
            LOG_INFO("mark server as failure due to phi calc").put("endpoint", ep);
            abnormal_count++;
            abnormal_servers[tag].push_back(server);
            failure_servers[tag].push_back(server);
            break;
        }
    }  // loop

    DamageSeverity ds = DamageSeverity::kNormal;
    std::unordered_map<std::string, DamageSeverity> server_in_safe_mode;
    for (const auto& server : total_servers) {
        const std::string& tag = server.first;
        const size_t tag_total_count = server.second;
        auto fiter = abnormal_servers.find(tag);
        if (fiter == abnormal_servers.end()) {
            continue;
        }
        const size_t tag_abnormal_count = fiter->second.size();
        g_metrics->EmitStore("abnormal_server_count", static_cast<int>(tag_abnormal_count),
                             {{"tag", tag}});
        if (tag_abnormal_count >
            tag_total_count * FLAGS_metaserver_convict_safe_mode_critical_ratio / 100) {
            ds = DamageSeverity::kCritical;
        } else if (tag_abnormal_count >
                   tag_total_count * FLAGS_metaserver_convict_safe_mode_warning_ratio / 100) {
            ds = DamageSeverity::kWarning;
        } else {
            ds = DamageSeverity::kNormal;
        }
        if (ds != DamageSeverity::kNormal && FLAGS_metaserver_convict_safe_mode_enabled) {
            server_in_safe_mode[tag] = ds;
            g_metrics->EmitStore("server_damage_severity", static_cast<int>(ds), {{"tag", tag}});
            LOG_WARNING("server reach tag safe mode")
                .put("sevirity", static_cast<int>(ds))
                .put("tag", tag)
                .put("abormal_count", tag_abnormal_count)
                .put("total", tag_total_count);
            continue;
        }

        std::vector<Endpoint> eps;
        for (const auto& iter : failure_servers[tag]) {
            eps.push_back(iter->GetEndpoint());
        }
        ds = server_damage_estimator_.Estimate(eps);
        if (ds != DamageSeverity::kNormal && FLAGS_metaserver_convict_safe_mode_enabled) {
            server_in_safe_mode[tag] = ds;
            g_metrics->EmitStore("server_damage_severity", static_cast<int>(ds), {{"tag", tag}});
            LOG_WARNING("server reach tag safe mode by histogram")
                .put("sevirity", static_cast<int>(ds))
                .put("tag", tag);
            continue;
        }

        if (eps.empty()) {
            LOG_DEBUG("all server healthy").put("tag", tag);
            continue;
        }

        if (FLAGS_metaserver_convict_server_enabled) {
            g_metrics->EmitCounter("server_convict_count", eps.size(), {{"tag", tag}});
            LOG_INFO("start to convict servers").put("count", eps.size());
            for (const auto& failure_server : failure_servers[tag]) {
                Convict(failure_server);
            }
        } else {
            LOG_WARNING("convict server disabled").put("count", eps.size());
        }
    }  // for tag
    metabase_->SetServerInSafeMode(std::move(server_in_safe_mode));
}

void ConvictRoutine::Convict(const ServerPtr& server) {
    for (auto& node : server->GetNodes()) {
        for (uint64_t id : node->GetPartitionIds()) {
            partition_id_t pid(id);
            TablePtr table;
            Status status = metabase_->GetNamespaceManager()->GetTable(pid.GetTableId(), &table);
            if (!status.ok()) {
                LOG_WARNING("meta state wrong").put("status", status);
                continue;
            }
            if (!table->CanFreezePartitionSafely(id)) {
                if (!FLAGS_metaserver_convict_force_for_orphan_partition) {
                    LOG_WARNING("server can not be frozen safely").put("cause_id", id);
                    return;
                } else {
                    LOG_WARNING("force freeze server").put("cause_id", id);
                }
            }
        }  // all pids of a node
    }
    FreezeServer(server, FreezeServerReason::CONVICT);
}

void InitRequestId(RequestId* id) {
    id->set_timestamp(butil::gettimeofday_s());
    id->set_cluster_name(FLAGS_metaserver_cluster_name);
    id->set_operator_name("convict_routine");
}

Status ConvictRoutine::FreezeServer(const ServerPtr& server, FreezeServerReason reason) {
    FreezeServerRequest request;
    InitRequestId(request.mutable_id());
    request.set_server_id(server->GetId());
    request.set_reason(reason);
    *request.mutable_endpoint() = server->GetEndpoint();
    request.set_force(true);
    uint64_t log_id = butil::fast_rand();
    LOG_INFO("propose to raft to freeze server")
        .put("log_id", log_id)
        .put("endpoint", request.endpoint());
    Status status = raft_connector_->Propose(log_id, MS_LOG_SERVER_FREEZE, &request);
    if (!status.ok()) {
        LOG_WARNING("propose to raft failed")
            .put("log_id", log_id)
            .put("status", status)
            .put("endpoint", request.endpoint());
    }
    return status;
}

void ConvictRoutine::ColdBoot() {
    static constexpr int64_t kMaxWaitingTimeSec = 5 * 60;  // 5 min
    int64_t start = butil::cpuwide_time_s();
    while (running_) {
        bthread_usleep(1'000'000);  // 1 sec
        auto servers = metabase_->GetServerLocationManager()->ListAll();
        size_t lost_count = 0;
        for (const auto& server : servers) {
            if (server->GetState() == ServerState::SERVER_NORMAL &&
                server->MutableRealtimeStats()->GetLastHeartbeatTimeUs() == 0) {
                lost_count++;
            }
        }
        LOG_INFO("get lost server stats").put("lost", lost_count).put("total", servers.size());
        if (lost_count == 0) {
            break;
        }
        const int64_t elapse = butil::cpuwide_time_s() - start;
        if (elapse > kMaxWaitingTimeSec) {
            LOG_WARNING("force exit waiting loop due to long time blocking")
                .put("elapse", elapse)
                .put("lost", lost_count)
                .put("total", servers.size());
            break;
        }
    }
}

DamageSeverity ConvictRoutine::DamageEstimator::Estimate(
    const std::vector<Endpoint>& failure_endpoints) {
    int64_t now = butil::cpuwide_time_s() / 60;
    bool cluster = false;
    if (now != curr_timepoint_) {
        if (now > curr_timepoint_ + static_cast<int64_t>(kDamageWindows) / 2) {
            cursor_ = 0;
            filled_ = false;
            for (auto& v : failure_window_) {
                v.clear();
            }
        } else {
            if (cursor_ == failure_window_.size() - 1) {
                cursor_ = 0;
                filled_ = true;
            } else {
                cursor_++;
            }
            if (filled_) {
                failure_window_[cursor_].clear();
            }
        }
        curr_timepoint_ = now;
        cluster = true;
    }

    for (auto& ep : failure_endpoints) {
        failure_window_[cursor_].insert(ep);
    }
    if (cluster) {
        // TODO(wuzhenyu) impl
        last_severity_ = DamageSeverity::kNormal;
    }
    return last_severity_;
}

void ConvictRoutine::HandleProxyHeartbeat(const ProxyHeartbeatEvent* e) {
    const Endpoint& ep = e->request.endpoint();
    ProxyPtr proxy = metabase_->GetProxyLocationManager()->Get(ep);
    if (!proxy) {
        return;
    }
    if (proxy->GetState() != ProxyState::PROXY_FROZEN) {
        fd_->Report(ep, e->GetPublishTimepointUs());
    }

    ProxyStats* stats = proxy->MutableRealtimeStats();
    stats->SetLastHeartbeatTimeUs(butil::gettimeofday_us());
    stats->SetBinaryVersion(e->request.binary_version());
    const int64_t reported_boot_time_us = stats->GetReportedBootTimeUs();
    if (reported_boot_time_us == 0) {
        stats->SetReportedBootTimeUs(e->request.boot_time_us());
    }
}

Status ConvictRoutine::DropProxy(const ProxyPtr& proxy) {
    DropProxyRequest request;
    InitRequestId(request.mutable_id());
    request.set_proxy_id(proxy->GetId());
    *request.mutable_endpoint() = proxy->GetEndpoint();
    uint64_t log_id = butil::fast_rand();
    LOG_INFO("propose to raft to drop proxy")
        .put("log_id", log_id)
        .put("endpoint", request.endpoint());
    Status status = raft_connector_->Propose(log_id, MS_LOG_PROXY_DROP, &request);
    if (!status.ok()) {
        LOG_WARNING("propose to raft failed")
            .put("log_id", log_id)
            .put("status", status)
            .put("endpoint", request.endpoint());
    }
    return status;
}

Status ConvictRoutine::FreezeProxy(const ProxyPtr& proxy) {
    DropProxyRequest request;
    InitRequestId(request.mutable_id());
    request.set_proxy_id(proxy->GetId());
    *request.mutable_endpoint() = proxy->GetEndpoint();
    uint64_t log_id = butil::fast_rand();
    LOG_INFO("propose to raft to drop proxy")
        .put("log_id", log_id)
        .put("endpoint", request.endpoint());
    Status status = raft_connector_->Propose(log_id, MS_LOG_PROXY_FREEZE, &request);
    if (!status.ok()) {
        LOG_WARNING("propose to raft failed")
            .put("log_id", log_id)
            .put("status", status)
            .put("endpoint", request.endpoint());
    }
    return status;
}

void ConvictRoutine::InterpretProxyForOneRound() {
    LOG_DEBUG("interpret proxy");
    std::map<std::string, std::vector<ProxyPtr>> abnormal_proxies;
    std::map<std::string, std::vector<ProxyPtr>> failure_proxies;
    std::map<std::string, size_t> total_proxies;
    size_t total_count = 0;
    size_t abnormal_count = 0;
    int64_t now = butil::cpuwide_time_us();
    for (const auto& proxy : proxy_list_cache_) {
        const std::string tag = proxy->GetLocation().vdc() + "_" + proxy->GetLocation().vau() +
                                "_" + proxy->GetLocation().tag();
        if (total_proxies.count(tag) == 0) {
            total_proxies[tag] = 1;
        } else {
            total_proxies[tag]++;
        }
        if ((++total_count) % 100 == 0) {
            now = butil::cpuwide_time_us();
        }
        if (proxy->GetState() == ProxyState::PROXY_FROZEN) {
            abnormal_count++;
            abnormal_proxies[tag].push_back(proxy);
            continue;
        }
        const Endpoint& ep = proxy->GetEndpoint();

        FailureDetector::Diagnose result = fd_->Interpret(ep, now);
        switch (result) {
        case FailureDetector::Diagnose::kUnknown:
        case FailureDetector::Diagnose::kNormal:
            break;
        case FailureDetector::Diagnose::kNotExists:
            fd_->Report(ep, now);
            break;
        case FailureDetector::Diagnose::kFailure:
            LOG_INFO("mark proxy as failure due to phi calc").put("endpoint", ep);
            abnormal_count++;
            abnormal_proxies[tag].push_back(proxy);
            failure_proxies[tag].push_back(proxy);
            break;
        }
    }  // loop

    DamageSeverity ds = DamageSeverity::kNormal;
    std::unordered_map<std::string, DamageSeverity> proxy_in_safe_mode;
    for (const auto& proxy : total_proxies) {
        const std::string& tag = proxy.first;
        const size_t tag_total_count = proxy.second;
        auto fiter = abnormal_proxies.find(tag);
        if (fiter == abnormal_proxies.end()) {
            continue;
        }
        const size_t tag_abnormal_count = fiter->second.size();
        if (tag_abnormal_count >
            tag_total_count * FLAGS_metaserver_convict_safe_mode_critical_ratio / 100) {
            ds = DamageSeverity::kCritical;
        } else if (tag_abnormal_count >
                   tag_total_count * FLAGS_metaserver_convict_safe_mode_warning_ratio / 100) {
            ds = DamageSeverity::kWarning;
        } else {
            ds = DamageSeverity::kNormal;
        }
        if (ds != DamageSeverity::kNormal) {
            proxy_in_safe_mode[tag] = ds;
            g_metrics->EmitStore("proxy_damage_severity", static_cast<int>(ds), {{"tag", tag}});
            LOG_WARNING("proxy reach tag safe mode")
                .put("severity", static_cast<int>(ds))
                .put("tag", tag)
                .put("abormal_count", tag_abnormal_count)
                .put("total", tag_total_count);
            continue;
        }

        std::vector<Endpoint> eps;
        for (const auto& failure_proxy : failure_proxies[tag]) {
            eps.push_back(failure_proxy->GetEndpoint());
        }
        if (eps.empty()) {
            continue;
        }

        if (FLAGS_metaserver_convict_proxy_enabled) {
            g_metrics->EmitCounter("proxy_convict_count", eps.size(), {{"tag", tag}});
            LOG_INFO("start to convict proxy").put("count", eps.size());
            for (auto& failure_proxy : failure_proxies[tag]) {
                FreezeProxy(failure_proxy);
            }
        } else {
            LOG_WARNING("convict proxy disabled").put("count", eps.size());
        }
    }  // for tag
    metabase_->SetProxyInSafeMode(std::move(proxy_in_safe_mode));
}

}  // namespace metaserver
}  // namespace bcache2
