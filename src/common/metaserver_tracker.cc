// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "common/metaserver_tracker.h"

#include <chrono>
#include <string>
#include <utility>

#include "butil/string_splitter.h"

#include "common/logging.h"

namespace bcache2 {

/// comma separated string, parse rule:
///  1. consul://<consul_name>
///  2. [<ip6>]:<port>
///  3. <ip4>:<port>
///  DNS is not supported currently
DEFINE_string(metaserver_uri,
              "consul://dev.bcache2.metaserver.dev1.service.lf,[fddb::1]:7000,127.0.0.1:700",
              "metaserver uri, comma separated");
DEFINE_uint64(metaserver_tracker_timeout_ms, 3000, "tracker timeout in millisecond");
BRPC_VALIDATE_GFLAG(metaserver_tracker_timeout_ms, brpc::PassValidate);
DEFINE_uint64(metaserver_tracker_loop_interval_ms, 10 * 1000, "tracker interval in millisecond");
BRPC_VALIDATE_GFLAG(metaserver_tracker_loop_interval_ms, brpc::PassValidate);

//////

MetaServerTracker::MetaServerTracker(std::string cluster_name)
    : MetaServerTracker(std::move(cluster_name), FLAGS_metaserver_uri) {}

MetaServerTracker::MetaServerTracker(std::string cluster_name, std::string uri)
    : cluster_name_(std::move(cluster_name)), uri_(std::move(uri)) {}

MetaServerTracker::~MetaServerTracker() { LoopThread::Stop(); }

Status MetaServerTracker::Start() {
    std::set<butil::EndPoint> endpoints;
    Status status = ParseUri(&endpoints, true);
    if (!status.ok()) {
        LOG_WARNING("failed to get endpoint from uri").put("status", status).put("uri", uri_);
    } else {
        TrackMetaServer(std::move(endpoints));
    }
    return LoopThread::Start();
}

bool MetaServerTracker::IsLeader(const butil::EndPoint& ep) {
    std::lock_guard<std::mutex> guard(mu_);
    return ep == leader_;
}

std::vector<butil::EndPoint> MetaServerTracker::GetEndpoints() {
    std::vector<butil::EndPoint> result;
    std::lock_guard<std::mutex> guard(mu_);
    result.insert(result.end(), endpoints_.begin(), endpoints_.end());
    return result;
}

Status MetaServerTracker::GetLeaderEndpoint(butil::EndPoint* endpoint) {
    std::lock_guard<std::mutex> guard(mu_);
    if (leader_.ip != butil::IP_ANY) {
        *endpoint = leader_;
        return Status::OK();
    }
    return Status::NotFound("no leader found");
}

uint64_t MetaServerTracker::LoopIntervalMs() { return FLAGS_metaserver_tracker_loop_interval_ms; }

void MetaServerTracker::DoLoop() {
    std::set<butil::EndPoint> endpoints;
    Status status = ParseUri(&endpoints);
    if (!status.ok()) {
        LOG_WARNING("failed to get endpoint from uri").put("status", status).put("uri", uri_);
        return;
    }
    TrackMetaServer(std::move(endpoints));
}

Status MetaServerTracker::ParseUri(std::set<butil::EndPoint>* endpoints, bool strict) {
    const std::string& uri = uri_;
    size_t cnt = 0;
    butil::StringSplitter sp(uri.c_str(), ',', butil::SKIP_EMPTY_FIELD);
    bool partial_failure = false;
    for (; sp; sp++) {
        if (partial_failure && strict) {
            break;
        }

        std::string piece(sp.field(), sp.length());
        if (piece.find("consul://") == 0) {
            std::vector<service_discovery::Endpoint> points;
            Status status = sd_.Lookup(piece.data() + 9,
                                       service_discovery::Consul::AddrFamily::DualStack, &points);
            if (!status.ok()) {
                LOG_WARNING("consul lookup failed").put("field", piece).put("status", status);
                partial_failure = true;
                continue;
            }

            for (auto& cep : points) {
                butil::EndPoint point;
                int rc = butil::str2endpoint(cep.host.data(), cep.port, &point);
                if (rc != 0) {
                    LOG_WARNING("parse failed").put("field", piece);
                    partial_failure = true;
                } else {
                    LOG_DEBUG("find endpoint").put("v", point);
                    cnt++;
                    endpoints->insert(std::move(point));
                }
            }
        } else {
            butil::EndPoint point;
            int rc = butil::str2endpoint(piece.data(), &point);
            if (rc != 0) {
                LOG_WARNING("parse failed").put("field", piece);
                partial_failure = true;
            } else {
                LOG_DEBUG("find endpoint").put("v", point);
                cnt++;
                endpoints->insert(std::move(point));
            }
        }
    }
    if (cnt == 0 || (strict && partial_failure)) {
        return Status::Aborted("failed to parse endpoint from uri, partial fail or empty");
    }
    LOG_DEBUG("parse endpoint success").put("count", cnt).put("cluster", cluster_name_);
    return Status::OK();
}

Status MetaServerTracker::QueryLeader(const butil::EndPoint& ep, butil::EndPoint* hint) {
    brpc::ChannelOptions opts;
    opts.connect_timeout_ms = -1;
    opts.timeout_ms = FLAGS_metaserver_tracker_timeout_ms;
    brpc::Channel channel;
    if (channel.Init(ep, &opts) != 0) {
        return Status::Internal("init channel failed");
    }
    brpc::Controller cntl;
    metaserver::QueryService_Stub stub(&channel);
    metaserver::QueryLeaderRequest request;
    metaserver::QueryLeaderResponse response;
    request.mutable_id()->set_cluster_name(cluster_name_);
    stub.QueryLeader(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return Status::Internal("rpc failed");
    }
    Status status = Status::FromRpcStatus(response.status());
    if (!status.ok()) {
        return status;
    }
    if (response.is_leader()) {
        return Status::OK();
    }
    if (response.has_leader()) {
        butil::str2endpoint(response.leader().ip4().data(), response.leader().port(), hint);
    }
    return Status::Cancelled("not leader");
}

Status MetaServerTracker::TrackMetaServer(std::set<butil::EndPoint> endpoints) {
    butil::EndPoint hint{};
    butil::EndPoint leader_endpoint{};
    std::set<butil::EndPoint> tried_endpoints;
    auto checkOp = [&](const butil::EndPoint& point) -> bool {
        hint.ip = butil::IP_ANY;
        if (endpoints.count(point) == 0) {
            return false;
        }
        tried_endpoints.insert(point);
        butil::EndPoint new_hint;
        Status status = QueryLeader(point, &new_hint);
        if (status.ok()) {
            return true;
        }
        hint = new_hint;
        return false;
    };
    {
        std::lock_guard<std::mutex> guard(mu_);
        hint = leader_;
    }
    LOG_DEBUG("try quick path");
    for (size_t try_count = 0; try_count < endpoints.size(); try_count++) {
        if (checkOp(hint)) {
            leader_endpoint = hint;
            break;
        }
    }
    LOG_DEBUG("try rest all");
    if (leader_endpoint.ip == butil::IP_ANY) {
        for (auto& ep : endpoints) {
            if (tried_endpoints.count(ep) == 0 && checkOp(ep)) {
                leader_endpoint = ep;
                break;
            }
        }
    }

    if (leader_endpoint.ip == butil::IP_ANY) {
        LOG_WARNING("no leader found")
            .put("tried_count", endpoints.size())
            .put("cluster", cluster_name_);
    } else {
        LOG_DEBUG("leader found").put("endpoint", leader_endpoint);
    }
    std::lock_guard<std::mutex> guard(mu_);
    // TODO(wuzhenyu) remove illegal endpoint
    std::swap(endpoints_, endpoints);
    std::swap(leader_, leader_endpoint);
    return Status::OK();
}

}  // namespace bcache2
