// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/service/heartbeat_service.h"

#include <memory>
#include <string>
#include <utility>

#include "common/logging.h"
#include "common/proto_enhance.h"
#include "metaserver_v2/events.h"
#include "metaserver_v2/flags.h"
#include "metaserver_v2/meta/metabase.h"
#include "metaserver_v2/raft_server.h"

namespace bcache2 {
namespace metaserver {

HeartbeatServiceImpl::HeartbeatServiceImpl(RaftServer* s, Metabase* mb, EventHarbor* eh)
    : raft_server_(s), metabase_(mb), event_harbor_(eh) {}

void HeartbeatServiceImpl::ServerHeartbeat(google::protobuf::RpcController* controller,
                                           const ServerHeartbeatRequest* request,
                                           ServerHeartbeatResponse* response,
                                           google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);
    auto clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    ServerPtr server = metabase_->GetServerLocationManager()->Get(request->endpoint());
    if (!server) {
        *response->mutable_status() = Status::NotFound("server not found").ToRpcStatus();
        return;
    }

    if (server->GetState() == ServerState::SERVER_FROZEN) {
        *response->mutable_status() = Status::ResourceFrozen("server frozen").ToRpcStatus();
        const FreezeServerReason freeze_reason = server->GetInfo().freeze_reason();
        if (freeze_reason == FreezeServerReason::MAINTAIN ||
            (freeze_reason == FreezeServerReason::CONVICT &&
             FLAGS_metaserver_forbid_auto_register_for_convict_server)) {
            response->set_forbid_auto_register(true);
        }
    }
    MS_METRIC(server_heartbeat_count)->Add(1);
    event_harbor_->Publish(new ServerHeartbeatEvent(clone_req));
}

void HeartbeatServiceImpl::ServerNotifyStop(google::protobuf::RpcController* controller,
                                            const ServerNotifyStopRequest* request,
                                            AckResponse* response,
                                            google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);
    auto clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    ServerPtr server = metabase_->GetServerLocationManager()->Get(request->endpoint());
    if (!server) {
        *response->mutable_status() = Status::NotFound("server not found").ToRpcStatus();
        return;
    }
    if (server->GetState() != ServerState::SERVER_NORMAL) {
        *response->mutable_status() = Status::FailedPrecondition("state not normal").ToRpcStatus();
        return;
    }
    event_harbor_->Publish(new ServerStopEvent(std::move(server)));
}

void HeartbeatServiceImpl::ProxyHeartbeat(google::protobuf::RpcController* controller,
                                          const ProxyHeartbeatRequest* request,
                                          ProxyHeartbeatResponse* response,
                                          google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);
    auto clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    ProxyPtr proxy = metabase_->GetProxyLocationManager()->Get(request->endpoint());
    if (!proxy) {
        *response->mutable_status() = Status::NotFound("proxy not found").ToRpcStatus();
        return;
    }

    if (proxy->GetState() == ProxyState::PROXY_FROZEN) {
        *response->mutable_status() = Status::ResourceFrozen("proxy frozen").ToRpcStatus();
    }
    ProxyGroup* group = proxy->GetProxyGroup();
    if (group != nullptr) {
        ProxyGroupInfo info = group->GetInfo();
        if (info.namespace_name() != request->namespace_name() ||
            info.config().version() > request->config_version()) {
            response->set_config_changed(true);
            response->set_namespace_name(info.namespace_name());
            *response->mutable_config() = info.config();
        }
    } else if (!request->namespace_name().empty()) {
        // release to idle state
        response->set_config_changed(true);
    }

    event_harbor_->Publish(new ProxyHeartbeatEvent(clone_req));
}

void HeartbeatServiceImpl::ProxyNotifyStop(google::protobuf::RpcController* controller,
                                           const ProxyNotifyStopRequest* request,
                                           AckResponse* response, google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);
    auto clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    ProxyPtr proxy = metabase_->GetProxyLocationManager()->Get(request->endpoint());
    if (!proxy) {
        *response->mutable_status() = Status::NotFound("proxy not found").ToRpcStatus();
        return;
    }
    if (proxy->GetState() != ProxyState::PROXY_NORMAL) {
        *response->mutable_status() = Status::FailedPrecondition("state not normal").ToRpcStatus();
        return;
    }
    event_harbor_->Publish(new ProxyStopEvent(std::move(proxy)));
}

Status HeartbeatServiceImpl::SanitizeRequest(brpc::Controller* cntl, RequestId* id) {
    if (!raft_server_->IsLeaderReady()) {
        return Status::FailedPrecondition("not leader or not ready");
    }
    if (id->cluster_name() != FLAGS_metaserver_cluster_name) {
        return Status::FailedPrecondition(fmt::format(
            "cluster name mismatch {} Vs. {}", id->cluster_name(), FLAGS_metaserver_cluster_name));
    }
    id->set_timestamp(butil::gettimeofday_s());
    std::string remote_ep = butil::endpoint2str(cntl->remote_side()).c_str();
    id->set_client_endpoint(std::move(remote_ep));
    return Status::OK();
}

}  // namespace metaserver
}  // namespace bcache2

