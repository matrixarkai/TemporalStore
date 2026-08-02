// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/service/query_service.h"

#include <string>
#include <vector>

#include "butil/endpoint.h"
#include "byteraft/include/raft_node.h"

#include "common/logging.h"
#include "common/status.h"
#include "metaserver_v2/meta/server.h"

namespace bcache2 {
namespace metaserver {

void QueryServiceImpl::QueryLeader(google::protobuf::RpcController* controller,
                                   const QueryLeaderRequest* request, QueryLeaderResponse* response,
                                   google::protobuf::Closure* done) {
    // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);
    if (request->id().cluster_name() != FLAGS_metaserver_cluster_name) {
        *response->mutable_status() =
            Status::FailedPrecondition(fmt::format("cluster name mismatch {} Vs. {}",
                                                   request->id().cluster_name(),
                                                   FLAGS_metaserver_cluster_name))
                .ToRpcStatus();
        return;
    }

    const bool is_leader_ready = raft_server_->IsLeaderReady();
    response->mutable_status()->set_code(kOK);
    response->set_is_leader(is_leader_ready);
    if (!is_leader_ready) {
        byteraft::NodeId leader = raft_server_->LeaderNode();
        butil::EndPoint ep;
        int rc = butil::str2endpoint(leader.raft_addr.c_str(), &ep);
        if (rc != 0) {
            response->mutable_status()->set_code(kUnavailable);
            response->mutable_status()->set_message("I do not know");
            return;
        }
        Endpoint* rep = response->mutable_leader();
        rep->set_ip4(butil::ip2str(ep.ip).c_str());
        rep->set_port(FLAGS_metaserver_server_port);
    }
}

#define SANITIZE_LEADER_STATE(read_stale)                                   \
    if (!raft_server_->IsLeaderReady() && !(read_stale)) {                  \
        response->mutable_status()->set_code(kFailedPrecondition);          \
        response->mutable_status()->set_message("not leader or not ready"); \
        return;                                                             \
    }

void QueryServiceImpl::QueryManageInfo(google::protobuf::RpcController* controller,
                                       const EmptyRequest* request,
                                       QueryManageInfoResponse* response,
                                       google::protobuf::Closure* done) {
    // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);

    SANITIZE_LEADER_STATE(request->read_stale());
    *response->mutable_info() = metabase_->GetManageInfo();
}

void QueryServiceImpl::QueryClusterStatus(google::protobuf::RpcController* controller,
                                          const EmptyRequest* request,
                                          QueryClusterStatusResponse* response,
                                          google::protobuf::Closure* done) {
    // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);

    if (raft_server_->IsLeaderReady()) {
        auto server_in_safe_mode = metabase_->GetServerInSafeMode();
        for (auto&& server : server_in_safe_mode) {
            auto srv = response->add_server_in_safe_mode();
            srv->set_location_full_name(server.first);
            srv->set_damage_severity(DamageSeverityToString(server.second));
        }
        auto proxy_in_safe_mode = metabase_->GetProxyInSafeMode();
        for (auto&& proxy : proxy_in_safe_mode) {
            auto pry = response->add_proxy_in_safe_mode();
            pry->set_location_full_name(proxy.first);
            pry->set_damage_severity(DamageSeverityToString(proxy.second));
        }
    }

    std::vector<byteraft::NodeId> nodes = raft_server_->GetMembership();
    for (auto& node : nodes) {
        RaftNode node_pb;
        node_pb.set_peer_id(node.peer_id);
        node_pb.set_raft_addr(node.raft_addr);
        node_pb.set_snapshot_addr(node.snapshot_addr);
        *response->add_raft_nodes() = node_pb;
    }

    RaftNode node_pb;
    byteraft::NodeId node = raft_server_->LeaderNode();
    node_pb.set_peer_id(node.peer_id);
    node_pb.set_raft_addr(node.raft_addr);
    node_pb.set_snapshot_addr(node.snapshot_addr);
    *response->mutable_raft_leader_info() = node_pb;

    response->set_binary_version(BCACHE2_VERSION);
    response->set_raft_applied_index(metabase_->GetRaftIndex());
    response->set_cluster_name(FLAGS_metaserver_cluster_name);

    response->mutable_status()->set_code(kOK);
}

void QueryServiceImpl::ListServer(google::protobuf::RpcController* controller,
                                  const ListServerRequest* request, ListServerResponse* response,
                                  google::protobuf::Closure* done) {
    // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);

    SANITIZE_LEADER_STATE(request->read_stale());
    const std::string& substr = request->ip_substr();
    const std::string& tag = request->location().tag();
    bool list_all_tag = request->list_all_tag();
    ServerState expect_state = request->state();
    auto servers = metabase_->GetServerLocationManager()->List(
        request->location(), [tag, substr, expect_state, list_all_tag](const auto& s) {
            return (substr.empty() || s->GetName().find(substr) != std::string::npos) &&
                   (list_all_tag || tag == s->GetLocation().tag()) &&
                   (expect_state == ServerState::SERVER_UNKNOWN_STATE ||
                    expect_state == s->GetState());
        });

    // TODO(wuzhenyu) pagination
    for (auto&& server : servers) {
        auto srv_blk = response->add_servers();
        *srv_blk->mutable_server_info() = server->GetInfo();
        srv_blk->set_extra(server->RealtimeStats().ToString());
        for (auto& node : server->GetNodes()) {
            auto node_blk = srv_blk->add_node_info();
            node_blk->set_node_id(node->GetId());
            for (uint64_t pid : node->GetPartitionIds()) {
                node_blk->add_partition_ids(pid);
            }
        }
    }
}

void QueryServiceImpl::ListProxy(google::protobuf::RpcController* controller,
                                 const ListProxyRequest* request, ListProxyResponse* response,
                                 google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    SANITIZE_LEADER_STATE(request->read_stale());
    const std::string& substr = request->ip_substr();
    ProxyState expect_state = request->state();
    auto proxies = metabase_->GetProxyLocationManager()->List(
        request->location(), [substr, expect_state](const auto& s) {
            return (substr.empty() || s->GetName().find(substr) != std::string::npos) &&
                   (expect_state == ProxyState::PROXY_UNKNOWN_STATE ||
                    expect_state == s->GetState());
        });

    for (auto&& proxy : proxies) {
        auto blk = response->add_proxies();
        *blk->mutable_proxy_info() = proxy->GetInfo();
        if (proxy->GetState() == ProxyState::PROXY_NORMAL) {
            ProxyGroup* group = proxy->GetProxyGroup();
            auto info = group->GetInfo();
            blk->set_namespace_name(info.namespace_name());
            *blk->mutable_config() = info.config();
        }
        blk->set_extra(proxy->RealtimeStats().ToString());
    }
}

void QueryServiceImpl::ListNamespace(google::protobuf::RpcController* controller,
                                     const ListNamespaceRequest* request,
                                     ListNamespaceResponse* response,
                                     google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    SANITIZE_LEADER_STATE(request->read_stale());
    std::vector<NamespacePtr> nslist = metabase_->GetNamespaceManager()->List();
    for (auto&& ns : nslist) {
        *response->add_namespaces() = ns->GetInfo();
    }
}

void QueryServiceImpl::ListTable(google::protobuf::RpcController* controller,
                                 const ListTableRequest* request, ListTableResponse* response,
                                 google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    SANITIZE_LEADER_STATE(request->read_stale());
    if (!request->namespace_name().empty()) {
        NamespacePtr ns;
        Status status = metabase_->GetNamespaceManager()->Get(request->namespace_name(), &ns);
        if (!status.ok()) {
            *response->mutable_status() = status.ToRpcStatus();
            return;
        }
        if (!request->table_name().empty()) {
            TablePtr table;
            Status status = ns->Get(request->table_name(), &table);
            if (!status.ok()) {
                *response->mutable_status() = status.ToRpcStatus();
                return;
            }
            *response->add_tables() = table->GetInfo();
        } else {
            for (auto&& table : ns->List(true /* with_frozen */)) {
                *response->add_tables() = table->GetInfo();
            }
        }
        return;
    }

    std::vector<NamespacePtr> nslist = metabase_->GetNamespaceManager()->List();
    for (auto&& ns : nslist) {
        for (auto&& table : ns->List(true /* with_frozen */)) {
            *response->add_tables() = table->GetInfo();
        }
    }
}

void QueryServiceImpl::ListPartition(google::protobuf::RpcController* controller,
                                     const ListPartitionRequest* request,
                                     ListPartitionResponse* response,
                                     google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    SANITIZE_LEADER_STATE(request->read_stale());
    NamespacePtr ns;
    Status status = metabase_->GetNamespaceManager()->Get(request->namespace_name(), &ns);
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    TablePtr table;
    status = ns->Get(request->table_name(), &table);
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    partition_id_t pid(request->partition_id());
    std::vector<PartitionSetPtr> psets;
    if (pid.id > 0) {
        PartitionSetPtr pset;
        status = table->GetPartitionSet(pid.GetPartitionSetId(), &pset);
        if (!status.ok()) {
            *response->mutable_status() = status.ToRpcStatus();
            return;
        }
        psets.push_back(pset);
    } else {
        psets = table->GetAllPartitionSets();
    }
    for (auto& set : psets) {
        auto body = response->add_info();
        *body->mutable_set_info() = set->GetInfo();
        std::vector<PartitionPtr> partitions = set->GetAllPartitions();
        for (auto& p : partitions) {
            *(body->add_partition_info()) = p->GetInfo();
        }
    }
}

void QueryServiceImpl::ListServerPartition(google::protobuf::RpcController* controller,
                                           const ListServerPartitionRequest* request,
                                           ListServerPartitionResponse* response,
                                           google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    SANITIZE_LEADER_STATE(request->read_stale());

    ServerPtr server;
    if (request->server_id() > 0) {
        server = metabase_->GetServerLocationManager()->Get(request->server_id());
    } else if (request->has_endpoint()) {
        server = metabase_->GetServerLocationManager()->Get(request->endpoint());
    } else {
        *response->mutable_status() = Status::FailedPrecondition("invalid argument").ToRpcStatus();
    }
    if (!server) {
        *response->mutable_status() = Status::NotFound("not found").ToRpcStatus();
        return;
    }
    *response->mutable_server_info() = server->GetInfo();
    for (auto& node : server->GetNodes()) {
        auto nps = response->add_node_partitions();
        nps->set_node_id(node->GetId());
        for (const uint64_t id : node->GetPartitionIds()) {
            partition_id_t pid(id);
            TablePtr table;
            Status status = metabase_->GetNamespaceManager()->GetTable(pid.GetTableId(), &table);
            if (!status.ok()) {
                // Note: maybe table has been dropped just now, return error and you may retry this
                // query
                LOG_ERROR("meta state wrong, partition in node but not found in namespace")
                    .put("partition_id", id)
                    .put("node", node->GetId());
                *response->mutable_status() = Status::Internal("meta state wrong").ToRpcStatus();
                return;
            }
            PartitionPtr partition;
            status = table->GetPartition(id, &partition);
            if (!status.ok()) {
                // ditto.
                LOG_ERROR("meta state wrong, partition in node but not found in namespace")
                    .put("partition_id", id)
                    .put("node", node->GetId());
                *response->mutable_status() = Status::Internal("meta state wrong").ToRpcStatus();
                return;
            }

            auto np = nps->add_partitions();
            LoadRequest load_request;
            status = SerializeToLoadRequest(partition, false /* async_load */, &load_request);
            if (!status.ok()) {
                LOG_ERROR("failed to serialize partition load metadata")
                    .put("partition_id", id)
                    .put("node", node->GetId())
                    .put("status", status);
                *response->mutable_status() = status.ToRpcStatus();
                return;
            }
            np->set_id(id);
            np->set_state(partition->GetState());
            np->set_load_version(load_request.load_version());
            np->set_partition_uri(load_request.partition_uri());
            np->set_start_slot(load_request.start_slot());
            np->set_end_slot(load_request.end_slot());
            np->set_persistent_type(load_request.persistent_type());
            np->set_readonly(load_request.readonly());
            np->set_table_name(load_request.table_name());
            *np->mutable_config() = load_request.config();
            *np->mutable_membership() = load_request.membership();
        }  // for each partition of node
    }      // for each node
}

void QueryServiceImpl::ListProxyGroup(google::protobuf::RpcController* controller,
                                      const ListProxyGroupRequest* request,
                                      ListProxyGroupResponse* response,
                                      google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    SANITIZE_LEADER_STATE(request->read_stale());
    if (!request->namespace_name().empty()) {
        NamespacePtr ns;
        Status status = metabase_->GetNamespaceManager()->Get(request->namespace_name(), &ns);
        if (!status.ok()) {
            *response->mutable_status() = status.ToRpcStatus();
            return;
        }
        if (request->has_placement()) {
            ProxyGroupPtr group;
            status = ns->GetProxyCluster()->GetProxyGroup(request->placement(), &group);
            if (!status.ok()) {
                *response->mutable_status() = status.ToRpcStatus();
                return;
            }
            response->add_groups()->CopyFrom(group->GetInfo());
        } else {
            for (auto& group : ns->GetProxyCluster()->ListAllProxyGroups()) {
                response->add_groups()->CopyFrom(group->GetInfo());
            }
        }
        return;
    }
    std::vector<NamespacePtr> nslist = metabase_->GetNamespaceManager()->List();
    for (auto& ns : nslist) {
        for (auto& group : ns->GetProxyCluster()->ListAllProxyGroups()) {
            response->add_groups()->CopyFrom(group->GetInfo());
        }
    }
}

#undef SANITIZE_LEADER_STATE

}  // namespace metaserver
}  // namespace bcache2
