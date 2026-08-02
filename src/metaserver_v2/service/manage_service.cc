// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/service/manage_service.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "butil/endpoint.h"
#include "spdlog/fmt/fmt.h"

#include "common/logging.h"
#include "common/proto_enhance.h"
#include "metaserver_v2/flags.h"

namespace bcache2 {
namespace metaserver {

// TODO(wuzhenyu) move audit log separate log file
#define AUDIT_LOG_ANCHOR(severity)                          \
    BYTE_DEFER({                                            \
        LOG_##severity("manage rpc trace")                  \
            .put("request_type", clone_req.GetTypeName())   \
            .put("request", clone_req.ShortDebugString())   \
            .put("response", response->ShortDebugString()); \
    })

void ManageServiceImpl::AddServer(google::protobuf::RpcController* controller,
                                  const AddServerRequest* request, AckResponse* response,
                                  google::protobuf::Closure* done) {
    //
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);

    AddServerRequest clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }

    AUDIT_LOG_ANCHOR(WARNING);

    ServerInfo info = Request2Info(*request);
    status = Validate(info);
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    ServerPtr server = metabase_->GetServerLocationManager()->Get(request->endpoint());
    if (server) {
        *response->mutable_status() = Status::AlreadyExists("already exists").ToRpcStatus();
        return;
    }

    status = raft_server_->Propose(cntl->log_id(), MS_LOG_SERVER_ADD, &clone_req);
    *response->mutable_status() = status.ToRpcStatus();
}

void ManageServiceImpl::FreezeServer(google::protobuf::RpcController* controller,
                                     const FreezeServerRequest* request, AckResponse* response,
                                     google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);

    FreezeServerRequest clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    AUDIT_LOG_ANCHOR(WARNING);

    ServerPtr server;
    if (request->server_id() > 0) {
        server = metabase_->GetServerLocationManager()->Get(request->server_id());
    } else {
        server = metabase_->GetServerLocationManager()->Get(request->endpoint());
    }
    if (!server) {
        *response->mutable_status() = Status::NotFound("not found").ToRpcStatus();
        return;
    }

    if (server->GetState() != ServerState::SERVER_NORMAL) {
        *response->mutable_status() =
            Status::FailedPrecondition("server state mismatch").ToRpcStatus();
        return;
    }
    for (auto& node : server->GetNodes()) {
        for (uint64_t id : node->GetPartitionIds()) {
            PartitionPtr partition;
            Status status = metabase_->LocatePartition(id, &partition);
            if (!status.ok()) {
                *response->mutable_status() = status.ToRpcStatus();
                return;
            }
            Table* table = partition->GetPartitionSet()->GetTable();
            CHECK(table);
            if (!table->CanFreezePartitionSafely(id) && !request->force()) {
                LOG_INFO("server can not be frozen safely").put("cause_id", id);
                *response->mutable_status() =
                    Status::FailedPrecondition(fmt::format("server can not be frozen safely, try "
                                                           "add force option if need, cause pid {}",
                                                           id))
                        .ToRpcStatus();
                return;
            }
        }  // all pids of a node
    }

    if (clone_req.server_id() == 0) {
        clone_req.set_server_id(server->GetId());
    }
    status = raft_server_->Propose(cntl->log_id(), MS_LOG_SERVER_FREEZE, &clone_req);
    *response->mutable_status() = status.ToRpcStatus();
}

void ManageServiceImpl::DropServer(google::protobuf::RpcController* controller,
                                   const DropServerRequest* request, AckResponse* response,
                                   google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);

    DropServerRequest clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    AUDIT_LOG_ANCHOR(WARNING);

    ServerPtr server;
    if (request->server_id() > 0) {
        server = metabase_->GetServerLocationManager()->Get(request->server_id());
    } else {
        server = metabase_->GetServerLocationManager()->Get(request->endpoint());
    }
    if (!server) {
        *response->mutable_status() = Status::NotFound("not found").ToRpcStatus();
        return;
    }

    if (server->GetState() != ServerState::SERVER_FROZEN) {
        *response->mutable_status() =
            Status::FailedPrecondition("server state mismatch").ToRpcStatus();
        return;
    }
    if (clone_req.server_id() == 0) {
        clone_req.set_server_id(server->GetId());
    }
    status = raft_server_->Propose(cntl->log_id(), MS_LOG_SERVER_DROP, &clone_req);
    *response->mutable_status() = status.ToRpcStatus();
}

void ManageServiceImpl::UpdateServer(google::protobuf::RpcController* controller,
                                     const UpdateServerRequest* request, AckResponse* response,
                                     google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);

    auto clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }

    AUDIT_LOG_ANCHOR(WARNING);

    ServerPtr server = metabase_->GetServerLocationManager()->Get(request->endpoint());
    if (!server) {
        *response->mutable_status() = Status::NotFound("not found").ToRpcStatus();
        return;
    }
    if (server->GetState() != ServerState::SERVER_NORMAL) {
        *response->mutable_status() =
            Status::FailedPrecondition("server state is not normal").ToRpcStatus();
        return;
    }

    status = raft_server_->Propose(cntl->log_id(), MS_LOG_SERVER_UPDATE, &clone_req);
    *response->mutable_status() = status.ToRpcStatus();
}

void ManageServiceImpl::AddProxy(google::protobuf::RpcController* controller,
                                 const AddProxyRequest* request, AckResponse* response,
                                 google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);

    auto clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    AUDIT_LOG_ANCHOR(WARNING);

    ProxyInfo info = Request2Info(*request);
    status = Validate(info);
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    ProxyPtr proxy = metabase_->GetProxyLocationManager()->Get(request->endpoint());
    if (proxy) {
        *response->mutable_status() = Status::AlreadyExists("already exists").ToRpcStatus();
        return;
    }

    status = raft_server_->Propose(cntl->log_id(), MS_LOG_PROXY_ADD, &clone_req);
    *response->mutable_status() = status.ToRpcStatus();
}

void ManageServiceImpl::DropProxy(google::protobuf::RpcController* controller,
                                  const DropProxyRequest* request, AckResponse* response,
                                  google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);

    auto clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    AUDIT_LOG_ANCHOR(WARNING);

    ProxyPtr proxy;
    if (request->proxy_id() != 0) {
        proxy = metabase_->GetProxyLocationManager()->Get(request->proxy_id());
    } else {
        proxy = metabase_->GetProxyLocationManager()->Get(request->endpoint());
    }
    if (!proxy) {
        *response->mutable_status() = Status::NotFound("proxy not found").ToRpcStatus();
        return;
    }
    clone_req.set_proxy_id(proxy->GetId());
    status = raft_server_->Propose(cntl->log_id(), MS_LOG_PROXY_DROP, &clone_req);
    *response->mutable_status() = status.ToRpcStatus();
}

void ManageServiceImpl::FreezeProxy(google::protobuf::RpcController* controller,
                                    const FreezeProxyRequest* request, AckResponse* response,
                                    google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);

    auto clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    AUDIT_LOG_ANCHOR(WARNING);

    ProxyPtr proxy;
    if (request->proxy_id() != 0) {
        proxy = metabase_->GetProxyLocationManager()->Get(request->proxy_id());
    } else {
        proxy = metabase_->GetProxyLocationManager()->Get(request->endpoint());
    }
    if (!proxy) {
        *response->mutable_status() = Status::NotFound("proxy not found").ToRpcStatus();
        return;
    }
    clone_req.set_proxy_id(proxy->GetId());
    status = raft_server_->Propose(cntl->log_id(), MS_LOG_PROXY_FREEZE, &clone_req);
    *response->mutable_status() = status.ToRpcStatus();
}

void ManageServiceImpl::UpdateManageInfo(google::protobuf::RpcController* controller,
                                         const UpdateManageInfoRequest* request,
                                         AckResponse* response, google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);

    auto clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }

    BYTE_DEFER({
        LOG_WARNING("rpc trace")
            .put("func", __FUNCTION__)
            .put("request", clone_req.ShortDebugString())
            .put("response", response->ShortDebugString());
    });

    status = raft_server_->Propose(cntl->log_id(), MS_LOG_MANAGE_INFO_UPDATE, &clone_req);
    *response->mutable_status() = status.ToRpcStatus();
}

void ManageServiceImpl::AddNamespace(google::protobuf::RpcController* controller,
                                     const AddNamespaceRequest* request, AckResponse* response,
                                     google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);

    auto clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    AUDIT_LOG_ANCHOR(WARNING);

    NamespaceInfo info = Request2Info(clone_req);
    status = metabase_->GetNamespaceManager()->ValidateNamespaceInfo(info);
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }

    status = raft_server_->Propose(cntl->log_id(), MS_LOG_NS_ADD, &clone_req);
    *response->mutable_status() = status.ToRpcStatus();
}

static void MergeWithDefaultTableConfig(Config* config) {
    EvicterConfig default_evict_config;
    default_evict_config.mutable_maxmemory()->set_value(
        FLAGS_metaserver_table_default_evicter_max_memory_mb * 1024 * 1024);
    config->mutable_evicter_config()->MergeFrom(default_evict_config);
}

void ManageServiceImpl::AddTable(google::protobuf::RpcController* controller,
                                 const AddTableRequest* request, AckResponse* response,
                                 google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);

    auto clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    AUDIT_LOG_ANCHOR(WARNING);

    // Preserve the requested election policy. The old hard-coded PROMOTE_DERIVED
    // path is correct for shared-store anti-entropy recovery, but Raft data-node
    // replication must keep one stable replica group and promote an existing
    // secondary instead of creating/freeze-rotating derived partitions.

    if (metabase_->GetNamespaceManager()->GetTableIdCursor() == kMaxTableId - 1) {
        *response->mutable_status() =
            Status::Internal("table id resource is empty, id reuse is not implemented")
                .ToRpcStatus();
        return;
    }

    MergeWithDefaultTableConfig(clone_req.mutable_config());
    TableInfo info = Request2Info(clone_req);
    status = metabase_->GetNamespaceManager()->ValidateTableInfo(info);
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }

    status = raft_server_->Propose(cntl->log_id(), MS_LOG_TABLE_ADD, &clone_req);
    *response->mutable_status() = status.ToRpcStatus();
}

void ManageServiceImpl::UpdateTable(google::protobuf::RpcController* controller,
                                    const UpdateTableRequestV2* request, AckResponse* response,
                                    google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);

    auto clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    AUDIT_LOG_ANCHOR(WARNING);

    NamespacePtr ns;
    TablePtr table;
    status = metabase_->GetNamespaceManager()->Get(request->namespace_name(), &ns);
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    status = ns->Get(request->name(), &table);
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    if (table->GetId() != request->table_id()) {
        *response->mutable_status() =
            Status::FailedPrecondition("table_id not match").ToRpcStatus();
        return;
    }
    if (table->GetState() != TableState::TABLE_NORMAL && !request->force()) {
        *response->mutable_status() =
            Status::FailedPrecondition("table is not normal, set force to force freeze")
                .ToRpcStatus();
        return;
    }
    if (request->has_update_partition_unit()) {
        TableInfo info = table->GetInfo();
        auto find_unit = [&info](uint32_t id) -> PartitionUnit* {
            for (int i = 0; i < info.partition_units_size(); i++) {
                auto unit_original_ptr = info.mutable_partition_units(i);
                if (unit_original_ptr->id() == id) {
                    return unit_original_ptr;
                }
            }
            return nullptr;
        };
        auto& unit = request->update_partition_unit();
        PartitionUnit* unit_original_ptr = find_unit(unit.id());
        if (unit_original_ptr == nullptr) {
            *response->mutable_status() =
                Status::FailedPrecondition("unit not found").ToRpcStatus();
            return;
        }
        // restore storage_pool_uri since it is not mutable
        PartitionUnit tmp = *unit_original_ptr;
        *unit_original_ptr = unit;
        unit_original_ptr->set_storage_pool_uri(tmp.storage_pool_uri());
        status = Validate(info);
        if (!status.ok()) {
            *response->mutable_status() = status.ToRpcStatus();
            return;
        }

        if (table->GetElectionPolicy() == ElectionPolicy::PROMOTE_DERIVED) {
            std::unordered_map<std::string, std::vector<Location>> vdc2loc;
            for (uint32_t i = 0; i < unit.partition_num(); i++) {
                const Location& loc = unit.placement_set(i % unit.placement_set_size());
                vdc2loc[loc.vregion() + loc.vdc()].push_back(loc);
            }
            for (auto pset : table->GetAllPartitionSets()) {
                auto partition = pset->GetPrimary(unit.id());
                auto loc = partition->GetPlacementExpect();
                std::string vdc = loc.vregion() + loc.vdc();
                if (vdc2loc.count(vdc) == 0) {
                    *response->mutable_status() =
                        Status::FailedPrecondition("primary partition would be removed")
                            .ToRpcStatus();
                    return;
                }

                // Note: just test 1 partition for PROMOTE_DERIVED scenario
                break;
            }
        }
    }

    status = raft_server_->Propose(cntl->log_id(), MS_LOG_TABLE_UPDATE, &clone_req);
    *response->mutable_status() = status.ToRpcStatus();
}

void ManageServiceImpl::FreezeTable(google::protobuf::RpcController* controller,
                                    const FreezeTableRequest* request, AckResponse* response,
                                    google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);

    auto clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    AUDIT_LOG_ANCHOR(WARNING);

    NamespacePtr ns;
    TablePtr table;
    status = metabase_->GetNamespaceManager()->Get(request->namespace_name(), &ns);
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    status = ns->Get(request->name(), &table);
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    if (table->GetId() != request->table_id()) {
        *response->mutable_status() =
            Status::FailedPrecondition("table_id not match").ToRpcStatus();
        return;
    }
    TableState state = table->GetState();
    if (state == TableState::TABLE_FROZEN) {
        *response->mutable_status() = Status::FailedPrecondition("already frozen").ToRpcStatus();
        return;
    } else if (state != TableState::TABLE_NORMAL && !request->force()) {
        *response->mutable_status() =
            Status::FailedPrecondition("table is not normal, set force to force freeze")
                .ToRpcStatus();
        return;
    }

    status = raft_server_->Propose(cntl->log_id(), MS_LOG_TABLE_FREEZE, &clone_req);
    *response->mutable_status() = status.ToRpcStatus();
}

void ManageServiceImpl::DropTable(google::protobuf::RpcController* controller,
                                  const DropTableRequest* request, AckResponse* response,
                                  google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);

    auto clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    AUDIT_LOG_ANCHOR(WARNING);

    TablePtr table;
    status = metabase_->GetNamespaceManager()->GetTable(request->table_id(), &table);
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    if (table->GetState() != TableState::TABLE_FROZEN) {
        *response->mutable_status() =
            Status::FailedPrecondition("table is not frozen").ToRpcStatus();
        return;
    }

    status = raft_server_->Propose(cntl->log_id(), MS_LOG_TABLE_DROP, &clone_req);
    *response->mutable_status() = status.ToRpcStatus();
}

void ManageServiceImpl::FreezePartition(google::protobuf::RpcController* controller,
                                        const FreezePartitionRequest* request,
                                        AckResponse* response, google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);

    auto clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    AUDIT_LOG_ANCHOR(WARNING);

    const uint64_t id = request->partition_id();
    PartitionPtr partition;
    status = metabase_->LocatePartition(id, &partition);
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }

    PartitionState state = partition->GetState();
    if (state != PartitionState::P_CREATING && state != PartitionState::P_LOADING &&
        state != PartitionState::P_NORMAL) {
        *response->mutable_status() =
            Status::FailedPrecondition("partition state wrong").ToRpcStatus();
        return;
    }
    Table* table = partition->GetPartitionSet()->GetTable();
    CHECK(table);
    if (!table->CanFreezePartitionSafely(id) && !request->force()) {
        LOG_INFO("can not be frozen safely").put("cause_id", id);
        *response->mutable_status() =
            Status::FailedPrecondition("can not be frozen safely").ToRpcStatus();
        return;
    }

    status = raft_server_->Propose(cntl->log_id(), MS_LOG_PARTITION_FREEZE, &clone_req);
    *response->mutable_status() = status.ToRpcStatus();
}

void ManageServiceImpl::DropPartition(google::protobuf::RpcController* controller,
                                      const DropPartitionRequest* request, AckResponse* response,
                                      google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);

    auto clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    AUDIT_LOG_ANCHOR(WARNING);

    const uint64_t id = request->partition_id();
    PartitionPtr partition;
    status = metabase_->LocatePartition(id, &partition);
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }

    PartitionState state = partition->GetState();
    if (state != PartitionState::P_FROZEN) {
        *response->mutable_status() =
            Status::FailedPrecondition("partition state wrong").ToRpcStatus();
        return;
    }
    status = raft_server_->Propose(cntl->log_id(), MS_LOG_PARTITION_DROP, &clone_req);
    *response->mutable_status() = status.ToRpcStatus();
}

void ManageServiceImpl::FinishLoadPartition(google::protobuf::RpcController* controller,
                                            const LoadPartitionFinishRequest* request,
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
    AUDIT_LOG_ANCHOR(WARNING);

    const uint64_t id = request->partition_id();
    PartitionPtr partition;
    status = metabase_->LocatePartition(id, &partition);
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }

    PartitionState state = partition->GetState();
    if (state != PartitionState::P_LOADING) {
        Status load_result = Status::FromRpcStatus(request->load_result());
        if (state == PartitionState::P_NORMAL && load_result.ok()) {
            LOG_INFO("partition load finish is already committed, treat as idempotent success")
                .put("partition_id", id);
            response->mutable_status()->set_code(kOK);
            return;
        }
        *response->mutable_status() =
            Status::FailedPrecondition("partition state wrong").ToRpcStatus();
        return;
    }
    status = raft_server_->Propose(cntl->log_id(), MS_LOG_PARTITION_LOAD_FINISH, &clone_req);
    *response->mutable_status() = status.ToRpcStatus();
}

void ManageServiceImpl::PutProxyGroup(google::protobuf::RpcController* controller,
                                      const PutProxyGroupRequest* request, AckResponse* response,
                                      google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);

    auto clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    AUDIT_LOG_ANCHOR(WARNING);

    const ProxyGroupInfo& info = request->info();
    status = Validate(info);
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }

    NamespacePtr ns;
    status = metabase_->GetNamespaceManager()->Get(info.namespace_name(), &ns);
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    ProxyClusterPtr cluster = ns->GetProxyCluster();
    ConsulMap* consul_map = metabase_->GetNamespaceManager()->GetConsulMap();
    for (const std::string& name : info.config().consul_names()) {
        status = consul_map->Validate(cluster, name);
        if (!status.ok()) {
            *response->mutable_status() = status.ToRpcStatus();
            return;
        }
    }

    status = raft_server_->Propose(cntl->log_id(), MS_LOG_PROXY_GROUP_PUT, &clone_req);
    *response->mutable_status() = status.ToRpcStatus();
}

void ManageServiceImpl::DropProxyGroup(google::protobuf::RpcController* controller,
                                       const DropProxyGroupRequest* request, AckResponse* response,
                                       google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);

    auto clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    AUDIT_LOG_ANCHOR(WARNING);

    NamespacePtr ns;
    status = metabase_->GetNamespaceManager()->Get(request->namespace_name(), &ns);
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    const Location& loc = request->placement();
    ProxyGroupPtr group;
    status = ns->GetProxyCluster()->GetProxyGroup(loc, &group);
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }

    status = raft_server_->Propose(cntl->log_id(), MS_LOG_PROXY_GROUP_DROP, &clone_req);
    *response->mutable_status() = status.ToRpcStatus();
}


void ManageServiceImpl::MuteMetaChange(google::protobuf::RpcController* controller,
                                         const EmptyRequest* request, AckResponse* response,
                                       google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);

    auto clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    AUDIT_LOG_ANCHOR(WARNING);
    status = raft_server_->Propose(cntl->log_id(), MS_MUTE_META_CHANGE, &clone_req);
    *response->mutable_status() = status.ToRpcStatus();
}

void ManageServiceImpl::ResumeMetaChange(google::protobuf::RpcController* controller,
                                       const EmptyRequest* request, AckResponse* response,
                                       google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    brpc::ClosureGuard done_guard(done);

    auto clone_req = *request;
    Status status = SanitizeRequest(cntl, clone_req.mutable_id());
    if (!status.ok()) {
        *response->mutable_status() = status.ToRpcStatus();
        return;
    }
    AUDIT_LOG_ANCHOR(WARNING);
    status = raft_server_->Propose(cntl->log_id(), MS_RESUME_META_CHANGE, &clone_req);
    *response->mutable_status() = status.ToRpcStatus();
}

Status ManageServiceImpl::SanitizeRequest(brpc::Controller* cntl, RequestId* id) {
    if (!raft_server_->IsLeaderReady()) {
        return Status::FailedPrecondition("not leader or not ready");
    }
    if (id->operator_name().empty()) {
        return Status::FailedPrecondition("operator is required");
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

#undef AUDIT_LOG_ANCHOR

}  // namespace metaserver
}  // namespace bcache2
