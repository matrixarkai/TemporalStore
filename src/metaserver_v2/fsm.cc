// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/fsm.h"

#include <utility>
#include <vector>

#include "butil/iobuf.h"
#include "byte/include/macros.h"

#include "common/logging.h"
#include "common/partition_id_type.h"
#include "common/time_tracer.h"
#include "metaserver_v2/event_harbor.h"
#include "metaserver_v2/flags.h"
#include "metaserver_v2/metrics.h"
#include "metaserver_v2/raft_server.h"
#include "metaserver_v2/scheduler/update_membership_task.h"

namespace bcache2 {
namespace metaserver {

static void FlushLogAndExit() {
    LOG_WARNING("flushing log and exit prog");
    LOG_FLUSH();
    std::exit(0);
}

static void FlushLogAndTerminate() {
    LOG_WARNING("flushing log and terminate prog");
    LOG_FLUSH();
    std::terminate();
}

StateMachine::~StateMachine() { Close(); }

Status StateMachine::Init(const Options& opts) {
    if (!opts.server) {
        return Status::Internal("missing server");
    }
    server_ = opts.server;

    if (!opts.metabase) {
        return Status::Internal("missing metabase");
    }
    metabase_ = opts.metabase;

    if (!opts.scheduler_manager) {
        return Status::Internal("missing schedule manager");
    }
    scheduler_manager_ = opts.scheduler_manager;
    convict_routine_ = opts.convict_routine;
    proxy_calibrate_routine_ = opts.proxy_calibrate_routine;
    meta_check_routine_ = opts.meta_check_routine;
    balance_routine_ = opts.balance_routine;
    event_harbor_ = opts.event_harbor;
    meta_puber_ = opts.meta_puber;

    leader_term_ = 0;
    is_leader_booting_ = false;
    is_leader_ready_ = false;

    return Status::OK();
}

byte::Status StateMachine::Open() {
    if (running_) {
        return byte::Status::OK();
    }
    LOG_INFO("open").put("peer", peer_id_);
    running_ = true;
    return byte::Status::OK();
}

byte::Status StateMachine::Close() {
    LOG_INFO("close").put("peer", peer_id_);
    if (!running_) {
        return byte::Status::OK();
    }
    running_ = false;
    if (is_leader_booting_) {
        election_post_task_.wait();
    }
    return byte::Status::OK();
}

byte::Status StateMachine::Apply(uint64_t index, const std::string& data) {
    RaftConnector::ParseResult parse_result = connector_->ParseLogData(data);
    if (!parse_result.result.ok()) {
        LOG_WARNING("parse failed, unknown log type").put("status", parse_result.result);
        return byte::Status::OK();
    }

    AcquireLock();
    BYTE_DEFER({
        metabase_->SetRaftIndex(index);
        ReleaseLock();
    });

    if (parse_result.meta.type() == MS_MUTE_META_CHANGE) {
        mute_meta_change_ = true;
        if (connector_) {
            connector_->SetContextStatus(parse_result.meta, Status::OK());
        }
        LOG_INFO("Stop meta change");
        return byte::Status::OK();
    }

    if (parse_result.meta.type() == MS_RESUME_META_CHANGE) {
        mute_meta_change_ = false;
        if (connector_) {
            connector_->SetContextStatus(parse_result.meta, Status::OK());
        }
        LOG_INFO("Resume meta change");
        return byte::Status::OK();
    }

    if (mute_meta_change_) {
        if (connector_) {
            connector_->SetContextStatus(parse_result.meta, Status::NoAction("meta change muted"));
        }
        LOG_INFO("Meta change has been muted, apply failed");
        return byte::Status::OK();
    }

    Status status;
    const google::protobuf::Message* message = parse_result.message.get();
    switch (parse_result.meta.type()) {
    case MS_LOG_SERVER_ADD:
        status = HandleServerAdd(static_cast<const AddServerRequest*>(message));
        break;

    case MS_LOG_SERVER_FREEZE:
        status = HandleServerFreeze(static_cast<const FreezeServerRequest*>(message));
        break;

    case MS_LOG_SERVER_DROP:
        status = HandleServerDrop(static_cast<const DropServerRequest*>(message));
        break;

    case MS_LOG_SERVER_UPDATE:
        status = HandleServerUpdate(static_cast<const UpdateServerRequest*>(message));
        break;

    case MS_LOG_NS_ADD:
        status = HandleNamespaceAdd(static_cast<const AddNamespaceRequest*>(message));
        break;

    case MS_LOG_MANAGE_INFO_UPDATE:
        status = HandleManageInfoUpdate(static_cast<const UpdateManageInfoRequest*>(message));
        break;

    case MS_LOG_TABLE_ADD:
        status = HandleTableAdd(static_cast<const AddTableRequest*>(message));
        break;

    case MS_LOG_TABLE_UPDATE:
        status = HandleTableUpdate(static_cast<const UpdateTableRequestV2*>(message));
        break;

    case MS_LOG_TABLE_FREEZE:
        status = HandleTableFreeze(static_cast<const FreezeTableRequest*>(message));
        break;

    case MS_LOG_TABLE_DROP:
        status = HandleTableDrop(static_cast<const DropTableRequest*>(message));
        break;

    case MS_LOG_PARTITION_CREATE_FINISH:
        status =
            HandlePartitionCreateFinish(static_cast<const CreatePartitionFinishRequest*>(message));
        break;

    case MS_LOG_PARTITION_LOAD_FINISH:
        status = HandlePartitionLoadFinish(static_cast<const LoadPartitionFinishRequest*>(message));
        break;

    case MS_LOG_MEMBERSHIP_UPDATE_FINISH:
        status = HandleMembershipUpdateFinish(
            static_cast<const UpdateMembershipFinishRequest*>(message));
        break;

    case MS_LOG_PARTITION_FREEZE:
        status = HandlePartitionFreeze(static_cast<const FreezePartitionRequest*>(message));
        break;

    case MS_LOG_PARTITION_DROP:
        status = HandlePartitionDrop(static_cast<const DropPartitionRequest*>(message));
        break;

    case MS_LOG_PROXY_GROUP_PUT:
        status = HandleProxyGroupPut(static_cast<const PutProxyGroupRequest*>(message));
        break;

    case MS_LOG_PROXY_GROUP_DROP:
        status = HandleProxyGroupDrop(static_cast<const DropProxyGroupRequest*>(message));
        break;

    case MS_LOG_PROXY_ADD:
        status = HandleProxyAdd(static_cast<const AddProxyRequest*>(message));
        break;

    case MS_LOG_PROXY_DROP:
        status = HandleProxyDrop(static_cast<const DropProxyRequest*>(message));
        break;

    case MS_LOG_PROXY_FREEZE:
        status = HandleProxyFreeze(static_cast<const FreezeProxyRequest*>(message));
        break;

    case MS_LOG_PROXY_ATTACH:
        status = HandleProxyAttach(static_cast<const AttachProxyRequest*>(message));
        break;

    case MS_LOG_PROXY_DETACH:
        status = HandleProxyDetach(static_cast<const DetachProxyRequest*>(message));
        break;

    default:
        LOG_WARNING("unknown log type").put("meta", parse_result.meta.ShortDebugString());
        status = Status::Internal("unknown type");
        break;
    }

    applied_index_ = index;
    if (connector_) {
        connector_->SetContextStatus(parse_result.meta, status);
    }

    if (!status.ok()                       //
        && !status.IsFailedPrecondition()  //
        && !status.IsAlreadyExists()       //
        && !status.IsNotFound()) {
        MS_METRIC(fsm_apply_fail_count)->Increment();
        if (FLAGS_metaserver_crash_on_fsm_failure) {
            CHECK(false) << status;
            *reinterpret_cast<char*>(-1) = 'c';  // to avoid CHECK skip
        }
    }

    LOG_INFO("apply data")
        .put("idx", index)
        .put("meta", parse_result.meta.ShortDebugString())
        .put("status", status);
    return byte::Status::OK();
}

byte::Status StateMachine::OnStartFollowing(uint64_t cur_leader_term,
                                            const uint64_t& cur_leader_id) {
    return byte::Status::OK();
}

byte::Status StateMachine::OnStopFollowing(uint64_t cur_leader_term,
                                           const uint64_t& cur_leader_id) {
    return byte::Status::OK();
}

constexpr size_t kMaxSnapshotIndexLen = 3;
uint64_t StateMachine::FlushedIndex() {
    std::lock_guard<bthread::Mutex> _(snapshot_index_list_lock_);
    if (snapshot_index_list_.size() < kMaxSnapshotIndexLen) {
        return 0;
    }
    return snapshot_index_list_.front();
}

void StateMachine::UpdateSnapshotIndexList(uint64_t index) {
    std::lock_guard<bthread::Mutex> _(snapshot_index_list_lock_);
    if (snapshot_index_list_.empty()) {
        snapshot_index_list_.push_back(index);
    } else {
        const uint64_t last = snapshot_index_list_.back();
        if (index > last) {
            snapshot_index_list_.push_back(index);
            if (snapshot_index_list_.size() > kMaxSnapshotIndexLen) {
                snapshot_index_list_.pop_front();
            }
        }
    }
}

byte::Status StateMachine::Checkpoint(const std::string& dir, uint64_t* applied_index) {
    TimeTracer tt;
    BYTE_DEFER({ LOG_INFO("time trace for dumping snapshot").put("v", tt.ToString()); });

    auto meta_snapshot = std::make_shared<Metabase>();
    meta_snapshot->Init();
    TopoAbstract topo_abst;

    AcquireLock();
    metabase_->DeepCopyTo(meta_snapshot.get());
    meta_puber_->TakeAbstract(&topo_abst);
    ReleaseLock();
    meta_snapshot->SetMuteMetaChange(mute_meta_change_);

    tt.AddEvent("copy");
    Status status = meta_snapshot->DumpSnapshot(dir);
    LOG_INFO("dump metabase snapshot")
        .put("applied_index", meta_snapshot->GetRaftIndex())
        .put("result", status)
        .put("mute_meta_change", std::to_string(mute_meta_change_).c_str());
    if (!status.ok()) {
        return byte::Status::Failed(status.ToString());
    }

    const std::string topo_abstract_path = dir + "/topo";
    status = meta_puber_->DumpAbstract(topo_abstract_path, topo_abst);
    tt.AddEvent("topo_abst");
    LOG_INFO("dump topo abstract").put("result", status);
    if (!status.ok()) {
        return byte::Status::Failed(status.ToString());
    }
    *applied_index = meta_snapshot->GetRaftIndex();
    UpdateSnapshotIndexList(*applied_index);
    return byte::Status::OK();
}

byte::Status StateMachine::OnSnapshotLoad(const std::string& dir) {
    TimeTracer tt;

    AcquireLock();
    BYTE_DEFER({
        ReleaseLock();
        LOG_INFO("time trace for load snapshot").put("v", tt.ToString());
    });

    Status status = metabase_->LoadSnapshot(dir);
    tt.AddEvent("load");
    LOG_INFO("load snapshot").put("result", status);
    if (!status.ok()) {
        return byte::Status::Failed(status.ToString());
    }

    LOG_INFO("start to fill topo");
    auto ns_mgr = metabase_->GetNamespaceManager();
    for (auto& ns : ns_mgr->List()) {
        for (auto& table : ns->List(false /* with frozen */)) {
            TableState state = table->GetState();
            CHECK(state != TableState::TABLE_FROZEN) << TableState_Name(state);
            LOG_INFO("start to fill topo").put("table", table->GetFullName());
            status = meta_puber_->AddTable(table->GetInfo());
            if (!status.ok()) {
                LOG_WARNING("failed to add table").put("result", status);
                CHECK(false) << this;
                return byte::Status::Failed(status.ToString());
            }
            for (auto& pset : table->GetAllPartitionSets()) {
                status = meta_puber_->UpdatePartitionSet(table->GetId(), pset->GetInfo());
                if (!status.ok()) {
                    LOG_WARNING("failed to update pset")
                        .put("result", status)
                        .put("pset_id", pset->GetId());
                    CHECK(false) << this;
                    return byte::Status::Failed(status.ToString());
                }
            }
        }
    }
    const std::string topo_abstract_path = dir + "/topo";
    status = meta_puber_->LoadAndApplyAbstract(topo_abstract_path);
    tt.AddEvent("topo_abst");
    LOG_INFO("load and apply topo abstract").put("result", status);
    if (!status.ok()) {
        return byte::Status::Failed(status.ToString());
    }
    const uint64_t idx = metabase_->GetRaftIndex();
    UpdateSnapshotIndexList(idx);
    mute_meta_change_ = metabase_->GetMuteMetaChange();
    LOG_INFO("set index_")
        .put("v", idx)
        .put("mute_meta_change", std::to_string(mute_meta_change_).c_str());
    return byte::Status::OK();
}

void StateMachine::OnConfigurationApplied(const std::vector<byteraft::NodeId>& old_config,
                                          const std::vector<byteraft::NodeId>& new_config) {
    //
}

#define TERMINATE_IF_ERROR(status, msg)                             \
    if (!(status).ok()) {                                           \
        LOG_WARNING(msg).put("term", term).put("status", (status)); \
        FlushLogAndTerminate();                                     \
    }

byte::Status StateMachine::OnLeaderStart(uint64_t term) {
    LOG_INFO("on leader start").put("term", term);
    leader_term_ = term;
    is_leader_booting_ = true;
    CHECK(!scheduler_manager_->Running()) << this;
    // async it to avoid blocking, ShuangQ byteraft!
    election_post_task_ = std::async(std::launch::async, [this, term] {
        LOG_INFO("waiting for all committed log applied to fsm").put("term", term);
        Status status = server_->WaitForLogApplied();
        TERMINATE_IF_ERROR(status, "failed to wait for all commited log applied");

        LOG_INFO("booting schedulers and repair broken tasks");
        status = scheduler_manager_->Start({
            .metabase = metabase_,
            .raft_connector = connector_,
        });
        TERMINATE_IF_ERROR(status, "failed to start scheduer manager");

        LOG_INFO("start to repair broken tasks");
        scheduler_manager_->RepairBrokenTasks();

        LOG_INFO("start convict routine");
        status = convict_routine_->Start({
            .raft_connector = connector_,
            .event_harbor = event_harbor_,
        });
        TERMINATE_IF_ERROR(status, "failed to start convict routine");

        LOG_INFO("start proxy calibrate routine");
        status = proxy_calibrate_routine_->Start({
            .metabase = metabase_,
            .raft_connector = connector_,
        });
        TERMINATE_IF_ERROR(status, "failed to start proxy calibrate routine");

        LOG_INFO("start meta check routine");
        status = meta_check_routine_->Start({
            .raft_connector = connector_,
            .event_harbor = event_harbor_,
        });
        TERMINATE_IF_ERROR(status, "failed to start meta check routine");

        LOG_INFO("start balance routine");
        status = balance_routine_->Start({
            .metabase = metabase_,
            .scheduler_manager = scheduler_manager_,
        });
        TERMINATE_IF_ERROR(status, "failed to start balance routine");

        is_leader_booting_ = false;
        is_leader_ready_ = true;
        LOG_INFO("leader booted").put("term", term);
    });

    return byte::Status::OK();
}
#undef TERMINATE_IF_ERROR

byte::Status StateMachine::OnLeaderStop(uint64_t term) {
    LOG_WARNING("on leader stop").put("term", term);
    FlushLogAndExit();
    // unreachable
    return byte::Status::OK();
}

void StateMachine::SetConnector(RaftConnector* connector) {
    BYTE_ASSERT(connector);
    connector_ = connector;
}

Status StateMachine::HandleServerAdd(const AddServerRequest* request) {
    ServerInfo info = Request2Info(*request);
    info.set_created_at(request->id().timestamp());
    LOG_INFO("add server").put("info", info.ShortDebugString());
    auto server = std::make_shared<Server>(std::move(info));  // this move makes no meaning
    server->SetState(ServerState::SERVER_NORMAL);
    Status status = metabase_->GetServerLocationManager()->Add(std::move(server));
    return status;
}

Status StateMachine::HandleServerFreeze(const FreezeServerRequest* request) {
    ServerPtr server = metabase_->GetServerLocationManager()->Get(request->server_id());
    if (!server) {
        LOG_WARNING("failed to find server").put("reqeuest", request->ShortDebugString());
        return Status::NotFound("");
    }
    if (server->GetState() != ServerState::SERVER_NORMAL) {
        return Status::FailedPrecondition("server state mismatch");
    }
    for (auto& node : server->GetNodes()) {
        for (uint64_t id : node->GetPartitionIds()) {
            PartitionPtr partition;
            Status status = metabase_->LocatePartition(id, &partition);
            if (status.IsNotFound()) {
                continue;
            }
            CHECK(status.ok()) << this << status;

            Table* table = partition->GetPartitionSet()->GetTable();
            CHECK(table);
            if (!table->CanFreezePartitionSafely(id) && !request->force()) {
                return Status::FailedPrecondition("partition can not be frozen");
            }
            PartitionState state = partition->GetState();
            if (state == PartitionState::P_CREATING || state == PartitionState::P_LOADING ||
                state == PartitionState::P_NORMAL) {
                LOG_INFO("start freeze partition").put("partition", *partition);
                FreezePartition(partition, request->id().timestamp());
            }
        }  // all pids of a node
    }
    LOG_INFO("freeze server")
        .put("endpoint", server->GetEndpoint())
        .put("reason", FreezeServerReason_Name(request->reason()));
    server->SetFrozenState(request->id().timestamp(), request->reason());
    return Status::OK();
}

Status StateMachine::HandleServerDrop(const DropServerRequest* request) {
    ServerPtr server = metabase_->GetServerLocationManager()->Get(request->server_id());
    if (!server) {
        LOG_WARNING("failed to find server").put("reqeuest", request->ShortDebugString());
        return Status::NotFound("");
    }
    if (server->GetState() != ServerState::SERVER_FROZEN) {
        return Status::FailedPrecondition("server state mismatch");
    }
    LOG_INFO("drop server").put("endpoint", server->GetEndpoint());
    metabase_->GetServerLocationManager()->Remove(server);
    event_harbor_->Publish(new ServerDropEvent(server->GetEndpoint()));
    return Status::OK();
}

Status StateMachine::HandleServerUpdate(const UpdateServerRequest* request) {
    ServerPtr server = metabase_->GetServerLocationManager()->Get(request->endpoint());
    if (!server) {
        LOG_WARNING("failed to find server").put("reqeuest", request->ShortDebugString());
        return Status::NotFound("");
    }
    if (server->GetState() != ServerState::SERVER_NORMAL) {
        return Status::FailedPrecondition("server state mismatch");
    }

    LOG_INFO("update server").put("endpoint", server->GetEndpoint());
    server->SetLocationTag(request->location_tag_name());
    const Location loc = server->GetLocation();
    for (auto& node : server->GetNodes()) {
        for (uint64_t id : node->GetPartitionIds()) {
            PartitionPtr partition;
            Status status = metabase_->LocatePartition(id, &partition);
            if (status.IsNotFound()) {
                continue;
            }
            CHECK(status.ok()) << this << status;
            PlacementSpec place = partition->GetPlacementActual();
            place.mutable_location()->set_tag(request->location_tag_name());
            CHECK(IsSame(place.location(), loc))
                << this << to_string(place.location()) << " " << to_string(loc);
            PartitionSet* pset = partition->GetPartitionSet();
            CHECK(pset) << this;
            pset->UpdatePlacementActual(partition, place);
        }  // all pids of a node
    }
    return Status::OK();
}

Status StateMachine::HandleNamespaceAdd(const AddNamespaceRequest* request) {
    NamespaceInfo info = Request2Info(*request);
    info.set_created_at(request->id().timestamp());
    LOG_INFO("add ns").put("info", info.ShortDebugString());
    auto ns = std::make_shared<Namespace>(std::move(info));
    return metabase_->GetNamespaceManager()->Add(std::move(ns));
}

Status StateMachine::HandleManageInfoUpdate(const UpdateManageInfoRequest* request) {
    // TODO(wuzhenyu) gracefully merge partition updates
    return metabase_->UpdateManageInfo(request->info());
}

Status StateMachine::HandleTableAdd(const AddTableRequest* request) {
    TableInfo info = Request2Info(*request);
    info.set_created_at(request->id().timestamp());
    TablePtr table = std::make_shared<Table>(info);
    Status status = metabase_->GetNamespaceManager()->AddTable(table);
    if (!status.ok()) {
        return status;
    }

    status = meta_puber_->AddTable(table->GetInfo());
    CHECK(status.ok()) << this << status;
    LOG_INFO("add table meta done").put("info", info.ShortDebugString());
    if (CanSubmitTask()) {
        LOG_INFO("create table").put("info", info.ShortDebugString());
        scheduler_manager_->CreateTable(table);
    }
    return Status::OK();
}

Status StateMachine::HandleTableUpdate(const UpdateTableRequestV2* request) {
    TablePtr table;
    Status status = metabase_->GetNamespaceManager()->GetTable(request->table_id(), &table);
    if (!status.ok()) {
        return status;
    }
    if (table->GetState() != TableState::TABLE_NORMAL && !request->force()) {
        return Status::FailedPrecondition("table state is not normal");
    }

    if (request->has_config()) {
        LOG_INFO("update table config").put("table_id", request->table_id());
        table->UpdateConfig(request->config());
    }

    if (request->has_update_partition_unit()) {
        LOG_INFO("update table unit").put("table_id", request->table_id());
        status = table->UpdatePartitionUnit(request->update_partition_unit());
        if (!status.ok()) {
            LOG_WARNING("failed to update table unit").put("result", status);
            return status;
        }

        if (CanSubmitTask()) {
            for (auto& pset : table->GetAllPartitionSets()) {
                for (auto& p : pset->GetPartitions(request->update_partition_unit().id())) {
                    PartitionState state = p->GetState();
                    if (state == PartitionState::P_FREEZING) {
                        LOG_INFO("submit to update membership for freezing").put("partition", *p);
                        SubmitUpdateMembershipTask(p);
                    } else if (state == PartitionState::P_CREATING) {
                        LOG_INFO("submit to update membership for creating").put("partition", *p);
                        scheduler_manager_->CreatePartition(p);
                    }
                }  // partition
            }      // pset
        }          // leader
    }
    return status;
}

Status StateMachine::HandleTableFreeze(const FreezeTableRequest* request) {
    TablePtr table;
    Status status = metabase_->GetNamespaceManager()->GetTable(request->table_id(), &table);
    if (!status.ok()) {
        return status;
    }
    TableState state = table->GetState();
    if (state == TableState::TABLE_FROZEN) {
        return Status::FailedPrecondition("table already frozen");
    } else if (state != TableState::TABLE_NORMAL && !request->force()) {
        return Status::FailedPrecondition("table state is not normal");
    }
    std::vector<PartitionSetPtr> psets = table->GetAllPartitionSets();
    for (auto& pset : psets) {
        // increase membership to inform all clients
        // here no partition would be frozen, freeze action is delayed to Drop period
        pset->IncrMembershipGlobalVersion();
    }
    Namespace* ns = table->GetNamespace();
    CHECK(ns != nullptr);
    LOG_INFO("freeze table").put("name", table->GetFullName()).put("id", table->GetId());
    status = ns->Freeze(table, request->id().timestamp());
    CHECK(status.ok());

    status = meta_puber_->DropTable(request->table_id());
    CHECK(status.ok()) << this << status;
    return Status::OK();
}

Status StateMachine::HandleTableDrop(const DropTableRequest* request) {
    TablePtr table;
    Status status = metabase_->GetNamespaceManager()->GetTable(request->table_id(), &table);
    if (!status.ok()) {
        return status;
    }
    if (table->GetState() != TableState::TABLE_FROZEN) {
        return Status::FailedPrecondition("table state is not frozen");
    }
    LOG_WARNING("start to drop table").put("name", table->GetFullName()).put("id", table->GetId());
    status = metabase_->GetNamespaceManager()->DropTable(request->table_id());
    CHECK(status.ok());
    return Status::OK();
}

Status StateMachine::HandlePartitionCreateFinish(const CreatePartitionFinishRequest* request) {
    partition_id_t pid(request->partition_id());
    PartitionPtr partition;
    Status status = metabase_->LocatePartition(pid.id, &partition);
    if (!status.ok()) {
        LOG_WARNING("failed to find partition").put("partition", pid);
        return status;
    }
    PartitionState state = partition->GetState();
    if (state != PartitionState::P_CREATING) {
        LOG_WARNING("partition state is not creating").put("partition", *partition);
        return Status::FailedPrecondition("partition is not in creating state");
    }

    numa_node_id_t node_id(request->node_id());
    ServerPtr server = metabase_->GetServerLocationManager()->Get(node_id.GetServerId());
    if (!server) {
        LOG_WARNING("failed to find server").put("node_id", node_id.id);
        FreezePartition(partition, request->id().timestamp());
        return Status::OK();
    }

    if (server->GetState() != ServerState::SERVER_NORMAL) {
        LOG_WARNING("server is not healthy, try to freeze this partition")
            .put("partition", *partition)
            .put("server", server->GetEndpoint().ShortDebugString());
        FreezePartition(partition, request->id().timestamp());
        return Status::OK();
    }

    NodePtr node;
    status = server->GetNode(node_id.id, &node);
    CHECK(status.ok());

    LOG_INFO("commit partition creation").put("partition_id", pid.id).put("node_id", node_id.id);
    node->CommitIntentPartition(pid.id);

    PlacementSpec place;
    *place.mutable_node() = node->GetInfo();
    *place.mutable_location() = server->GetLocation();
    *place.mutable_server() = server->GetEndpoint();
    partition->SetPlacementActual(std::move(place), node);
    partition->FinishCreating(request->id().timestamp());

    if (!request->async_load()) {
        LOG_INFO("finish loading stage directly").put("partition", *partition);
        FinishLoadingPartition(partition);
    }

    return Status::OK();
}

Status StateMachine::HandlePartitionLoadFinish(const LoadPartitionFinishRequest* request) {
    partition_id_t pid(request->partition_id());
    PartitionPtr partition;
    Status status = metabase_->LocatePartition(pid.id, &partition);
    if (!status.ok()) {
        LOG_WARNING("failed to find partition").put("partition", pid);
        return status;
    }
    PartitionState state = partition->GetState();
    if (state != PartitionState::P_LOADING) {
        LOG_WARNING("partition state is not loading").put("partition", *partition);
        return Status::FailedPrecondition("partition is not in creating state");
    }

    Status result = Status::FromRpcStatus(request->load_result());
    if (result.ok()) {
        LOG_INFO("load partition success").put("partition", *partition);
        FinishLoadingPartition(partition);
    } else {
        LOG_INFO("load partition failed, try to freeze it and create new one")
            .put("partition", *partition)
            .put("reported_result", result);
        FreezePartition(partition, request->id().timestamp());
    }
    return Status::OK();
}

Status StateMachine::HandleMembershipUpdateFinish(const UpdateMembershipFinishRequest* request) {
    partition_id_t pid(request->partition_id());
    PartitionPtr partition;
    Status status = metabase_->LocatePartition(pid.id, &partition);
    if (!status.ok()) {
        LOG_WARNING("failed to find partition").put("partition", pid);
        return status;
    }

    PartitionState state = partition->GetState();
    switch (state) {
    case PartitionState::P_CREATING:
    case PartitionState::P_LOADING:
        break;

    case PartitionState::P_FREEZING:
        CommitFreezePartition(partition, request->id().timestamp());
        break;

    default:
        break;
    }
    PartitionSet* pset = partition->GetPartitionSet();
    CHECK(pset) << this;
    Table* table = pset->GetTable();
    CHECK(table) << this;
    if (table->GetState() != TableState::TABLE_FROZEN) {
        status = meta_puber_->UpdatePartitionSet(table->GetId(), pset->GetInfo());
        CHECK(status.ok()) << this << status;
    }

    return Status::OK();
}

void StateMachine::FreezePartition(const PartitionPtr& partition, int64_t ts) {
    PartitionState state = partition->GetState();
    if (state == PartitionState::P_FREEZING || state == PartitionState::P_FROZEN) {
        return;
    }

    Status status;
    PartitionPtr derived;
    PartitionSet* pset = partition->GetPartitionSet();
    CHECK(pset) << this;
    Table* table = pset->GetTable();
    CHECK(table) << this;
    if (NeedRecoverPartition(partition)) {
        const PartitionRole role = partition->GetRole();
        LOG_WARNING("partition need recover")
            .put("partition", *partition)
            .put("role", PartitionRole_Name(role));

        status =
            pset->DerivePartition(partition, &derived, ts, PartitionBornObjective::PBO_RECOVER);
        if (!status.ok()) {
            LOG_WARNING("failed to derive partition")
                .put("partition", *partition)
                .put("result", status);
            CHECK(false) << this;
            return;
        }
        derived->Attach(pset);
        // table is the entrance of Add,
        // because table has a partition flat map...
        status = table->AddPartition(derived);
        CHECK(status.ok()) << this << status;
    }

    // new primary would be promoted if necessary
    status = pset->Freeze(partition, table->GetElectionPolicy());
    CHECK(status.ok()) << this << status;
    if (!CanSubmitTask()) {
        return;
    }

    SubmitUpdateMembershipTask(partition);
    if (derived) {
        LOG_INFO("submit create partition to scheduler").put("partition", derived);
        scheduler_manager_->CreatePartition(derived);
    }
}

void StateMachine::FinishLoadingPartition(const PartitionPtr& partition) {
    CHECK(partition->GetState() == PartitionState::P_LOADING);
    partition->FinishLoading();

    PartitionSet* pset = partition->GetPartitionSet();
    CHECK(pset) << this;
    pset->FinishCreatingPartition(partition);
    Table* table = pset->GetTable();
    CHECK(table) << this;
    if (pset->GetState() == PartitionSetState::PSET_NORMAL) {
        table->FinishCreatingPartitionSet(pset->GetId());
    }
    if (table->GetState() != TableState::TABLE_FROZEN) {
        Status status = meta_puber_->UpdatePartitionSet(table->GetId(), pset->GetInfo());
        CHECK(status.ok()) << this;
    }
}

void StateMachine::CommitFreezePartition(const PartitionPtr& partition, int64_t ts) {
    LOG_INFO("set partition to frozen").put("partition", *partition).put("ts", ts);
    partition->GetPartitionSet()->SetFrozen(partition, ts);
}

bool StateMachine::NeedRecoverPartition(const PartitionPtr& partition) {
    switch (partition->GetState()) {
    case PartitionState::P_FREEZING:
    case PartitionState::P_FROZEN:
        return false;

    case PartitionState::P_NORMAL:
        return true;

    case PartitionState::P_CREATING:
    case PartitionState::P_LOADING: {
        if (partition->GetBornObjective() != PartitionBornObjective::PBO_BALANCE) {
            return true;
        }
        // Note: below condition is unreachable currently, because balance now is implemented
        // by freeze -> recover workflow
        PartitionPtr parent = partition->GetInherited();
        // Scenario:
        //  1. Original: A(normal) -> A'(balancing)
        //  2. Pre-condition: Freeze A, got: null -> A'(balancing)
        //  3. Current condition: Freeze A', we need: null -> null -> A''
        if (parent == nullptr || parent->GetState() != PartitionState::P_NORMAL) {
            return true;
        }
        return false;
    }

    default:
        return false;
    }
}

Status StateMachine::SubmitUpdateMembershipTask(const PartitionPtr& partition) {
    CHECK(CanSubmitTask());

    LOG_INFO("submit update membership task to scheduler").put("partition", *partition);
    UpdateMembershipTask::Options opts;
    switch (partition->GetState()) {
    case PartitionState::P_FREEZING:
        // Since there is a fence mechanism between server and storage pool called bytestore
        // inline blob, we do not need a quorum threshold to ensure data reliability
        opts.success_threshold = 1;
        opts.submit_fsm = true;
        break;

    case PartitionState::P_CREATING:
    case PartitionState::P_LOADING:
        // TODO(wuzhenyu) think again
        opts.success_threshold = 0;
        break;

    default:
        break;
    }

    return scheduler_manager_->UpdateMembership(partition, std::move(opts));
}

Status StateMachine::HandlePartitionFreeze(const FreezePartitionRequest* request) {
    partition_id_t pid(request->partition_id());
    PartitionPtr partition;
    Status status = metabase_->LocatePartition(pid.id, &partition);
    if (!status.ok()) {
        return status;
    }
    PartitionState state = partition->GetState();
    if (state != PartitionState::P_CREATING && state != PartitionState::P_LOADING &&
        state != PartitionState::P_NORMAL) {
        return Status::FailedPrecondition("partition state not match");
    }
    Table* table = partition->GetPartitionSet()->GetTable();
    CHECK(table);
    if (!table->CanFreezePartitionSafely(pid.id) && !request->force()) {
        return Status::FailedPrecondition("partition can not be frozen");
    }
    LOG_INFO("start freeze partition").put("partition", *partition);
    FreezePartition(partition, request->id().timestamp());
    return Status::OK();
}

Status StateMachine::HandlePartitionDrop(const DropPartitionRequest* request) {
    partition_id_t pid(request->partition_id());
    PartitionPtr partition;
    Status status = metabase_->LocatePartition(pid.id, &partition);
    if (!status.ok()) {
        return status;
    }

    PartitionState state = partition->GetState();
    if (state != PartitionState::P_FROZEN) {
        return Status::FailedPrecondition("partition state not match");
    }
    LOG_INFO("start drop partition").put("partition", *partition);
    Table* table = partition->GetPartitionSet()->GetTable();
    return table->DropPartition(partition);
}

Status StateMachine::HandleProxyGroupPut(const PutProxyGroupRequest* request) {
    const ProxyGroupInfo& info = request->info();
    LOG_INFO("put proxy group").put("info", info.ShortDebugString());
    NamespacePtr ns;
    Status status = metabase_->GetNamespaceManager()->Get(info.namespace_name(), &ns);
    RETURN_IF_STATUS_ERROR(status);

    ProxyClusterPtr cluster = ns->GetProxyCluster();
    ConsulMap* consul_map = metabase_->GetNamespaceManager()->GetConsulMap();
    for (const std::string& name : info.config().consul_names()) {
        status = consul_map->Validate(cluster, name);
        RETURN_IF_STATUS_ERROR(status);
    }

    status = cluster->CreateOrUpdateProxyGroup(info);
    CHECK(status.ok()) << this << status;

    status = consul_map->Calibrate(cluster);
    CHECK(status.ok()) << this << status;

    return Status::OK();
}

Status StateMachine::HandleProxyGroupDrop(const DropProxyGroupRequest* request) {
    NamespacePtr ns;
    Status status = metabase_->GetNamespaceManager()->Get(request->namespace_name(), &ns);
    RETURN_IF_STATUS_ERROR(status);

    const Location& loc = request->placement();
    ProxyGroupPtr group;
    status = ns->GetProxyCluster()->GetProxyGroup(loc, &group);
    RETURN_IF_STATUS_ERROR(status);

    status = ns->GetProxyCluster()->DropProxyGroup(group);
    CHECK(status.ok()) << this << status;
    return Status::OK();
}

Status StateMachine::HandleProxyAdd(const AddProxyRequest* request) {
    ProxyInfo info = Request2Info(*request);
    info.set_created_at(request->id().timestamp());
    LOG_INFO("add proxy").put("info", info.ShortDebugString());
    auto proxy = std::make_shared<Proxy>(std::move(info));
    proxy->SetState(ProxyState::PROXY_IDLE);
    return metabase_->GetProxyLocationManager()->Add(std::move(proxy));
}

Status StateMachine::HandleProxyFreeze(const FreezeProxyRequest* request) {
    ProxyPtr proxy = metabase_->GetProxyLocationManager()->Get(request->proxy_id());
    if (!proxy) {
        LOG_WARNING("failed to find proxy").put("id", request->proxy_id());
        return Status::NotFound("proxy not found");
    }
    Status status = Status::OK();
    ProxyState state = proxy->GetState();
    switch (state) {
    case ProxyState::PROXY_FROZEN:
        status = Status::FailedPrecondition("already frozen");
        break;
    case ProxyState::PROXY_IDLE:
        proxy->SetState(ProxyState::PROXY_FROZEN);
        break;
    case ProxyState::PROXY_NORMAL: {
        ProxyGroup* group = proxy->GetProxyGroup();
        status = group->RemoveProxy(proxy);
        break;
    }

    default:
        CHECK(false) << this;
        status = Status::Internal("invalid state");
        break;
    }

    return status;
}

Status StateMachine::HandleProxyDrop(const DropProxyRequest* request) {
    ProxyPtr proxy = metabase_->GetProxyLocationManager()->Get(request->proxy_id());
    if (!proxy) {
        LOG_WARNING("failed to find proxy").put("id", request->proxy_id());
        return Status::NotFound("proxy not found");
    }
    Status status = Status::OK();
    ProxyState state = proxy->GetState();
    switch (state) {
    case ProxyState::PROXY_FROZEN:
    case ProxyState::PROXY_IDLE:
        break;
    case ProxyState::PROXY_NORMAL: {
        ProxyGroup* group = proxy->GetProxyGroup();
        status = group->RemoveProxy(proxy);
        break;
    }

    default:
        CHECK(false) << this;
        status = Status::Internal("invalid state");
        break;
    }
    RETURN_IF_STATUS_ERROR(status);

    proxy->SetState(ProxyState::PROXY_FROZEN);
    metabase_->GetProxyLocationManager()->Remove(proxy);
    event_harbor_->Publish(new ProxyDropEvent(proxy->GetEndpoint()));
    return Status::OK();
}

Status StateMachine::HandleProxyAttach(const AttachProxyRequest* request) {
    ProxyPtr proxy = metabase_->GetProxyLocationManager()->Get(request->proxy_id());
    if (!proxy) {
        LOG_WARNING("failed to find proxy").put("id", request->proxy_id());
        return Status::NotFound("proxy not found");
    }

    if (proxy->GetState() != ProxyState::PROXY_IDLE) {
        return Status::FailedPrecondition("proxy state mismatch");
    }

    NamespacePtr ns;
    Status status = metabase_->GetNamespaceManager()->Get(request->namespace_name(), &ns);
    RETURN_IF_STATUS_ERROR(status);

    const Location& loc = request->placement();
    ProxyGroupPtr group;
    status = ns->GetProxyCluster()->GetProxyGroup(loc, &group);
    RETURN_IF_STATUS_ERROR(status);
    return group->AddProxy(proxy);
}

Status StateMachine::HandleProxyDetach(const DetachProxyRequest* request) {
    ProxyPtr proxy = metabase_->GetProxyLocationManager()->Get(request->proxy_id());
    if (!proxy) {
        LOG_WARNING("failed to find proxy").put("id", request->proxy_id());
        return Status::NotFound("proxy not found");
    }

    if (proxy->GetState() != ProxyState::PROXY_NORMAL) {
        return Status::FailedPrecondition("proxy state mismatch");
    }
    ProxyGroup* group = proxy->GetProxyGroup();
    return group->RemoveProxy(proxy);
}

void StateMachine::AcquireLock() {
    lock_.lock();
    CHECK(!locked_);
    locked_ = true;
}

void StateMachine::ReleaseLock() {
    CHECK(locked_);
    locked_ = false;
    lock_.unlock();
}

}  // namespace metaserver
}  // namespace bcache2
