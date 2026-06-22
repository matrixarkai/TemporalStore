// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/metaserver.h"

#include <string>
#include <vector>

#include "absl/strings/str_split.h"
#include "spdlog/fmt/fmt.h"

#include "common/logging.h"
#include "common/macros.h"
#include "metaserver_v2/balance/balance_routine.h"
#include "metaserver_v2/event_harbor.h"
#include "metaserver_v2/ha/convict_routine.h"
#include "metaserver_v2/ha/meta_check_routine.h"
#include "metaserver_v2/ha/proxy_calibrate_routine.h"
#include "metaserver_v2/meta/metabase.h"
#include "metaserver_v2/meta_publisher.h"
#include "metaserver_v2/raft_server.h"
#include "metaserver_v2/scheduler/scheduler_manager.h"
#include "metaserver_v2/service/heartbeat_service.h"
#include "metaserver_v2/service/manage_service.h"
#include "metaserver_v2/service/meta_query_service.h"
#include "metaserver_v2/service/query_service.h"
#include "metaserver_v2/service/raft_control_service.h"
#include "metaserver_v2/trivial_routine.h"

namespace bcache2 {
namespace metaserver {

MetaServer::MetaServer() {}
MetaServer::~MetaServer() { Stop(); }

Status MetaServer::Init() {
    stage_ = Stage::Initial;

    Status status;
    LOG_INFO("init metabase");
    metabase_.reset(new Metabase());
    status = metabase_->Init();
    if (!status.ok()) {
        return status;
    }

    LOG_INFO("init schedulers");
    scheduler_manager_.reset(new SchedulerManager());

    LOG_INFO("init convict routine");
    convict_routine_.reset(new ConvictRoutine(metabase_.get()));

    LOG_INFO("init proxy calibrate routine");
    proxy_calibrate_routine_.reset(new ProxyCalibrateRoutine());

    LOG_INFO("init meta check routine");
    meta_check_routine_.reset(new MetaCheckRoutine(metabase_.get()));

    LOG_INFO("init balance routine");
    balance_routine_.reset(new BalanceRoutine());

    LOG_INFO("init event harbor");
    event_harbor_.reset(new EventHarbor());

    LOG_INFO("init meta puber");
    meta_puber_.reset(new MetaPublisher());

    LOG_INFO("init raft server");
    status = InitRaftServer();
    if (!status.ok()) {
        return status;
    }

    LOG_INFO("init rpc server");
    status = InitRpcServer();
    if (!status.ok()) {
        return status;
    }

    LOG_INFO("init trivial routine");
    trivial_routine_.reset(new TrivialRoutine(raft_server_.get()));

    stage_ = Stage::Standby;
    return Status::OK();
}

Status MetaServer::Start() {
    if (stage_ != Stage::Standby) {
        return Status::Internal("Init() is not called or Init() was failed");
    }

    LOG_INFO("start event harbor");
    event_harbor_->Start();

    LOG_INFO("start raft server");
    Status status = raft_server_->Start();
    if (!status.ok()) {
        return status;
    }

    const uint32_t port = FLAGS_metaserver_server_port;
    LOG_INFO("start rpc server").put("port", port);
    brpc::ServerOptions options;
    if (rpc_server_->Start(port, &options) != 0) {
        return Status::Internal("failed to start server");
    }
    LOG_INFO("start meta query server").put("port", port - 1000);
    brpc::ServerOptions options2;
    if (meta_query_server_->Start(port - 1000, &options2) != 0) {
        rpc_server_->Stop(0);
        rpc_server_->Join();
        raft_server_->Stop();
        event_harbor_->Stop();
        return Status::Internal("failed to start meta query server");
    }

    LOG_INFO("start trivial routine");
    trivial_routine_->Start();

    stage_ = Stage::Running;
    return Status::OK();
}

void MetaServer::Stop() {
    if (stage_ != Stage::Running) {
        return;
    }
    stage_ = Stage::Stopping;

    LOG_INFO("stop raft server");
    raft_server_->Stop();

    LOG_INFO("stop rpc server");
    rpc_server_->Stop(0);
    rpc_server_->Join();
    meta_query_server_->Stop(0);
    meta_query_server_->Join();

    LOG_INFO("stop event harbor");
    event_harbor_->Stop();

    LOG_INFO("stop balance routine");
    balance_routine_->Stop();

    LOG_INFO("stop meta check routine");
    meta_check_routine_->Stop();

    LOG_INFO("stop convict routine");
    convict_routine_->Stop();

    LOG_INFO("stop proxy calibrate routine");
    proxy_calibrate_routine_->Stop();

    LOG_INFO("trivial routine");
    trivial_routine_->Stop();

    LOG_INFO("stop scheduler");
    scheduler_manager_->Stop();
}

Status MetaServer::InitRaftServer() {
    RaftServer::Options rs_opts;
    rs_opts.metabase = metabase_.get();
    rs_opts.convict_routine = convict_routine_.get();
    rs_opts.proxy_calibrate_routine = proxy_calibrate_routine_.get();
    rs_opts.meta_check_routine = meta_check_routine_.get();
    rs_opts.balance_routine = balance_routine_.get();
    rs_opts.scheduler_manager = scheduler_manager_.get();
    rs_opts.event_harbor = event_harbor_.get();
    rs_opts.meta_puber = meta_puber_.get();

    byteraft::Options& br_opts = rs_opts.byteraft;
    br_opts.enable_pre_vote = true;
    br_opts.peer_id = FLAGS_metaserver_raft_id;
    br_opts.wal_dir = fmt::format("{}/raft_wal/", FLAGS_metaserver_work_dir);
    br_opts.snapshot_dir = fmt::format("{}/raft_snapshot/", FLAGS_metaserver_work_dir);
    br_opts.election_cycle_tick =
        FLAGS_metaserver_raft_election_cycle_ms / FLAGS_metaserver_raft_heartbeat_cycle_ms;
    br_opts.wal_sync = FLAGS_metaserver_raft_wal_sync;
    br_opts.max_segment_bytes = FLAGS_metaserver_raft_segment_size;
    br_opts.max_flush_batch_bytes = FLAGS_metaserver_raft_max_sync_log_size;
    br_opts.max_apply_batch_bytes = FLAGS_metaserver_raft_max_apply_log_size;
    br_opts.max_inflights_apply_task = FLAGS_metaserver_raft_max_inflight_apply_task;
    br_opts.max_cache_memory_bytes = FLAGS_metaserver_raft_max_log_buffer_size;
    br_opts.reorder_window_size = FLAGS_metaserver_raft_reorder_queue_size;
    br_opts.enable_reorder_queue = FLAGS_metaserver_raft_enable_reorder_queue;
    br_opts.reorder_timeout_us = FLAGS_metaserver_raft_reorder_cache_us;
    br_opts.max_applied_log_bytes = FLAGS_metaserver_raft_max_applied_log_bytes;
    std::string peers_str = FLAGS_metaserver_raft_peers;
    std::vector<std::string> peers;
    butil::StringSplitter sp(peers_str.c_str(), ',', butil::SKIP_EMPTY_FIELD);
    for (; sp; sp++) {
        peers.push_back(std::string(sp.field(), sp.length()));
    }

    if (peers.empty() || peers.size() % 4 != 0) {
        LOG_ERROR("invalid raft peers, format: [node id],[raft addr],[snapshot addr],[role]")
            .put("got", FLAGS_metaserver_raft_peers);
        return Status::Internal("invalid peer format");
    }

    auto parse_role = [](int role) -> auto {
        if (role == 0) {
            return byteraft::RoleState::State::kNormal;
        } else if (role == 1) {
            return byteraft::RoleState::State::kLearner;
        } else {
            throw std::invalid_argument("invalid role type");
        }
    };

    for (size_t i = 0; i < peers.size(); i += 4) {
        // peer format: [node id],[raft addr],[snapshot addr],[role]
        // e.g.
        // 1,10.132.152.94:8130,10.132.152.94:8330,0,2,10.202.83.201:8130,10.202.83.201:8330,0,3,10.202.83.201:8130,10.202.83.201:8330,1
        const std::string& node_id = peers[i];
        const std::string& raft_addr = peers[i + 1];
        const std::string& snapshot_addr = peers[i + 2];
        const std::string& role = peers[i + 3];

        byteraft::NodeId node;
        try {
            node.peer_id = std::stoull(node_id);
            node.raft_addr = raft_addr;
            node.snapshot_addr = snapshot_addr;
            node.role_state.state = parse_role(std::stoi(role));
            br_opts.peers.emplace_back(node);
        } catch (const std::exception& e) {
            LOG_ERROR("invalid raft peers, format: [node id],[raft addr],[snapshot addr],[role]")
                .put("e", e.what());
            return Status::Internal("invalid peer format");
        }

        if (node.peer_id == FLAGS_metaserver_raft_id) {
            // peer is myself
            br_opts.role_state.state = node.role_state.state;
            br_opts.raft_addr = node.raft_addr;
            br_opts.snapshot_addr = node.snapshot_addr;
        }
    }  // for tokens

    if (br_opts.raft_addr.empty()) {
        return Status::Internal("i'm not in raft peers");
    }

    raft_server_.reset(new RaftServer());
    return raft_server_->Init(rs_opts);
}

Status MetaServer::InitRpcServer() {
    rpc_server_.reset(new brpc::Server());
    meta_query_server_.reset(new brpc::Server());

    LOG_INFO("init manage service");
    manage_service_.reset(new ManageServiceImpl(raft_server_.get(), metabase_.get()));
    if (rpc_server_->AddService(manage_service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        return Status::Internal("failed to add manager service");
    }

    LOG_INFO("init query service");
    query_service_.reset(new QueryServiceImpl(raft_server_.get(), metabase_.get()));
    if (rpc_server_->AddService(query_service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        return Status::Internal("failed to add query service");
    }

    LOG_INFO("init heartbeat service");
    heartbeat_service_.reset(
        new HeartbeatServiceImpl(raft_server_.get(), metabase_.get(), event_harbor_.get()));
    if (rpc_server_->AddService(heartbeat_service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        return Status::Internal("failed to add heartbeat service");
    }

    LOG_INFO("init meta query service");
    meta_query_service_.reset(new MetaQueryServiceImpl(raft_server_.get(), meta_puber_.get()));
    if (rpc_server_->AddService(meta_query_service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        return Status::Internal("failed to add meta query service");
    }
    rpc_server_->MaxConcurrencyOf(meta_query_service_.get(), "GetTableTopo") =
        FLAGS_metaserver_meta_query_max_concurrency;
    if (meta_query_server_->AddService(meta_query_service_.get(),
                                       brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        return Status::Internal("failed to add meta query service");
    }
    meta_query_server_->MaxConcurrencyOf(meta_query_service_.get(), "GetTableTopo") =
        FLAGS_metaserver_meta_query_max_concurrency;

    LOG_INFO("init raft cntl service");
    raft_cntl_service_.reset(new RaftControlServiceImpl(raft_server_.get()));
    if (rpc_server_->AddService(raft_cntl_service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        return Status::Internal("failed to add raft cntl service");
    }

    return Status::OK();
}

}  // namespace metaserver
}  // namespace bcache2
