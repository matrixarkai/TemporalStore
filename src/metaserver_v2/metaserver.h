// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>

#include "brpc/server.h"

#include "common/status.h"

namespace bcache2 {
namespace metaserver {

class Metabase;
class EventHarbor;
class SchedulerManager;
class RaftServer;
class ConvictRoutine;
class ProxyCalibrateRoutine;
class MetaCheckRoutine;
class BalanceRoutine;
class MetaPublisher;
class TrivialRoutine;

class ManageServiceImpl;
class QueryServiceImpl;
class HeartbeatServiceImpl;
class MetaQueryServiceImpl;
class RaftControlServiceImpl;

class MetaServer {
 public:
    MetaServer();
    ~MetaServer();

    Status Init();
    Status Start();
    void Stop();

 private:
    Status InitRaftServer();
    Status InitRpcServer();

 private:
    enum class Stage { Initial, Standby, Running, Stopping };

 private:
    Stage stage_{Stage::Initial};

    std::unique_ptr<Metabase> metabase_;
    std::unique_ptr<EventHarbor> event_harbor_;
    std::unique_ptr<SchedulerManager> scheduler_manager_;
    std::unique_ptr<RaftServer> raft_server_;
    std::unique_ptr<ConvictRoutine> convict_routine_;
    std::unique_ptr<ProxyCalibrateRoutine> proxy_calibrate_routine_;
    std::unique_ptr<MetaCheckRoutine> meta_check_routine_;
    std::unique_ptr<BalanceRoutine> balance_routine_;
    std::unique_ptr<MetaPublisher> meta_puber_;

    std::unique_ptr<brpc::Server> rpc_server_;
    std::unique_ptr<brpc::Server> meta_query_server_;  // for client compatibility
    std::unique_ptr<ManageServiceImpl> manage_service_;
    std::unique_ptr<QueryServiceImpl> query_service_;
    std::unique_ptr<HeartbeatServiceImpl> heartbeat_service_;
    std::unique_ptr<MetaQueryServiceImpl> meta_query_service_;
    std::unique_ptr<RaftControlServiceImpl> raft_cntl_service_;

    std::unique_ptr<TrivialRoutine> trivial_routine_;
};

}  // namespace metaserver
}  // namespace bcache2
