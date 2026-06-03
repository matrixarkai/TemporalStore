// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include "brpc/closure_guard.h"
#include "brpc/controller.h"

#include "common/status.h"
#include "protocol/metaserver.pb.h"

namespace bcache2 {
namespace metaserver {

class RaftServer;
class Metabase;
class EventHarbor;

class HeartbeatServiceImpl : public HeartbeatService {
 public:
    HeartbeatServiceImpl(RaftServer* s, Metabase* mb, EventHarbor* eh);

    void ServerHeartbeat(google::protobuf::RpcController* controller,
                         const ServerHeartbeatRequest* request, ServerHeartbeatResponse* response,
                         google::protobuf::Closure* done) override;

    void ServerNotifyStop(google::protobuf::RpcController* controller,
                          const ServerNotifyStopRequest* request, AckResponse* response,
                          google::protobuf::Closure* done) override;

    void ProxyHeartbeat(google::protobuf::RpcController* controller,
                        const ProxyHeartbeatRequest* request, ProxyHeartbeatResponse* response,
                        google::protobuf::Closure* done) override;

    void ProxyNotifyStop(google::protobuf::RpcController* controller,
                         const ProxyNotifyStopRequest* request, AckResponse* response,
                         google::protobuf::Closure* done) override;

 private:
    Status SanitizeRequest(brpc::Controller* cntl, RequestId* id);

 private:
    RaftServer* const raft_server_{nullptr};    // NOT OWNED
    Metabase* const metabase_{nullptr};         // NOT OWNED
    EventHarbor* const event_harbor_{nullptr};  // NOT OWNED
};

}  // namespace metaserver
}  // namespace bcache2

