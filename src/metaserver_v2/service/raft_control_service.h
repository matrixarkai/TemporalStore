// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include "common/status.h"
#include "metaserver_v2/raft_server.h"
#include "protocol/metaserver.pb.h"

namespace bcache2 {
namespace metaserver {

class RaftControlServiceImpl : public RaftControlService {
 public:
    explicit RaftControlServiceImpl(RaftServer* s) : raft_server_(s) {}

    void AddNode(google::protobuf::RpcController* controller, const AddRaftNodeRequest* request,
                 AckResponse* response, google::protobuf::Closure* done) override;

    void RemoveNode(google::protobuf::RpcController* controller,
                    const RemoveRaftNodeRequest* request, AckResponse* response,
                    google::protobuf::Closure* done) override;

    void ListMembership(google::protobuf::RpcController* controller, const EmptyRequest* request,
                        ListRaftMembershipResponse* response,
                        google::protobuf::Closure* done) override;

    void TriggerSnapshot(google::protobuf::RpcController* controller, const EmptyRequest* request,
                         AckResponse* response, google::protobuf::Closure* done) override;

 private:
    Status SanitizeRequest(const RequestId& id, bool strict_op_name = true);

 private:
    RaftServer* const raft_server_{nullptr};  // NOT OWNED
};

}  // namespace metaserver
}  // namespace bcache2

