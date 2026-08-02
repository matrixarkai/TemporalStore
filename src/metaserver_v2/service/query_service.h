// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <string>

#include "common/status.h"
#include "metaserver_v2/meta/metabase.h"
#include "metaserver_v2/raft_server.h"
#include "protocol/metaserver.pb.h"

namespace bcache2 {
namespace metaserver {

class QueryServiceImpl : public QueryService {
 public:
    explicit QueryServiceImpl(RaftServer* s, Metabase* mb) : raft_server_(s), metabase_(mb) {}

    void QueryLeader(google::protobuf::RpcController* controller, const QueryLeaderRequest* request,
                     QueryLeaderResponse* response, google::protobuf::Closure* done) override;

    void QueryManageInfo(google::protobuf::RpcController* controller, const EmptyRequest* request,
                         QueryManageInfoResponse* response,
                         google::protobuf::Closure* done) override;

    void QueryClusterStatus(google::protobuf::RpcController* controller,
                            const EmptyRequest* request, QueryClusterStatusResponse* response,
                            google::protobuf::Closure* done) override;

    void ListServer(google::protobuf::RpcController* controller, const ListServerRequest* request,
                    ListServerResponse* response, google::protobuf::Closure* done) override;

    void ListProxy(google::protobuf::RpcController* controller, const ListProxyRequest* request,
                   ListProxyResponse* response, google::protobuf::Closure* done) override;

    void ListProxyGroup(google::protobuf::RpcController* controller,
                        const ListProxyGroupRequest* request, ListProxyGroupResponse* response,
                        google::protobuf::Closure* done) override;

    void ListNamespace(google::protobuf::RpcController* controller,
                       const ListNamespaceRequest* request, ListNamespaceResponse* response,
                       google::protobuf::Closure* done) override;

    void ListTable(google::protobuf::RpcController* controller, const ListTableRequest* request,
                   ListTableResponse* response, google::protobuf::Closure* done) override;

    void ListPartition(google::protobuf::RpcController* controller,
                       const ListPartitionRequest* request, ListPartitionResponse* response,
                       google::protobuf::Closure* done) override;

    void ListServerPartition(google::protobuf::RpcController* controller,
                             const ListServerPartitionRequest* request,
                             ListServerPartitionResponse* response,
                             google::protobuf::Closure* done) override;

 private:
    RaftServer* const raft_server_{nullptr};  // NOT OWNED
    Metabase* const metabase_{nullptr};       // NOT OWNED
};

}  // namespace metaserver
}  // namespace bcache2
