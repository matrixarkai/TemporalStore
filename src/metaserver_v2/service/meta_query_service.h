// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <string>

#include "common/status.h"
#include "metaserver_v2/meta_publisher.h"
#include "metaserver_v2/raft_server.h"
#include "protocol/master.pb.h"
#include "protocol/metaserver.pb.h"

namespace bcache2 {
namespace metaserver {

class MetaQueryServiceImpl : public MasterService {
 public:
    MetaQueryServiceImpl(RaftServer* s, MetaPublisher* puber) : raft_server_(s), puber_(puber) {}
    ~MetaQueryServiceImpl() = default;

    void GetTableTopo(google::protobuf::RpcController* ctrl, const GetTableTopoRequest* request,
                      GetTableTopoResponse* response, google::protobuf::Closure* done) override;

#define RPC_DEPRECATED                                      \
    {                                                       \
        brpc::ClosureGuard done_guard(done);                \
        response->mutable_status()->set_code(kUnavailable); \
    }
    void CreateTable(google::protobuf::RpcController* ctrl, const CreateTableRequest* request,
                     CreateTableResponse* response,
                     google::protobuf::Closure* done) override RPC_DEPRECATED;

    void DeleteTable(google::protobuf::RpcController* ctrl, const DeleteTableRequest* request,
                     DeleteTableResponse* response,
                     google::protobuf::Closure* done) override RPC_DEPRECATED;

    void UpdateTable(google::protobuf::RpcController* ctrl, const UpdateTableRequest* request,
                     UpdateTableResponse* response,
                     google::protobuf::Closure* done) override RPC_DEPRECATED;

    void OpenTable(google::protobuf::RpcController* ctrl, const OpenTableRequest* request,
                   OpenTableResponse* response,
                   google::protobuf::Closure* done) override RPC_DEPRECATED;

    void CloseTable(google::protobuf::RpcController* ctrl, const CloseTableRequest* request,
                    CloseTableResponse* response,
                    google::protobuf::Closure* done) override RPC_DEPRECATED;

    void RegisterServer(google::protobuf::RpcController* ctrl, const RegisterServerRequest* request,
                        RegisterServerResponse* response,
                        google::protobuf::Closure* done) override RPC_DEPRECATED;

    void UnRegisterServer(google::protobuf::RpcController* ctrl,
                          const UnRegisterServerRequest* request,
                          UnRegisterServerResponse* response,
                          google::protobuf::Closure* done) override RPC_DEPRECATED;
#undef RPC_DEPRECATED

 private:
    RaftServer* const raft_server_{nullptr};  // NOT OWNED
    MetaPublisher* const puber_{nullptr};     // NOT OWNED
};

}  // namespace metaserver
}  // namespace bcache2

