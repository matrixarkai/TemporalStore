// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include "protocol/bench.pb.h"
#include "protocol/server.pb.h"

namespace bcache2 {
namespace bench {

class Bench;
class WorkloadsBunch;
class ConsistencyChecker;

class ServiceImpl : public BenchService {
 public:
    ServiceImpl(Bench* bench, WorkloadsBunch* workloads, ConsistencyChecker* checker)
        : bench_(bench), workloads_(workloads), checker_(checker) {}
    ~ServiceImpl() {}

    void GetStats(::google::protobuf::RpcController* controller,
                  const ::bcache2::bench::GetStatsRequest* request,
                  ::bcache2::bench::GetStatsResponse* response,
                  ::google::protobuf::Closure* done) override;

 private:
    Bench* bench_ = nullptr;
    WorkloadsBunch* workloads_ = nullptr;
    ConsistencyChecker* checker_ = nullptr;
};

}  // namespace bench
}  // namespace bcache2
