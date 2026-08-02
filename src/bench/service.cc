// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "bench/service.h"

#include <byte/include/macros.h>

#include <utility>

#include "bench/bench.h"
#include "bench/consistency_checker.h"
#include "bench/flags.h"
#include "bench/workloads/workloads.h"

namespace bcache2 {
namespace bench {

void ServiceImpl::GetStats(::google::protobuf::RpcController* controller,
                           const ::bcache2::bench::GetStatsRequest* request,
                           ::bcache2::bench::GetStatsResponse* response,
                           ::google::protobuf::Closure* done) {
    BYTE_DEFER({ done->Run(); });

    *response->mutable_total_stats() = std::move(bench_->GetTotalStats());

    auto cmd_stats = bench_->GetCmdStats();
    for (size_t i = 0; i < cmd_stats.size(); ++i) {
        *response->add_command_stats() = std::move(cmd_stats[i]);
    }
    response->set_checking(checker_->Checking());
    response->set_consistency(checker_->Consistency());
    response->set_round(workloads_->GetRound());
}

}  // namespace bench
}  // namespace bcache2
