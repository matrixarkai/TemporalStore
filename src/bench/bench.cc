// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "bench/bench.h"

#include <absl/strings/numbers.h>
#include <byte/algorithm/crc32.h>
#include <gtest/gtest.h>
#include <sys/stat.h>

#include <algorithm>
#include <cassert>
#include <memory>
#include <utility>
#include <vector>

#include "bench/client/client.h"
#include "bench/proto_utils.h"
#include "bench/workloads/workloads.h"
#include "common/cmd_manager.h"
#include "common/coclosure.h"
#include "common/logging.h"
#include "common/time.h"
#include "extension/common/interface.pb.h"
#include "protocol/bench.pb.h"

namespace bcache2 {
namespace bench {

static std::random_device dev;
static std::mt19937 rng(dev());

void Bench::Init(Options opts) {
    opts_ = std::move(opts);

    auto modules = CmdManager::GetModuleInfos();
    for (size_t module_id = 0; module_id < modules.size(); ++module_id) {
        if (!Module_IsValid(module_id)) {
            continue;
        }
        for (size_t cmd_id = 0; cmd_id < modules[module_id].cmd_executors.size(); ++cmd_id) {
            cmd_stats_[MakeCmdId(module_id, cmd_id)].send_bytes.get_value();
        }
    }

    byte::AsyncThreadPoolOptions work_options;
    work_options.thread_num_ = opts.jobs;
    BYTE_ASSERT(work_pool_.Init(work_options));
    BYTE_ASSERT(work_pool_.Start());
}

void Bench::Start() {
    stopped_ = false;
    worker_contextes_.clear();
    total_operations_num_ = 0;
    worker_countdown_ = absl::make_unique<byte::CountDownLatch>(opts_.jobs * opts_.depth);
    wakeup_worker_num_ = opts_.jobs * opts_.depth;
    for (uint64_t thread_index = 0, worker_idx = 0; thread_index < opts_.jobs; ++thread_index) {
        for (uint32_t depth_index = 0; depth_index < opts_.depth; ++depth_index, ++worker_idx) {
            worker_contextes_.emplace_back(new WorkerContext(worker_idx));
            work_pool_.KthThread(thread_index)
                ->Invoke(NewCoClosure(this, &Bench::MainLoop, worker_contextes_.back().get()));
        }
    }
}

void Bench::Stop() {
    stopped_ = true;
    worker_countdown_->Wait();
}

void Bench::MainLoop(WorkerContext* worker) {
    BYTE_ASSERT(IsCoContext());

    LOG_INFO("Worker going to start")
        .put("WorkerId", worker->id)
        .put("Round", opts_.workloads->GetRound());
    Wakeup(worker);
    while (wakeup_worker_num_ != 0) {
        LOG_INFO("Waiting for other worker wakeup")
            .put("WorkerId", worker->id)
            .put("Round", opts_.workloads->GetRound());
        CoSleep(1 * 1000 * 1000);
    }

    if (!opts_.workloads->GetReusedKeys().empty() && opts_.delay_start_ms > 0) {
        CoSleep(opts_.delay_start_ms * 1000);
    }

    LOG_INFO("Worker started")
        .put("WorkerId", worker->id)
        .put("Round", opts_.workloads->GetRound());
    while (!stopped_) {
        Operation operation = opts_.workloads->NextOperation();
        operation.set_start_time_us(GetCurrentTimeInUs());
        LOG_DEBUG("Invoke Operation")
            .put("WorkerId", worker->id)
            .put("Operation", operation)
            .put("Round", opts_.workloads->GetRound());

        Controller ctrl;
        ctrl.set_trace_id(GetCurrentTimeInNs());
        SYNC_CALL(opts_.client->Execute, &ctrl, &operation);
        if (!ctrl.status().IsOK() && !ctrl.status().IsNotFound()) {
            operation.set_code(kInternal);
        } else if (ctrl.status().IsNotFound()) {
            // TODO(wangtai.10): separate key NotFound and something else NotFound
            operation.set_code(kNotFound);
        }
        operation.set_message(ctrl.status().ToString());

        operation.set_end_time_us(GetCurrentTimeInUs());
        byte::LogLevel log_level = byte::LogLevel::LOG_LEVEL_DEBUG;
        if (operation.code() != kOK && operation.code() != kNotFound) {
            log_level = byte::LogLevel::LOG_LEVEL_WARNING;
        }
        LOG_MESSAGE(log_level, "Request finish")
            .put("WorkerId", worker->id)
            .put("Round", opts_.workloads->GetRound())
            .put("Operation", operation)
            .put("Status", ctrl.status())
            .put("Round", opts_.workloads->GetRound());

        StatOperation(worker, operation);
        if (opts_.stay_operations) {
            worker->operations.emplace_back(std::move(operation));
            ++total_operations_num_;
        }
    }

    LOG_INFO("Worker going to stop")
        .put("WorkerId", worker->id)
        .put("Round", opts_.workloads->GetRound());
    Postpone(worker);

    worker_countdown_->CountDown();
    LOG_INFO("Worker stopped")
        .put("WorkerId", worker->id)
        .put("Round", opts_.workloads->GetRound());
}

void Bench::Wakeup(WorkerContext* worker) {
    // delete all reused_keys
    const std::vector<std::string>& keys = opts_.workloads->GetReusedKeys();
    for (auto& key : keys) {
        uint32_t crc = byte::CRCUtil::ComputeCRC32(0, key.data(), key.size());
        if (crc % (opts_.jobs * opts_.depth) != worker->id) {
            // only care about keys belongs to me
            continue;
        }

        // delete key until success
        Operation op;
        do {
            Controller ctrl;
            common2::DelObjectRequest request;
            common2::DelObjectResponse response;
            request.set_key(key);

            op.Clear();
            op.set_module_id(Module::COMMON);
            op.set_function_id(common2::DEL_OBJECT);
            op.set_key(key);
            request.SerializeToString(op.mutable_request_bytes());
            SYNC_CALL(opts_.client->Execute, &ctrl, &op);
            if (!ctrl.status().IsOK() && !ctrl.status().IsNotFound()) {
                op.set_code(kInternal);
            } else if (ctrl.status().IsNotFound()) {
                // TODO(wangtai.10): separate key NotFound and something else NotFound
                op.set_code(kNotFound);
            }
            op.set_message(ctrl.status().ToString());

            byte::LogLevel log_level = byte::LogLevel::LOG_LEVEL_DEBUG;
            if (op.code() != kOK && op.code() != kNotFound) {
                log_level = byte::LogLevel::LOG_LEVEL_WARNING;
            }
            LOG_MESSAGE(log_level, "Delete request finish")
                .put("WorkerId", worker->id)
                .put("Round", opts_.workloads->GetRound())
                .put("Operation", op)
                .put("Status", ctrl.status());
            StatOperation(worker, op);
        } while (op.code() != kOK && op.code() != kNotFound);
        LOG_INFO("Delete key")
            .put("WorkerId", worker->id)
            .put("Key", key)
            .put("Round", opts_.workloads->GetRound());
    }
    --wakeup_worker_num_;
}

void Bench::Postpone(WorkerContext* worker) {
    // FIXME(wangtai.10): all keys not exist

    // set ttl for all keys
    for (auto& raw_key : opts_.workloads->GetRawKeys()) {
        uint32_t crc = byte::CRCUtil::ComputeCRC32(0, raw_key.data(), raw_key.size());
        if (crc % (opts_.jobs * opts_.depth) != worker->id) {
            // only care about keys belongs to me
            continue;
        }

        std::string key = opts_.workloads->GetRoundKey(raw_key);
        // expire key until success
        Operation op;
        do {
            Controller ctrl;
            common2::ExpireRequest request;
            common2::ExpireResponse response;
            request.set_key(key);
            request.set_ttl_ms(opts_.key_ttl_ms);

            op.Clear();
            op.set_module_id(Module::COMMON);
            op.set_function_id(common2::EXPIRE);
            op.set_key(key);
            request.SerializeToString(op.mutable_request_bytes());
            SYNC_CALL(opts_.client->Execute, &ctrl, &op);
            if (!ctrl.status().IsOK() && !ctrl.status().IsNotFound()) {
                op.set_code(kInternal);
            } else if (ctrl.status().IsNotFound()) {
                // TODO(wangtai.10): separate key NotFound and something else NotFound
                op.set_code(kNotFound);
            }
            op.set_message(ctrl.status().ToString());

            byte::LogLevel log_level = byte::LogLevel::LOG_LEVEL_DEBUG;
            if (op.code() != kOK && op.code() != kNotFound) {
                log_level = byte::LogLevel::LOG_LEVEL_WARNING;
            }
            LOG_MESSAGE(log_level, "Expire request finish")
                .put("WorkerId", worker->id)
                .put("Round", opts_.workloads->GetRound())
                .put("Operation", op)
                .put("Status", ctrl.status());
            StatOperation(worker, op);
        } while (op.code() != kOK && op.code() != kNotFound);
        LOG_INFO("Set ttl for key")
            .put("WorkerId", worker->id)
            .put("Round", opts_.workloads->GetRound())
            .put("Key", key)
            .put("TtlMs", opts_.key_ttl_ms)
            .put("Round", opts_.workloads->GetRound());
    }
}

void Bench::StatOperation(WorkerContext* worker, const Operation& operation) {
    uint64_t latency_us = operation.end_time_us() - operation.start_time_us();

    Stats* stats = &cmd_stats_[MakeCmdId(operation.module_id(), operation.function_id())];
    stats->effective = true;
    total_stats_.total_latency_us << latency_us;
    total_stats_.send_bytes << operation.request_bytes().size();
    total_stats_.receive_bytes << operation.response_bytes().size();
    stats->total_latency_us << latency_us;
    stats->send_bytes << operation.request_bytes().size();
    stats->receive_bytes << operation.response_bytes().size();
    if (operation.code() == kOK || operation.code() == kNotFound) {
        total_stats_.succ_latency_us << latency_us;
        stats->succ_latency_us << latency_us;
    } else {
        total_stats_.fail_latency_us << latency_us;
        stats->fail_latency_us << latency_us;
    }

    if (operation.end_time_us() - operation.start_time_us() > 100 * 1000) {
        LOG_WARNING("Slow Request")
            .put("WorkerId", worker->id)
            .put("Round", opts_.workloads->GetRound())
            .put("Operation", operation);
    }
}

std::vector<CommandStats> Bench::GetCmdStats() const {
    std::vector<CommandStats> cmd_stats;
    for (const auto& pair : cmd_stats_) {
        if (!pair.second.effective) {
            continue;
        }

        CommandStats stats;
        stats.set_command(CmdManager::GetCmd(pair.first)->module_name + "_" +
                          CmdManager::GetCmd(pair.first)->name);
        stats.set_total_qps(pair.second.total_latency_us.qps());
        stats.set_success_qps(pair.second.succ_latency_us.qps());
        stats.set_failed_qps(pair.second.fail_latency_us.qps());
        stats.set_avg_latency_us(pair.second.succ_latency_us.latency());
        stats.set_p50_latency_us(pair.second.succ_latency_us.latency_percentile(0.5));
        stats.set_p99_latency_us(pair.second.succ_latency_us.latency_percentile(0.99));
        stats.set_send_throughput_bytes(pair.second.send_throughput.get_value(5));
        stats.set_receive_throughput_bytes(pair.second.receive_throughput.get_value(5));
        cmd_stats.emplace_back(stats);
    }
    return cmd_stats;
}

CommandStats Bench::GetTotalStats() const {
    CommandStats stats;
    stats.set_total_qps(total_stats_.total_latency_us.qps());
    stats.set_success_qps(total_stats_.succ_latency_us.qps());
    stats.set_failed_qps(total_stats_.fail_latency_us.qps());
    stats.set_avg_latency_us(total_stats_.succ_latency_us.latency());
    stats.set_p50_latency_us(total_stats_.succ_latency_us.latency_percentile(0.5));
    stats.set_p99_latency_us(total_stats_.succ_latency_us.latency_percentile(0.99));
    stats.set_send_throughput_bytes(total_stats_.send_throughput.get_value(5));
    stats.set_receive_throughput_bytes(total_stats_.receive_throughput.get_value(5));
    return stats;
}

void Bench::PrintStats() const {
    const auto& cmd_stats = GetCmdStats();
    const auto& total_stats = GetTotalStats();
    printf("Bench Stats\n");

    printf("\tTotal\n");
    printf("\t\ttotal/success/failed qps %" PRIu64 "/%" PRIu64 "/%" PRIu64
           ", avg/p50/p99 latency %" PRIu64 "/%" PRIu64 "/%" PRIu64
           " us"
           ", send/receive throughput %.2f/%.2f MB\n",
           total_stats.total_qps(), total_stats.success_qps(), total_stats.failed_qps(),
           total_stats.avg_latency_us(), total_stats.p50_latency_us(), total_stats.p99_latency_us(),
           1.0 * total_stats.send_throughput_bytes() / 1024 / 1024,
           1.0 * total_stats.receive_throughput_bytes() / 1024 / 1024);
    printf("\t\toperations: %" PRIu64 "\n", total_operations_num_.load());

    printf("\tCommands\n");
    for (const auto& stats : cmd_stats) {
        printf("\t\t%s: total/success/failed qps %" PRIu64 "/%" PRIu64 "/%" PRIu64
               ", avg/p50/p99 latency %" PRIu64 "/%" PRIu64 "/%" PRIu64
               " us"
               ", send/receive throughput %.2f/%.2f MB\n",
               stats.command().c_str(), stats.total_qps(), stats.success_qps(), stats.failed_qps(),
               stats.avg_latency_us(), stats.p50_latency_us(), stats.p99_latency_us(),
               1.0 * stats.send_throughput_bytes() / 1024 / 1024,
               1.0 * stats.receive_throughput_bytes() / 1024 / 1024);
    }

    printf("\tWorkloads\n");
    printf("\t\tRound: %" PRIu64 "\n", opts_.workloads->GetRound());
}

}  // namespace bench
}  // namespace bcache2
