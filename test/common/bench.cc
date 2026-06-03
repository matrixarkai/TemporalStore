// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "test/common/bench.h"

#include <absl/strings/numbers.h>

#include <iomanip>
#include <iostream>

#include "common/logging.h"
#include "common/time.h"

namespace bcache2 {

Bench::Bench() {}

Bench::~Bench() {}

void Bench::Init(const Options& options) { options_ = options; }

void Bench::RegisterOp(int op_type, const std::string& name, uint32_t depth, uint64_t interval_ms) {
    OpInfo* op_info = new OpInfo;
    op_info->op_type = op_type;
    op_info->name = name;
    op_info->depth = depth;
    op_info->interval_us = interval_ms * 1000;

    op_info_map_[op_type].reset(op_info);
}

void Bench::RegisterOp(int op_type, const std::string& name, const std::string& freq) {
    std::string::size_type pos = freq.find('/');
    uint32_t depth = 0;
    uint64_t interval_ms = 0;
    if (pos == std::string::npos) {
        BYTE_ASSERT(absl::SimpleAtoi(freq.substr(0, pos), &depth));
    } else {
        BYTE_ASSERT(absl::SimpleAtoi(freq.substr(0, pos), &depth));
        BYTE_ASSERT(absl::SimpleAtoi(freq.substr(pos + 1), &interval_ms));
    }
    RegisterOp(op_type, name, depth, interval_ms);
}

void Bench::Start() {
    int total_depth = 0;
    for (auto& it : op_info_map_) {
        OpInfo* op_info = it.second.get();
        op_info->contexts.reset(new OpContext[options_.jobs * op_info->depth]);
        for (int thread_index = 0; thread_index < options_.jobs; ++thread_index) {
            for (uint32_t depth_index = 0; depth_index < op_info->depth; ++depth_index) {
                LOG_INFO("Run op thread")
                    .put("Op", op_info->name)
                    .put("ThreadIndex", thread_index)
                    .put("DepthInex", depth_index);
                OpContext* context =
                    &op_info->contexts[thread_index * op_info->depth + depth_index];
                context->op_info = op_info;
                context->thread = options_.thread_pool->KthThread(thread_index);
                context->thread->Invoke(NewClosure(this, &Bench::RunOp, context));

                ++total_depth;
            }
        }
    }
    count_latch_.reset(new byte::CountDownLatch(total_depth));
}

void Bench::Stop() {
    stop_ = true;
    count_latch_->Wait();
}

std::vector<Bench::Stat> Bench::GetStats() {
    std::vector<Bench::Stat> stats;
    for (auto& it : op_info_map_) {
        OpInfo* op_info = it.second.get();
        uint64_t succ_count = 0;
        uint64_t succ_sum_latency = 0;
        uint64_t fail_count = 0;
        uint64_t fail_sum_latency = 0;
        for (size_t i = 0; i < options_.jobs * op_info->depth; ++i) {
            OpContext* context = &op_info->contexts[i];
            succ_count += context->succ_count;
            succ_sum_latency += context->succ_sum_latency;
            fail_count += context->fail_count;
            fail_sum_latency += context->fail_sum_latency;
        }

        Bench::Stat stat;
        BYTE_ASSERT(succ_count >= op_info->last_succ_count);
        BYTE_ASSERT(succ_sum_latency >= op_info->last_succ_sum_latency);
        BYTE_ASSERT(fail_count >= op_info->last_fail_count);
        BYTE_ASSERT(fail_sum_latency >= op_info->last_fail_sum_latency);

        stat.name = op_info->name;
        stat.succ_count = succ_count - op_info->last_succ_count;
        stat.succ_avg_latency_ns =
            stat.succ_count == 0 ? 0 : (succ_sum_latency - op_info->last_succ_sum_latency) /
                                           stat.succ_count;
        stat.fail_count = fail_count - op_info->last_fail_count;
        stats.push_back(stat);

        op_info->last_succ_count = succ_count;
        op_info->last_succ_sum_latency = succ_sum_latency;
        op_info->last_fail_count = fail_count;
        op_info->last_fail_sum_latency = fail_sum_latency;
    }
    return stats;
}

void Bench::ShowStats() {
    std::vector<Bench::Stat> stats = GetStats();
    static uint64_t counter = 0;
    std::time_t time = std::time(nullptr);
    if (counter++ % 10 == 0) {
        std::cout << "             ";
        for (size_t i = 0; i < stats.size(); ++i) {
            printf("%20s      ", stats[i].name.c_str());
        }
        printf("\n");
        std::cout << "             ";
        for (size_t i = 0; i < stats.size(); ++i) {
            printf("  error  success latency  ");
        }
        printf("\n");
    }
    std::cout << std::put_time(std::localtime(&time), "%d %H:%M:%S") << "  ";
    for (size_t i = 0; i < stats.size(); ++i) {
        printf("%7lu /%7lu:%7lu  ", stats[i].fail_count, stats[i].succ_count,
               stats[i].succ_avg_latency_ns / 1000);
    }
    printf("\n");
}

void Bench::RunOp(OpContext* context) {
    BYTE_ASSERT(byte::GetCurrentThread() == context->thread);
    BYTE_ASSERT(context->operation == nullptr);
    context->operation = options_.verifier->GeneratorOp(context->op_info->op_type);
    BYTE_ASSERT(context->operation != nullptr);
    context->ctrl.Reset();
    context->start_time_ns = GetCurrentTimeInNs();
    context->operation->Run(&context->ctrl, NewClosure(this, &Bench::OnRunOpDone, context));
}

void Bench::OnRunOpDone(OpContext* context) {
    BYTE_ASSERT(byte::GetCurrentThread() == context->thread);
    context->end_time_ns = GetCurrentTimeInNs();
    BYTE_ASSERT(context->end_time_ns >= context->start_time_ns);
    if (context->ctrl.status().ok()) {
        context->succ_count++;
        context->succ_sum_latency += context->end_time_ns - context->start_time_ns;
    } else {
        context->fail_count++;
        context->fail_sum_latency += context->end_time_ns - context->start_time_ns;
    }

    options_.verifier->FinishOp(context->operation);
    context->operation = nullptr;

    if (UNLIKELY(stop_)) {
        count_latch_->CountDown();
        return;
    }
    uint64_t interval_us = context->ctrl.status().ok() ? context->op_info->interval_us : 1000000UL;
    if (UNLIKELY(interval_us != 0)) {
        byte::InvokeLaterInCurrentThread(interval_us, NewClosure(this, &Bench::RunOp, context));
        return;
    }
    byte::InvokeInCurrentThread(NewClosure(this, &Bench::RunOp, context));
}

}  // namespace bcache2
