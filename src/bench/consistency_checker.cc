// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "bench/consistency_checker.h"

#include <byte/concurrent/count_down_latch.h>
#include <byte/include/assert.h>
#include <byte/thread/async_thread.h>
#include <inttypes.h>

#include <algorithm>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bench/model/nil_model.h"
#include "bench/proto_utils.h"
#include "common/cmd_manager.h"
#include "common/logging.h"
#include "common/time.h"

namespace bcache2 {
namespace bench {

void ConsistencyChecker::Init(Options opts) {
    consistency_ = true;
    opts_ = opts;
    apply_opts_.max_expire_ambiguous_time_ms = opts_.max_expire_ambiguous_time_ms;
    if (opts_.eventual_consistency_mode) {
        // do not check history if eventual consistency mode
        apply_opts_.max_expire_ambiguous_time_ms =
            opts_.max_expire_ambiguous_time_ms + opts_.eventual_consistency_history_time_us / 1000;
    } else {
        // do not check history if not eventual consistency mode
        opts_.eventual_consistency_history_time_us = 0;
    }

    byte::AsyncThreadPoolOptions work_options;
    work_options.thread_num_ = opts.worker_num;
    BYTE_ASSERT(worker_pool_.Init(work_options));
    BYTE_ASSERT(worker_pool_.Start());
}

void ConsistencyChecker::CheckConsistency(std::vector<std::vector<Operation>> ops) {
    // waiting for previous checker finish
    if (checker_countdown_ != nullptr) {
        checker_countdown_->Wait();
    }

    // aggregate by key
    std::unordered_map<std::string, std::vector<Operation>> key_ops;  // ops of key
    for (auto& op_vec : ops) {
        for (auto& op : op_vec) {
            if (op.code() != kOK && op.code() != kNotFound) {
                // operatio failed, we don't know when the operation is actually executed,
                // and we don't even know if the operation is actually executed or not.
                // so we increase operation.end_time and do some guesses later
                op.set_end_time_us(op.end_time_us() + opts_.max_ambiguous_time_ms * 1000);
            }
            key_ops[op.key()].emplace_back(std::move(op));
        }
    }

    // submit check task
    total_checker_ = key_ops.size();
    checker_countdown_.reset(new byte::CountDownLatch(total_checker_));
    for (auto& iter : key_ops) {
        CheckContext* context = new CheckContext();
        context->key = iter.first;
        context->ops = std::move(iter.second);
        context->countdown = checker_countdown_.get();
        worker_pool_.PushTask(NewClosure(this, &ConsistencyChecker::CheckInternal, context), 0);
    }
}

void ConsistencyChecker::CheckInternal(CheckContext* context) {
    BYTE_DEFER({
        context->countdown->CountDown();
        delete context;
    });

    LOG_INFO("Check consistency").put("Key", context->key).put("OperationNum", context->ops.size());
    auto start = GetCurrentTimeInUs();
    BYTE_DEFER({
        LOG_INFO("Finish check validate")
            .put("Key", context->key)
            .put("ElapsedUs", GetCurrentTimeInUs() - start);
    });

    std::sort(context->ops.begin(), context->ops.end(),
              [](const Operation& lhs, const Operation& rhs) {
                  return lhs.start_time_us() < rhs.start_time_us();
              });
    for (auto& op : context->ops) {
        context->end_time_us_heap.insert(op.end_time_us());
    }

    // we have n operations and n+1 state
    context->states_history.resize(context->ops.size() + 1, nullptr);
    NilModel init;
    if (!WG(context, 0, 0, &init, GetCurrentTimeInUs())) {
        consistency_ = false;
        LOG_WARNING("Consistency check failed")
            .put("Key", context->key)
            .put("OperationNum", context->ops.size());
        for (const auto& op : context->ops) {
            LOG_WARNING("Show Operations").put("Key", context->key).put("Operation", op);
        }
    }
}

bool ConsistencyChecker::WG(CheckContext* context, size_t depth, uint64_t max_start_time_us,
                            const Model* current_state, uint64_t start_time_us) {
    context->states_history[depth] = current_state;

    if (byte::GetMinLogLevel() <= byte::LogLevel::LOG_LEVEL_DEBUG) {
        for (size_t i = 0; i < depth; ++i) {
            LOG_DEBUG("Current state")
                .put("Key", context->key)
                .put("Depth", depth)
                .put("Idx", i)
                .put("Operations", context->ops[i])
                .put("StateHistories", *context->states_history[i + 1]);
        }
    }

    if (depth == context->ops.size()) {
        LOG_INFO("Found a topological order that satisfies operations correctness")
            .put("Key", context->key);
        return true;
    }

    if (opts_.timeout_ms > 0 && GetCurrentTimeInUs() - start_time_us > opts_.timeout_ms * 1000) {
        LOG_WARNING("Timeout").put("Key", context->key);
        timeout_ = true;
        return false;
    }

    if (*context->end_time_us_heap.begin() < max_start_time_us) {
        LOG_DEBUG("Invalite path")
            .put("Key", context->key)
            .put("Depth", depth)
            .put("MaxStartTimeUs", max_start_time_us)
            .put("MaxOperationEndTimeUs", *context->end_time_us_heap.begin());
        return false;
    }

    // pick an operation replay and check correctness
    for (size_t i = depth; i < context->ops.size(); ++i) {
        BYTE_ASSERT(context->ops[i].end_time_us() >= max_start_time_us);

        std::vector<std::unique_ptr<Model>> next_states;
        Status status = TryApplyOperation(context, depth, context->ops[i], &next_states);
        if (!status.ok()) {
            LOG_DEBUG("Skip operation")
                .put("Key", context->key)
                .put("Depth", depth)
                .put("Index", i)
                .put("Operation", context->ops[i])
                .put("Status", status);
            continue;
        }

        LOG_DEBUG("Apply operation")
            .put("Key", context->key)
            .put("Depth", depth)
            .put("Index", i)
            .put("Operation", context->ops[i])
            .put("NextStatNum", next_states.size());
        auto it = context->end_time_us_heap.find(context->ops[i].end_time_us());
        BYTE_ASSERT(it != context->end_time_us_heap.end());
        context->end_time_us_heap.erase(it);
        std::swap(context->ops[depth], context->ops[i]);

        for (auto& next_state : next_states) {
            if (WG(context, depth + 1,
                   std::max(max_start_time_us, context->ops[depth].start_time_us()),
                   next_state.get(), start_time_us)) {
                return true;
            }
        }

        LOG_DEBUG("Redo operation")
            .put("Key", context->key)
            .put("Depth", depth)
            .put("Operation", context->ops[depth]);
        std::swap(context->ops[depth], context->ops[i]);
        context->end_time_us_heap.insert(context->ops[i].end_time_us());
    }

    return false;
}

Status ConsistencyChecker::TryApplyOperation(CheckContext* context, size_t depth,
                                             const Operation& op,
                                             std::vector<std::unique_ptr<Model>>* next_states) {
    if (!opts_.eventual_consistency_mode || depth == 0) {
        // always apply on newest state
        return context->states_history[depth]->Apply(apply_opts_, op, next_states);
    }

    // eventual consistency mode
    bool first_write_op = true;
    uint64_t history_gap_us = 0;
    auto cmd = CmdManager::GetCmd(op.module_id(), op.function_id());
    for (size_t op_history_idx = depth - 1; op_history_idx >= 0 && op_history_idx != UINT64_MAX;
         --op_history_idx) {
        const CmdManager::CmdInfo* history_cmd = CmdManager::GetCmd(
            context->ops[op_history_idx].module_id(), context->ops[op_history_idx].function_id());
        CmdRwFlag history_flag = history_cmd->flag;
        if (history_flag != CmdRwFlag::kWrite) {
            continue;
        }

        if (!first_write_op && history_gap_us > opts_.eventual_consistency_history_time_us) {
            // The operation history is too far removed from op
            continue;
        }

        first_write_op = false;
        history_gap_us =
            std::max(context->ops[op_history_idx].start_time_us(), op.start_time_us()) -
            std::min(context->ops[op_history_idx].start_time_us(), op.start_time_us());

        // apply to the state after op_history_idx applied
        std::vector<std::unique_ptr<Model>> sub_next_states;
        Status status =
            context->states_history[op_history_idx + 1]->Apply(apply_opts_, op, &sub_next_states);
        LOG_DEBUG("Try apply operation")
            .put("Key", context->key)
            .put("Depth", depth)
            .put("OperationHistoryIdx", op_history_idx)
            .put("BaseState", *context->states_history[op_history_idx + 1])
            .put("PickedOperation", op)
            .put("Status", status);
        if (status.ok()) {
            for (auto& state : sub_next_states) {
                next_states->emplace_back(std::move(state));
            }
        }

        if (cmd->flag == CmdRwFlag::kWrite) {
            // wo do not check more history for write cmd
            break;
        }
    }

    // try apply on init state
    if (first_write_op || history_gap_us <= opts_.eventual_consistency_history_time_us) {
        std::vector<std::unique_ptr<Model>> sub_next_states;
        Status status = context->states_history[0]->Apply(apply_opts_, op, &sub_next_states);
        LOG_DEBUG("Try apply operation")
            .put("Key", context->key)
            .put("Depth", depth)
            .put("BaseState", *context->states_history[0])
            .put("PickedOperation", op)
            .put("Status", status);
        if (status.ok()) {
            for (auto& state : sub_next_states) {
                next_states->emplace_back(std::move(state));
            }
        }
    }

    if (next_states->empty()) {
        return Status::Internal("No valid history state");
    }
    return Status::OK();
}

void ConsistencyChecker::PrintStats() {
    printf("\tConsistency Checker\n");
    printf("\t\tConsistency: %s, Checking: %s, CheckContextes %" PRIu64 "/%" PRIu64 "\n",
           consistency_ ? "true" : "false",
           checker_countdown_ && checker_countdown_->GetCount() > 0 ? "true" : "false",
           checker_countdown_
               ? total_checker_ - static_cast<uint64_t>(checker_countdown_->GetCount())
               : total_checker_,
           total_checker_);
}

}  // namespace bench
}  // namespace bcache2
