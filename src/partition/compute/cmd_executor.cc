// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/compute/cmd_executor.h"

#include <byte/algorithm/crc64.h>
#include <gflags/gflags.h>

#include <utility>
#include <vector>

#include "common/cmd_manager.h"
#include "common/logging.h"
#include "common/scoped_invoker.h"
#include "common/slot.h"
#include "model/hash_model.h"
#include "model/ips/ips_table_manager.h"
#include "partition/compute/execute_env.h"
#include "partition/partition.h"
#include "partition/quota_manager.h"
#include "partition/storage/object_manager.h"

DECLARE_int32(load_slot_retry_times);

namespace bcache2 {
namespace partition {

CmdExecutor::CmdExecutor(Partition* partition, ObjectManager* object_manager, OpLogger* op_logger,
                         MetricsManager* metrics_manager)
    : partition_(partition),
      object_manager_(object_manager),
      op_logger_(op_logger) {
    metrics_.Init(metrics_manager);
    quota_manager_.reset(new QuotaManager());
}

void CmdExecutor::Execute(uint16_t module_id, uint64_t function_id,
                          const google::protobuf::Message* request,
                          google::protobuf::Message* response, CmdContext* cmd_ctx,
                          Closure<void>* callback) {
    ScopedInvoker done(callback);

    Status status = PrepareCmd(module_id, function_id);
    cmd_ctx->time_tracer.AddEvent("Prepare");
    if (!status.ok()) {
        LOG_WARNING("Prepare cmd failed")
            .put("PartitionId", partition_->GetPartitionID())
            .put("Module", module_id)
            .put("Cmd", function_id)
            .put("Status", status);
        cmd_ctx->ctrl->set_status(status);
        return;
    }

    done.Release();
    ExecuteCmd(module_id, function_id, request, response, cmd_ctx, callback);
}

Status CmdExecutor::PrepareCmd(uint16_t module_id, uint64_t function_id) {
    const CmdManager::CmdInfo* cmd = CmdManager::GetCmd(module_id, function_id);
    if (cmd == nullptr) {
        LOG_WARNING("Get cmd func failed")
            .put("PartitionId", partition_->GetPartitionID())
            .put("Module", module_id)
            .put("Cmd", function_id);
        return Status::Unimplemented("Cmd not found");
    }

    auto cmd_metrics = metrics_.cmd_metrics_[module_id][function_id].get();
    switch (cmd->flag) {
    case CmdRwFlag::kWrite:
        if (partition_->IsApplyingDataRaftEntry()) {
            break;
        }
        if (partition_->Readonly()) {
            return Status::PermissionDenied("Readonly partition");
        }
        if (!quota_manager_->ConsumeQuota(QuotaType::WriteQuota)) {
            cmd_metrics->write_limit_qps->get()->Increment();
            return Status::ResourceExhausted("No write quota");
        }
        break;
    case CmdRwFlag::kRead:
        if (!quota_manager_->ConsumeQuota(QuotaType::ReadQuota)) {
            cmd_metrics->read_limit_qps->get()->Increment();
            return Status::ResourceExhausted("No read quota");
        }
        break;
    }
    return Status::OK();
}

// NOTE: ExecuteCmd may be invoke multi times due to slot load
void CmdExecutor::ExecuteCmd(uint16_t module_id, uint64_t function_id,
                             const google::protobuf::Message* request,
                             google::protobuf::Message* response, CmdContext* cmd_ctx,
                             Closure<void>* callback) {
    ScopedInvoker done(callback);

    if (cmd_ctx->load_slot_times == 1) {
        cmd_ctx->time_tracer.AddEvent("LoadObject");
    }

    const CmdManager::CmdInfo* cmd = CmdManager::GetCmd(module_id, function_id);
    cmd_ctx->metrics = metrics_.cmd_metrics_[module_id][function_id]->req_metrics.get();
    auto cmd_metrics = metrics_.cmd_metrics_[module_id][function_id].get();
    auto module_metrics = metrics_.module_metrics_[module_id].get();
    auto module_custom_config = module_configs_[cmd_ctx->module_id].get();
    ExecuteEnv execute_env(cmd_ctx, cmd_ctx->partition_id, op_logger_, object_manager_,
                           &cmd_ctx->time_tracer, cmd_metrics, module_metrics,
                           module_custom_config);
    *cmd_ctx->status = cmd->executor(&execute_env, *cmd_ctx->request, cmd_ctx->response);
    // if there are some slots that are not in memory
    if (!execute_env.off_memory_slots_.empty() &&
        cmd_ctx->load_slot_times < FLAGS_load_slot_retry_times) {
        done.Release();
        cmd_ctx->load_slot_times++;
        object_manager_->LoadObject(cmd_ctx->ctrl, execute_env.off_memory_slots_[0], false,
                                    NewClosure(this, &CmdExecutor::ExecuteCmd, module_id,
                                               function_id, request, response, cmd_ctx, callback));
        return;
    }

    cmd_ctx->time_tracer.AddEvent("ExecuteFinish");
    cmd_metrics->object_get_qps->get()->Increment();  // we think only one object is executed
    if (cmd_ctx->object_hit) {
        cmd_metrics->object_hit_qps->get()->Increment();
    }
    if (cmd_ctx->object_in_memory) {
        cmd_metrics->object_in_memory_qps->get()->Increment();
    }
}

void CmdExecutor::UpdateLimitConfig(const LimitConfig& limit_config) {
    quota_manager_->UpdateConfig(limit_config);
}

void CmdExecutor::ReapMetrics() {
    for (auto& cmd_metrics : metrics_.cmd_metrics_) {
        for (auto& cmd_metric : cmd_metrics) {
            double object_get_qps = cmd_metric->object_get_qps->get()->GetValue();
            if (object_get_qps > 1.0) {
                double object_hit_qps = cmd_metric->object_hit_qps->get()->GetValue();
                double object_in_memory_qps = cmd_metric->object_in_memory_qps->get()->GetValue();

                cmd_metric->object_hit_ratio->get()->Set(object_hit_qps / object_get_qps);

                cmd_metric->object_in_memory_ratio->get()->Set(object_in_memory_qps /
                                                               object_get_qps);
            }
        }
    }
}

Status CmdExecutor::LoadModuleCustomConfig(const Config& config) {
    auto modules = CmdManager::GetModuleInfos();
    for (size_t i = 0; i < modules.size(); i++) {
        if (Module_IsValid(i) && modules[i].config_builder_func != nullptr) {
            if (module_configs_.size() <= i) {
                module_configs_.resize(i + 1);
            }
            module_configs_[i].reset(modules[i].config_builder_func());
            CustomConfig custom_config;
            if (config.module_custom_config().count(i) > 0) {
                custom_config = config.module_custom_config().at(i);
            }
            Status status = module_configs_[i]->LoadCustomConfig(custom_config);
            if (!status.IsOK()) {
                return status;
            }
        }
    }
    return Status::OK();
}

Status CmdExecutor::UpdateModuleCustomConfig(const Config& config) {
    auto modules = CmdManager::GetModuleInfos();
    for (size_t i = 0; i < modules.size(); i++) {
        if (Module_IsValid(i) && modules[i].config_builder_func != nullptr) {
            if (module_configs_.size() <= i) {
                module_configs_.resize(i + 1);
            }
            if (config.module_custom_config().count(i) > 0) {
                const auto& custom_config = config.module_custom_config().at(i);
                Status status = module_configs_[i]->UpdateCustomConfig(custom_config);
                if (!status.IsOK()) {
                    return status;
                }
            }
        }
    }
    return Status::OK();
}

}  // namespace partition
}  // namespace bcache2
