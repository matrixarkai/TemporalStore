// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/container/flat_hash_map.h>
#include <byte/base/closure.h>
#include <byte/include/macros.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/controller.h"
#include "common/logging.h"
#include "common/metrics.h"
#include "partition/cmd_context.h"
#include "partition/metrics.h"
#include "protocol/server.pb.h"

#define CMD_EXECUTOR_PREPARER_NAME(module_type, cmd_type) module_type##cmd_type##PrepareCtx
#define CMD_EXECUTOR_EXECUTOR_NAME(module_type, cmd_type) module_type##cmd_type##Execute
#define CMD_EXECUTOR_NAME_INTERNAL(name) name##Internal

#define REGISTER_CMD_EXECUTOR(name_space, module_type, module_name, cmd_type, func, is_executor) \
    INITIALIZE({                                                                                 \
        ModuleManager::Ref().RegisterModuleCmdExecutor(                                          \
            static_cast<size_t>(CmdRequest::k##module_type##Request),                            \
            static_cast<size_t>(name_space::module_type##ModuleRequest::k##cmd_type##Request),   \
            #module_type, #cmd_type,                                                             \
            [](const CmdRequest* request) {                                                      \
                return static_cast<size_t>(request->module_name##_request().cmd_case());         \
            },                                                                                   \
            std::move(func), is_executor);                                                       \
    });

#define CMD_EXECUTOR_FUNC_DEFINITION(name_space, is_executor, func_name, module_type, module_name, \
                                     cmd_type, cmd_name)                                           \
    Status func_name(const ModuleManager::Options& options, CmdContext* ctx,                       \
                     const name_space::cmd_type##Request* request,                                 \
                     name_space::cmd_type##Response* response);                                    \
    Status CMD_EXECUTOR_NAME_INTERNAL(func_name)(const ModuleManager::Options& options,            \
                                                 CmdContext* ctx, const CmdRequest* request,       \
                                                 CmdResponse* response) {                          \
        return func_name(                                                                          \
            options, ctx, &request->module_name##_request().cmd_name##_request(),                  \
            response->mutable_##module_name##_response()->mutable_##cmd_name##_response());        \
    }                                                                                              \
    REGISTER_CMD_EXECUTOR(name_space, module_type, module_name, cmd_type,                          \
                          CMD_EXECUTOR_NAME_INTERNAL(func_name), is_executor)                      \
    Status func_name(const ModuleManager::Options& options, CmdContext* ctx,                       \
                     const name_space::cmd_type##Request* request,                                 \
                     name_space::cmd_type##Response* response)

#define CMD_EXECUTOR_PREPARE_CTX(name_space, module_type, module_name, cmd_type, cmd_name)       \
    CMD_EXECUTOR_FUNC_DEFINITION(name_space, false,                                              \
                                 CMD_EXECUTOR_PREPARER_NAME(module_type, cmd_type), module_type, \
                                 module_name, cmd_type, cmd_name)

#define CMD_EXECUTOR_EXECUTE(name_space, module_type, module_name, cmd_type, cmd_name)           \
    CMD_EXECUTOR_FUNC_DEFINITION(name_space, true,                                               \
                                 CMD_EXECUTOR_EXECUTOR_NAME(module_type, cmd_type), module_type, \
                                 module_name, cmd_type, cmd_name)

#define CMD_EXECUTOR_PREPARE_CTX_DEFAULT(name_space, module_type, module_name, cmd_type, cmd_name, \
                                         object_model_id)                                          \
    CMD_EXECUTOR_PREPARE_CTX(name_space, module_type, module_name, cmd_type, cmd_name) {           \
        auto& key = request->key();                                                                \
        ctx->slot_id = hash_func(key.data(), key.size());                                          \
        ctx->key = absl::string_view(key.data(), key.size());                                      \
        ctx->model_id = object_model_id;                                                           \
        return Status::OK();                                                                       \
    }
namespace bcache2 {
namespace partition {

class ObjectManager;

// all modules will be registered here when Server starts
class ModuleManager {
 public:
    struct Options {
        ObjectManager* object_manager_ = nullptr;
    };

    using ExecuteFunc = std::function<Status(const Options& options, CmdContext* ctx,
                                             const CmdRequest* request, CmdResponse* response)>;
    using GetCmdIdFunc = std::function<size_t(const CmdRequest* request)>;

    struct CmdExecutorApi {
        std::string name;
        ExecuteFunc ctx_preparer;
        ExecuteFunc executor;
    };

    struct ModuleApi {
        std::string name;
        GetCmdIdFunc get_cmd_id;
        std::vector<CmdExecutorApi> cmd_executors;
    };

    static ModuleManager& Ref() {  // singleton
        static ModuleManager ref;
        return ref;
    }

    void RegisterModuleCmdExecutor(size_t module_index, size_t cmd_id,
                                   const std::string& module_name, const std::string& cmd_name,
                                   GetCmdIdFunc get_cmd_id, ExecuteFunc func, bool is_executor) {
        BYTE_ASSERT(module_index >= 0);

        if (modules_api_.size() <= module_index) {
            modules_api_.resize(module_index + 1);
        }

        if (modules_api_[module_index].get_cmd_id == nullptr) {
            modules_api_[module_index].name = module_name;
            modules_api_[module_index].get_cmd_id = std::move(get_cmd_id);
        }
        if (modules_api_[module_index].cmd_executors.size() <= cmd_id) {
            modules_api_[module_index].cmd_executors.resize(cmd_id + 1);
        }
        modules_api_[module_index].cmd_executors[cmd_id].name = cmd_name;
        if (is_executor) {
            modules_api_[module_index].cmd_executors[cmd_id].executor = std::move(func);
        } else {
            modules_api_[module_index].cmd_executors[cmd_id].ctx_preparer = std::move(func);
        }
    }

    const std::vector<ModuleApi>& GetModuleApiTable() const { return modules_api_; }

    Status GetFunc(size_t module_index, size_t cmd_id, ExecuteFunc* preparer,
                   ExecuteFunc* executor) {
        if (modules_api_.size() <= module_index ||
            modules_api_[module_index].cmd_executors[cmd_id].ctx_preparer == nullptr ||
            modules_api_[module_index].cmd_executors[cmd_id].executor == nullptr) {
            return Status::Unimplemented("Cmd not set");
        }
        *preparer = modules_api_[module_index].cmd_executors[cmd_id].ctx_preparer;
        *executor = modules_api_[module_index].cmd_executors[cmd_id].executor;
        return Status::OK();
    }

    Status GetId(const CmdRequest* request, size_t* module_index, size_t* cmd_id) {
        if (request->module_case() == CmdRequest::MODULE_NOT_SET) {
            return Status::Unimplemented("Module not set");
        }

        *module_index = static_cast<size_t>(request->module_case());

        if (modules_api_.size() <= *module_index ||
            modules_api_[*module_index].get_cmd_id == nullptr) {
            return Status::Unimplemented("Cmd not set");
        }
        *cmd_id = modules_api_[*module_index].get_cmd_id(request);
        return Status::OK();
    }

 private:
    ModuleManager() = default;
    ~ModuleManager() = default;

    std::vector<ModuleApi> modules_api_;

    DISALLOW_COPY_AND_ASSIGN(ModuleManager);
};

// For commands without module ID (compatibility)
// This class tries to fetch the module ID from the request
class CmdExecutorManager {
 public:
    CmdExecutorManager(ObjectManager* object_manager, MetricsManager* metrics_manager);
    ~CmdExecutorManager() = default;

    void ExecuteCmd(CmdContext* ctx, const CmdRequest* request, CmdResponse* response,
                    Closure<void>* callback);
    void CallExecutor(ModuleManager::ExecuteFunc executor, ModuleManager::Options options,
                      CmdContext* ctx, const CmdRequest* request, CmdResponse* response,
                      Closure<void>* callback);

 private:
    ObjectManager* object_manager_ = nullptr;
    std::vector<std::vector<std::unique_ptr<RequestMetrics>>> cmd_metrics_;

    DISALLOW_COPY_AND_ASSIGN(CmdExecutorManager);
};

}  // namespace partition
}  // namespace bcache2
