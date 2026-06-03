// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/assert.h>
#include <google/protobuf/message.h>

#include <functional>
#include <string>
#include <vector>

#include "common/macros.h"
#include "common/module_config.h"
#include "common/status.h"
#include "extension/modules.pb.h"
#include "protocol/config.pb.h"

namespace bcache2 {
class MetricsManager;
class ModuleMetrics;
}  // namespace bcache2

#define REGISTER_FUNCTION(module, function_id, function, rw)                                     \
    static_assert(function_id >= 0 && function_id < 65536, "id must be uint16_t");               \
    INITIALIZE({                                                                                 \
        bcache2::CmdManager::RegisterFunction(Module::module, #function, function_id, &function, \
                                              CmdRwFlag::k##rw);                                 \
    })

#define REGISTER_MODULE_METRICS(module, model_metrics)                                           \
    INITIALIZE({                                                                                 \
        bcache2::CmdManager::RegisterModuleMetrics(                                              \
            Module::module,                                                                      \
            [](MetricsManager* metrics_manager) { return new model_metrics(metrics_manager); }); \
    })

#define REGISTER_MODULE_CONFIG_INIT(module, module_custom_config)                               \
    INITIALIZE({                                                                                \
        bcache2::CmdManager::RegisterModuleConfig(Module::module,                               \
                                                  []() { return new module_custom_config(); }); \
    })

namespace bcache2 {

class ExecuteEnv;

enum class CmdRwFlag {
    kWrite,
    kRead,
};

inline uint32_t MakeCmdId(uint16_t module_id, uint16_t function_id) {
    return (static_cast<uint32_t>(module_id) << 16) | function_id;
}

inline uint16_t GetModuleId(uint32_t cmd_id) { return cmd_id >> 16; }

inline uint16_t GetFunctionId(uint32_t cmd_id) { return cmd_id & 0xFFFF; }

class CmdManager {
 public:
    using RequestBuilderFunc = std::function<google::protobuf::Message*()>;
    using ResponseBuilderFunc = std::function<google::protobuf::Message*()>;
    using ExecutorFunc = std::function<Status(ExecuteEnv*, const google::protobuf::Message&,
                                              google::protobuf::Message*)>;
    using ModuleMetricsFactoryFunc = std::function<ModuleMetrics*(MetricsManager*)>;
    using CustomConfigBuilderFunc = std::function<ModuleCustomConfig*()>;

    struct CmdInfo {
        std::string module_name;
        std::string name;
        RequestBuilderFunc request_builder;
        ResponseBuilderFunc response_builder;
        ExecutorFunc executor;
        CmdRwFlag flag = CmdRwFlag::kWrite;
    };

    struct ModuleInfo {
        std::string name;
        std::vector<CmdInfo> cmd_executors;
        ModuleMetricsFactoryFunc module_metrics_factory_func;
        CustomConfigBuilderFunc config_builder_func;
    };

    static CmdManager& Instance();

    static void RegisterModuleMetrics(Module module, ModuleMetricsFactoryFunc func) {
        BYTE_ASSERT(Module_IsValid(module));
        uint16_t module_id = static_cast<uint16_t>(module);
        if (module_id >= Instance().modules_.size()) {
            Instance().modules_.resize(module_id + 1);
        }
        Instance().modules_[module_id].module_metrics_factory_func = func;
    }

    template <typename Request, typename Response>
    static void RegisterFunction(Module module, const std::string& function_name,
                                 uint16_t function_id,
                                 Status (*func)(ExecuteEnv*, const Request&, Response*),
                                 CmdRwFlag flag) {
        /*
        LOG_INFO("Register function")
            .put("Module", static_cast<int>(module))
            .put("Name", function_name)
            .put("FunctionId", function_id)
            .put("Func", func);
            */
        BYTE_ASSERT(!function_name.empty() && func != nullptr) << function_name << " " << func;
        BYTE_ASSERT(Module_IsValid(module));
        uint16_t module_id = static_cast<uint16_t>(module);
        if (module_id >= Instance().modules_.size()) {
            Instance().modules_.resize(module_id + 1);
        }
        ModuleInfo& module_info = Instance().modules_[module_id];
        module_info.name = Module_Name(module);
        if (function_id >= module_info.cmd_executors.size()) {
            module_info.cmd_executors.resize(function_id + 1);
        }
        CmdInfo& cmd = module_info.cmd_executors[function_id];
        cmd.module_name = Module_Name(module);
        cmd.name = function_name;
        cmd.request_builder = []() -> google::protobuf::Message* { return new Request(); };
        cmd.response_builder = []() -> google::protobuf::Message* { return new Response(); };
        cmd.executor = [func](ExecuteEnv* env, const google::protobuf::Message& request_message,
                              google::protobuf::Message* response_message) -> Status {
            const Request& request = static_cast<const Request&>(request_message);
            Response* response = static_cast<Response*>(response_message);
            return func(env, request, response);
        };
        cmd.flag = flag;
    }

    static void RegisterModuleConfig(Module module, CustomConfigBuilderFunc config_builder_func) {
        BYTE_ASSERT(Module_IsValid(module));
        uint16_t module_id = static_cast<uint16_t>(module);
        if (module_id >= Instance().modules_.size()) {
            Instance().modules_.resize(module_id + 1);
        }
        ModuleInfo& module_info = Instance().modules_[module_id];
        module_info.config_builder_func = config_builder_func;
    }

    static const std::vector<ModuleInfo>& GetModuleInfos() { return Instance().modules_; }

    static const CmdInfo* GetCmd(uint32_t cmd_id) {
        uint16_t module_id = GetModuleId(cmd_id);
        uint16_t function_id = GetFunctionId(cmd_id);
        return GetCmd(module_id, function_id);
    }

    static const CmdInfo* GetCmd(uint16_t module_id, uint16_t function_id) {
        if (UNLIKELY(module_id >= Instance().modules_.size())) {
            return nullptr;
        }
        const ModuleInfo& module = Instance().modules_[module_id];
        if (UNLIKELY(function_id >= module.cmd_executors.size())) {
            return nullptr;
        }
        if (UNLIKELY(module.cmd_executors[function_id].request_builder == nullptr)) {
            return nullptr;
        }
        return &module.cmd_executors[function_id];
    }

 private:
    CmdManager() {}
    ~CmdManager() = default;

    std::vector<ModuleInfo> modules_;

    DISALLOW_COPY_AND_ASSIGN(CmdManager);
};

}  // namespace bcache2
