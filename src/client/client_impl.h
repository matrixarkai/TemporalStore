// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <butil/containers/doubly_buffered_data.h>
#include <byte/concurrent/mutex.h>

#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bthread/mutex.h"
#include "byte/base/closure.h"
#include "client/client.h"
#include "client/command.h"
#include "client/router_factory.h"
#include "client/server_pool.h"
#include "common/controller.h"
#include "common/status.h"
#include "protocol/server.pb.h"

namespace bcache2 {
namespace client {

class MetaSyncer;
class NeptuneSyncer;

class TableCore {
 public:
    struct Request {
        uint32_t cmd_id = 0;
        Command::OpType method;
        std::string key;
        CmdRequest input;
    };

    struct Response {
        Response() : output(new CmdResponse()) {}

        std::unique_ptr<CmdResponse> output;
    };

    TableCore(const std::string& name_space, const std::string& table_name,
              const TableOptions& options, const ClientOptions* client_opts,
              MetaSyncer* meta_syncer_ptr, NeptuneSyncer* neptune_syncer_ptr);
    TableCore(const TableCore& table_core);
    virtual ~TableCore() {}

    virtual void Execute(Controller* ctrl, Request* request, Response* response,
                         Closure<void>* callback, Closure<void>* post_execute,
                         const RequestOptions& option);
    void BatchExecute(Controller* ctrl, const std::vector<Request*>& request,
                      const std::vector<Response*>& response, Closure<void>* callback,
                      const RequestOptions& option);

    static std::string GetTableCombineName(const std::string& name_space,
                                           const std::string& table_name);
    std::string GetTableCombineName() const { return combine_name_; }
    std::string GetTableName() const { return table_name_; }
    std::string GetNameSpace() const { return name_space_; }

    Command* GetCommand(Command::OpType type) const { return cmd_manager_->GetCommand(type); }
    RequestMetrics* GetMetric(uint32_t cmd_id) const {
        uint16_t module_id = GetModuleId(cmd_id);
        uint16_t function_id = GetFunctionId(cmd_id);
        if (UNLIKELY(module_id >= cmd_metrics_.cmd_metrics.size() ||
            function_id >= cmd_metrics_.cmd_metrics[module_id].size())) {
            LOG_WARNING_SAMPLE("No invaild metrics")
                .put("CmdId", cmd_id)
                .put("ModuleId", module_id);
            return nullptr;
        }
        return cmd_metrics_.cmd_metrics[module_id][function_id].get();
    }

    Status UpdateTopo(const GetTableTopoResponse& topo);
    uint64_t GenRandom() { return gen_(); }
    RequestMetrics* GetBatchRequestMetrics() { return batch_request_metrics_.get(); }
    MetricsEnv::HistogramHolder* GetBatchSizeMetric() { return batch_size_metric_.get(); }

 private:
    std::string Endpoint2String(const Endpoint& ep);

 private:
    class BCache2ExecuteContext;
    friend class BCache2ExecuteContext;

    struct BatchExecuteContext {
        Closure<void>* callback;

        std::atomic<uint32_t> done;
        std::vector<std::unique_ptr<BCache2ExecuteContext>> requests;
    };

    class BCache2ExecuteContext : public google::protobuf::Closure {
     public:
        BCache2ExecuteContext(TableCore* table, BatchExecuteContext* context, uint64_t trace_id,
                              uint64_t timeout_ms);
        ~BCache2ExecuteContext();

        void Deliver();
        void Run();
        void OnResponse(bool success);

     private:
        uint64_t trace_id_;
        friend class TableCore;
        TableCore* table_ = nullptr;
        BatchExecuteContext* context_ = nullptr;
        std::vector<Response*> responses_;

        BatchExecuteCmdRequest batch_request_;
        std::unique_ptr<BatchExecuteCmdResponse> batch_response_;

        std::string endpoint_;
        brpc::Controller cntl_;
        std::shared_ptr<BackendServerPool::ChannelContext> channel_context_ = nullptr;
    };

    std::random_device rd_;
    std::mt19937_64 gen_;

    std::string name_space_;
    std::string table_name_;
    TableOptions options_;
    const ClientOptions* client_opts_;
    // TODO(zhangfucheng.0): remove interdependence with MetaSyncer
    MetaSyncer* meta_syncer_ptr_ = nullptr;
    NeptuneSyncer* neptune_syncer_ptr_ = nullptr;

    std::string combine_name_;
    std::unique_ptr<MetricsManager> metrics_manager_;
    std::unique_ptr<CommandManager> cmd_manager_;
    std::unique_ptr<RequestMetrics> batch_request_metrics_;
    std::unique_ptr<MetricsEnv::HistogramHolder> batch_size_metric_;
    CmdMetrics cmd_metrics_;

    AddressFamily af_;
    butil::DoublyBufferedData<RouterFactory> router_;
    butil::DoublyBufferedData<BackendServerPool> server_pool_;
};

class TableImpl : public Pipeline, public TableCore {
 public:
    TableImpl(const std::string& name_space, const std::string& table_name,
              const TableOptions& options, const ClientOptions* client_opts,
              MetaSyncer* meta_syncer_ptr, NeptuneSyncer* neptune_syncer_ptr)
        : TableCore(name_space, table_name, options, client_opts,
                    meta_syncer_ptr, neptune_syncer_ptr) {}
    explicit TableImpl(const TableImpl* table) : TableCore(*table) {}
    ~TableImpl() {}

    Status OpenPipeline(Pipeline** pipeline) override;

    Status HGet(const std::string& key, const std::string& field, std::string* value,
                const RequestOptions& option) override;
    Status HSet(const std::string& key, const std::string& field, const std::string& value,
                const RequestOptions& option) override;
    Status HDel(const std::string& key, const std::string& field,
                const RequestOptions& option) override;
    Status Get(const std::string& key, std::string* value, const RequestOptions& option) override;
    Status Set(const std::string& key, const std::string& value,
               const RequestOptions& option) override;
    Status Del(const std::string& key, const RequestOptions& option) override;
    Status Expire(const std::string& key, uint64_t ttl, const RequestOptions& option) override;
    Status Ttl(const std::string& key, uint64_t* ttl, const RequestOptions& option) override;
    Status SetEx(const std::string& key, const std::string& value, uint64_t ttl,
                 const RequestOptions& option) override;

    void AsyncDel(Controller* ctrl, const std::string& key, const RequestOptions& option,
                  Closure<void>* callback);
    void AsyncExpire(Controller* ctrl, const std::string& key, uint64_t ttl,
                     const RequestOptions& option, Closure<void>* callback);
    void AsyncTtl(Controller* ctrl, const std::string& key, uint64_t* ttl,
                  const RequestOptions& option, Closure<void>* callback);
    void AsyncSetEx(Controller* ctrl, const std::string& key, const std::string& value,
                    uint64_t ttl, const RequestOptions& option, Closure<void>* callback);
    void AsyncHGet(Controller* ctrl, const std::string& key, const std::string& field,
                   std::string* value, const RequestOptions& option, Closure<void>* callback);
    void AsyncHSet(Controller* ctrl, const std::string& key, const std::string& field,
                   const std::string& value, const RequestOptions& option, Closure<void>* callback);
    void AsyncHDel(Controller* ctrl, const std::string& key, const std::string& field,
                   const RequestOptions& option, Closure<void>* callback);
    void AsyncGet(Controller* ctrl, const std::string& key, std::string* value,
                  const RequestOptions& option, Closure<void>* callback);
    void AsyncSet(Controller* ctrl, const std::string& key, const std::string& value,
                  const RequestOptions& option, Closure<void>* callback);

    // 以下命令为：字节电商平台治理相关的操作命令
    Status RiskHset(const RiskHsetRequest& req, RiskHsetResponse* resp,
                    const RequestOptions& option);
    Status RiskHquery(const RiskHqueryRequest& req, RiskHqueryResponse* resp,
                      const RequestOptions& option);

    Status RiskFolSet(const RiskFolSetRequest& req, RiskFolSetResponse* resp,
                      const RequestOptions& option);

    Status RiskFolQuery(const RiskFolQueryRequest& req, RiskFolQueryResponse* resp,
                        const RequestOptions& option);
    Status RiskCPCSet(const RiskCPCSetRequest& req, RiskCPCSetResponse* resp,
                      const RequestOptions& option);
    Status RiskCPCQuery(const RiskCPCQueryRequest& req, RiskCPCQueryResponse* resp,
                        const RequestOptions& option);
    Status RiskManager(const RiskManagerRequest& req, RiskManagerResponse* resp,
                       const RequestOptions& option);
    void AsyncRiskHset(Controller* ctrl, const RiskHsetRequest& req, RiskHsetResponse* resp,
                       const RequestOptions& option, Closure<void>* callback);
    void AsyncRiskHquery(Controller* ctrl, const RiskHqueryRequest& risk_hquery_req,
                         RiskHqueryResponse* risk_hquery_resp, const RequestOptions& option,
                         Closure<void>* callback);
    void AsyncRiskFolSet(Controller* ctrl, const RiskFolSetRequest& req, RiskFolSetResponse* resp,
                         const RequestOptions& option, Closure<void>* callback);
    void AsyncRiskFolQuery(Controller* ctrl, const RiskFolQueryRequest& req,
                           RiskFolQueryResponse* resp, const RequestOptions& option,
                           Closure<void>* callback);
    void AsyncRiskCPCSet(Controller* ctrl, const RiskCPCSetRequest& req, RiskCPCSetResponse* resp,
                         const RequestOptions& option, Closure<void>* callback);
    void AsyncRiskCPCQuery(Controller* ctrl, const RiskCPCQueryRequest& req,
                           RiskCPCQueryResponse* resp, const RequestOptions& option,
                           Closure<void>* callback);
    void AsyncRiskManager(Controller* ctrl, const RiskManagerRequest& req,
                          RiskManagerResponse* resp, const RequestOptions& option,
                          Closure<void>* callback);
};

class ClientImpl : public Client {
 public:
    ClientImpl();
    virtual ~ClientImpl();

    Status Init(const ClientOptions& options);
    Status OpenTable(const std::string& name_space, const std::string& table_name,
                     const TableOptions& options, Table** table) override;
    Status CloseTable(Table* table) override;

 private:
    ClientOptions options_;

    mutable bthread::Mutex mutex_;
    std::unordered_map<std::string, TableImpl*> table_map_;
    std::unique_ptr<MetaSyncer> meta_syncer_;
    std::unique_ptr<NeptuneSyncer> neptune_syncer_;
    std::shared_ptr<MetricsEnv> metrics_env_;

    DISALLOW_COPY_AND_ASSIGN(ClientImpl);
};

}  // namespace client
}  // namespace bcache2
