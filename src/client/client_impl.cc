// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "client/client_impl.h"

#include <butil/rand_util.h>

#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "client/client.h"
#include "client/command.h"
#include "client/meta_syncer.h"
#include "client/neptune_syncer.h"
#include "client/pipeline_impl.h"
#include "common/cmd_manager.h"
#include "common/consul.h"
#include "common/function_closure.h"
#include "common/logging.h"
#include "client/metrics.h"
#include "common/proto_enhance.h"
#include "common/scoped_invoker.h"
#include "common/sync_closure.h"
#include "common/env.h"

namespace brpc {
namespace policy {
DECLARE_int32(h2_client_connection_window_size);
DECLARE_int32(h2_client_stream_window_size);
}  // namespace policy
}  // namespace brpc

namespace bcache2 {
namespace client {

namespace {

bcache2::EventReplicationMode ToProtoEventReplicationMode(EventReplicationMode mode) {
    switch (mode) {
    case EventReplicationMode::kAsyncStorage:
        return bcache2::EVENT_REPLICATION_ASYNC_STORAGE;
    case EventReplicationMode::kSyncStorage:
        return bcache2::EVENT_REPLICATION_SYNC_STORAGE;
    case EventReplicationMode::kRaft:
        return bcache2::EVENT_REPLICATION_RAFT;
    case EventReplicationMode::kInherit:
    default:
        return bcache2::EVENT_REPLICATION_INHERIT;
    }
}

}  // namespace

#define SYNC_CALL_CMD(method, ...)         \
    do {                                   \
        Controller ctrl;                   \
        CoSyncClosure sync;                \
        method(&ctrl, __VA_ARGS__, &sync); \
        sync.Wait();                       \
        return ctrl.status();              \
    } while (false);

#define CALLBACK(req, resp)                                                  \
    std::unique_ptr<Request> ScopedRequest(req);                             \
    std::unique_ptr<Response> ScopedResponse(resp);                          \
    do {                                                                     \
        Status status = Status::FromRpcStatus(resp->output->status());       \
        if (status.ok()) {                                                   \
            status = Status::FromRpcStatus(resp->output->response_status()); \
        }                                                                    \
        if (!status.ok()) {                                                  \
            return;                                                          \
        }                                                                    \
    } while (false)

Status Table::Del(const std::string& key) { return Del(key, RequestOptions()); }

Status Table::Expire(const std::string& key, uint64_t ttl) {
    return Expire(key, ttl, RequestOptions());
}

Status Table::Ttl(const std::string& key, uint64_t* ttl) { return Ttl(key, ttl, RequestOptions()); }

Status Table::SetEx(const std::string& key, const std::string& value, uint64_t ttl) {
    return SetEx(key, value, ttl, RequestOptions());
}

Status Table::Get(const std::string& key, std::string* value) {
    return Get(key, value, RequestOptions());
}

Status Table::Set(const std::string& key, const std::string& value) {
    return Set(key, value, RequestOptions());
}

Status Table::HGet(const std::string& key, const std::string& field, std::string* value) {
    return HGet(key, field, value, RequestOptions());
}

Status Table::HSet(const std::string& key, const std::string& field, const std::string& value) {
    return HSet(key, field, value, RequestOptions());
}

Status Table::HDel(const std::string& key, const std::string& field) {
    return HDel(key, field, RequestOptions());
}

Status Client::Create(const ClientOptions& options, Client** client) {
    // we need larger window size to receive GetTableTopoResponse
    // TODO(wangtai.10): remove after all clsuters upgrade to metaserver V2
    brpc::policy::FLAGS_h2_client_connection_window_size = 100 * 1024 * 1024;
    brpc::policy::FLAGS_h2_client_stream_window_size = 100 * 1024 * 1024;

    *client = new ClientImpl();
    return dynamic_cast<ClientImpl*>(*client)->Init(options);
}

ClientImpl::ClientImpl() {}
ClientImpl::~ClientImpl() { metrics_env_->Stop(); }

Status ClientImpl::Init(const ClientOptions& options) {
    options_ = options;

    // Init Logger
    byte::SetByteLogDir(options_.log_dir);
    byte::SetMinLogLevel(byte::LogLevel(options_.log_level));
    std::string brpc_log_path = options_.log_dir + "client_brpc.log";

    if (options_.master_addr.empty() && options_.master_consul.empty()) {
        LOG_ERROR("Client option master_addr and master_consul empty!");
        return Status::InvalidArgument("master_addr and master_consul empty");
    }
    if (options_.psm.empty()) {
        char* tce_psm = std::getenv("TCE_PSM");
        if (tce_psm != nullptr) {
            options_.psm = std::string(tce_psm);
        }
        if (options_.psm.empty()) {
            options_.psm = "bcache2.client.unknown";
        }
    }

    if (options_.cluster.empty()) {
        char* tce_cluster = std::getenv("TCE_CLUSTER");
        if (tce_cluster != nullptr) {
            options_.cluster = std::string(tce_cluster);
        }
        if (options_.cluster.empty()) {
            options_.cluster = "default";
        }
    }

    if (options_.idc.empty()) {
        options_.idc = IDC();
    }

    if (options_.idc.empty()) {
        LOG_ERROR("Options_.idc empty!");
        return Status::InvalidArgument("options_.idc empty");
    }

    PartitionPickOptions& pick_opts = options_.partition_pick_opts;
    if (pick_opts.affinity_vdc.empty()) {
        pick_opts.affinity_vdc = options_.idc;
    }

    // Init Metrics Env
    MetricsEnv::Options metrics_env_option;
    metrics_env_option.prefix = "bcache2.client";
    metrics_env_option.common_tags = {
        {"psm", options_.psm}, {"idc", options_.idc},
        {"client_version", BCACHE2_VERSION}};
    metrics_env_ = std::make_shared<MetricsEnv>();
    metrics_env_->Init(metrics_env_option);


    MetaSyncer::Options meta_syncer_options;
    meta_syncer_options.consul = options_.master_consul;
    meta_syncer_options.timer_interval_ms = options_.meta_sync_interval_ms;
    meta_syncer_options.standalone_interval_delta_ms = options_.topo_error_retry_interval_ms;
    meta_syncer_options.host = options_.host;
    meta_syncer_options.idc = options_.idc;
    meta_syncer_options.endpoint = options_.master_addr;
    meta_syncer_options.client_version = BCACHE2_VERSION;
    meta_syncer_options.meta_fetch_timeout_ms = options_.meta_fetch_timeout_ms;
    meta_syncer_.reset(new MetaSyncer(meta_syncer_options));


    NeptuneSyncer::Options neptune_syncer_options;
    neptune_syncer_options.timer_interval_ms = 1000 * 60;
    neptune_syncer_.reset(new NeptuneSyncer(neptune_syncer_options,
                &options_));

    LOG_INFO("Client Option")
    .put("Psm", options_.psm)
    .put("Cluster", options_.cluster)
    .put("Bcache2 Psm", options_.bcache2_psm)
    .put("Host", options_.host)
    .put("IDC", options_.idc)
    .put("Log_dir", options_.log_dir)
    .put("Meta_sync_interval_ms", options_.meta_sync_interval_ms)
    .put("Topo_error_retry_interval_ms",
        options_.topo_error_retry_interval_ms)
    .put("Meta_fetch_timeout_ms", options_.meta_fetch_timeout_ms)
    .put("Master_consul", options_.master_consul)
    .put("Master_addr", options_.master_addr)
    .put("Affinity_vdc", options_.partition_pick_opts.affinity_vdc);

    LOG_INFO("Meta Syncer Option")
    .put("Consul", meta_syncer_options.consul)
    .put("Timer_interval_ms", meta_syncer_options.timer_interval_ms)
    .put("Standalone_interval_delta_ms", meta_syncer_options.standalone_interval_delta_ms)
    .put("Host", meta_syncer_options.host)
    .put("Idc", meta_syncer_options.idc)
    .put("Endpoint", meta_syncer_options.endpoint)
    .put("Client_version", meta_syncer_options.client_version)
    .put("Meta_fetch_timeout_ms", meta_syncer_options.meta_fetch_timeout_ms);

    LOG_INFO("Neptune Syncer option")
    .put("Time interval", neptune_syncer_options.timer_interval_ms);

    Status status = neptune_syncer_->Init();
    if (!status.ok()) {
        LOG_ERROR("Neptune init failed");
        return status;
    }

    return meta_syncer_->Init();
}

Status ClientImpl::OpenTable(const std::string& name_space, const std::string& table_name,
                             const TableOptions& options, Table** table) {
    std::lock_guard<bthread::Mutex> lock(mutex_);

    std::string combine_name = TableCore::GetTableCombineName(name_space, table_name);
    auto iter = table_map_.find(combine_name);
    if (iter != table_map_.end()) {
        *table = iter->second;
        return Status::OK();
    }

    TableImpl* table_impl =
        new TableImpl(name_space, table_name, options, &options_,
                meta_syncer_.get(), neptune_syncer_.get());
    Status status = meta_syncer_->OpenTable(table_impl);
    if (!status.ok()) {
        LOG_ERROR("MetaSyncer open table failed")
            .put("Table", combine_name)
            .put("Msg", status.ToString());
        return status;
    }

    table_map_.insert(std::make_pair(combine_name, table_impl));
    *table = table_impl;
    LOG_INFO("Client open table success").put("Table", combine_name);
    return Status::OK();
}

Status ClientImpl::CloseTable(Table* table) {
    std::lock_guard<bthread::Mutex> lock(mutex_);

    TableImpl* table_impl = dynamic_cast<TableImpl*>(table);
    std::string table_name = table_impl->GetTableCombineName();
    auto iter = table_map_.find(table_name);
    if (iter == table_map_.end()) {
        LOG_INFO("Table not found").put("Table", table_name);
        return Status::NotFound("Table not found");
    }

    meta_syncer_->CloseTable(table_impl);
    table_map_.erase(iter);
    LOG_DEBUG("Client close table success").put("Table", table_name);

    return Status::OK();
}

TableCore::TableCore(const std::string& name_space, const std::string& table_name,
                     const TableOptions& options, const ClientOptions* client_opts,
                     MetaSyncer* meta_syncer_ptr, NeptuneSyncer* neptune_syncer_ptr)
    : name_space_(name_space),
      table_name_(table_name),
      options_(options),
      client_opts_(client_opts),
      meta_syncer_ptr_(meta_syncer_ptr),
      neptune_syncer_ptr_(neptune_syncer_ptr) {
    gen_.seed(rd_());
    combine_name_ = GetTableCombineName(name_space_, table_name_);
    metrics_manager_.reset(new MetricsManager({{"table_name", combine_name_}}, "table"));
    CommandManager::Options command_manager_options;
    command_manager_options.metrics_manager = metrics_manager_.get();
    cmd_manager_.reset(new CommandManager(command_manager_options));
    cmd_metrics_.Init(metrics_manager_.get());
    batch_request_metrics_.reset(
        new RequestMetrics(metrics_manager_.get(), kMetricsBatchRequest, {}));
    batch_size_metric_ = metrics_manager_->Get<MetricsEnv::Histogram>(kMetricsBatchSize, {});

    BackendServerPool::Options pool_options;
    pool_options.type = BackendServerPool::ServerType::kBCache2;
    pool_options.channel_options.max_retry = 0;
    pool_options.channel_options.timeout_ms = options.io_timeout_ms;
    pool_options.channel_options.connect_timeout_ms = options.connect_timeout_ms;
    auto init_server_pool_fn = [&](BackendServerPool& server_pool) {
        server_pool.Init(pool_options);
        return true;
    };
    server_pool_.Modify(init_server_pool_fn);
}

TableCore::TableCore(const TableCore& table_core)
    : TableCore(table_core.name_space_, table_core.table_name_, table_core.options_,
                table_core.client_opts_, table_core.meta_syncer_ptr_,
                table_core.neptune_syncer_ptr_) {}

std::string TableCore::GetTableCombineName(const std::string& name_space,
                                           const std::string& table_name) {
    return name_space + "/" + table_name;
}

std::string TableCore::Endpoint2String(const Endpoint& ep) {
    if (client_opts_->af == AddressFamily::kIp4) {
        return ep.ip4() + ":" + std::to_string(ep.port());
    }
    return ep.ip6() + ":" + std::to_string(ep.port());
}

Status TableCore::UpdateTopo(const GetTableTopoResponse& topo) {
    LOG_DEBUG("Table update topom").put("Table", combine_name_);
    // must add server before changing the router
    std::set<std::string> servers;
    for (auto iter = topo.partitions().begin(); iter < topo.partitions().end(); ++iter) {
        if (iter->load_infos_size() > 0) {
            for (auto server_iter = iter->load_infos().begin();
                 server_iter < iter->load_infos().end(); ++server_iter) {
                if (server_iter->has_endpoint()) {
                    // metaserver v2 scenario
                    servers.insert(Endpoint2String(server_iter->endpoint()));
                } else {
                    // alchemy configserver scenario
                    servers.insert(server_iter->host() + ":" + std::to_string(server_iter->port()));
                }
            }
        } else if (iter->compressed_load_infos_size() > 0) {
            // metaserver v2 scenario
            for (auto server_iter = iter->compressed_load_infos().begin();
                 server_iter < iter->compressed_load_infos().end(); ++server_iter) {
                Endpoint ep;
                Status status = ToEndpoint(server_iter->endpoint2(), &ep);
                BYTE_ASSERT(status.ok());
                servers.insert(Endpoint2String(ep));
            }
        }
    }
    auto update_server_pool_fn = [&](BackendServerPool& server_pool) {
        Status status = server_pool.AddServers(servers);
        return status.ok();
    };
    if (!server_pool_.Modify(update_server_pool_fn)) {
        LOG_WARNING("Update server pool failed").put("Table", combine_name_);
        return Status::Internal("Update server pool failed");
    }

    auto fn = [&](RouterFactory& router) {
        router.SetAddressFamily(client_opts_->af);
        Status status = router.UpdatePartition(topo);
        return status.ok();
    };
    size_t rc = router_.Modify(fn);
    if (!rc) {
        LOG_WARNING("Update router failed").put("Table", combine_name_);
        return Status::Internal("Update router failed");
    }
    auto cleanup_server_pool_fn = [&](BackendServerPool& server_pool) {
        server_pool.CleanupPool(servers);
        return true;
    };
    server_pool_.Modify(cleanup_server_pool_fn);
    LOG_DEBUG("Table update topom success").put("Table", combine_name_);
    return Status::OK();
}

void TableCore::Execute(Controller* ctrl, Request* request, Response* response,
                        Closure<void>* callback, Closure<void>* post_execute,
                        const RequestOptions& option) {
    TimeCost cost;
    std::vector<Request*> requests;
    requests.emplace_back(request);
    std::vector<Response*> responses;
    responses.emplace_back(response);

    auto func = [this, request, response, ctrl, callback, post_execute, cost] {
        LOG_DEBUG("Response callback")
            .put("TraceId", ctrl->trace_id())
            .put("Response", response->output->ShortDebugString())
            .put("Cost", cost.GetElapsedInUs());
        RequestMetrics* metrics = request->cmd_id == 0 ? GetCommand(request->method)->GetMetrics()
                                                       : GetMetric(request->cmd_id);
        ScopedCallback done(callback);
        if (response->output->status().code() != Code::kOK) {
            LOG_ERROR("Status failed")
                .put("TraceId", ctrl->trace_id())
                .put("Response", response->output->ShortDebugString());
            ctrl->set_status(Status::FromRpcStatus(response->output->status()));
        }
        if (LIKELY(metrics != nullptr)) {
            metrics->Set(ctrl->status().ok(), cost.GetElapsedInUs(), request->input.ByteSize(),
                        response->output->ByteSize(), ctrl->status().errorcode());
        }
        if (response->output->response_status().code() != Code::kOK) {
            ctrl->set_status(Status::FromRpcStatus(response->output->response_status()));
        }
        if (post_execute) {
            post_execute->Run();
        }
    };

    BatchExecute(ctrl, requests, responses, NewFuncClosure(func), option);
}

void TableCore::BatchExecute(Controller* ctrl, const std::vector<Request*>& requests,
                             const std::vector<Response*>& responses, Closure<void>* callback,
                             const RequestOptions& option) {
    LOG_DEBUG("BatchExecute call");
    BatchExecuteContext* execute_context = new BatchExecuteContext();
    auto func = [execute_context, callback] {
        LOG_DEBUG("BatchExecute callback");
        std::unique_ptr<BatchExecuteContext> ScopedBatchExecuteContext(execute_context);
        ScopedCallback done(callback);
    };
    execute_context->callback = NewFuncClosure(std::move(func));
    ScopedCallback done(execute_context->callback);

    if (requests.empty()) {
        ctrl->set_status(Status::OK());
        return;
    }

    if (requests.size() != responses.size()) {
        LOG_WARNING("Batch request size check failed")
            .put("Request size", requests.size())
            .put("Response size", responses.size());
        ctrl->set_status(Status::Internal("Batch request size check failed"));
        return;
    }

    butil::DoublyBufferedData<RouterFactory>::ScopedPtr router_factory_ptr;
    int rc = router_.Read(&router_factory_ptr);
    if (rc != 0) {
        LOG_WARNING("Read router failed").put("Table", combine_name_);
        ctrl->set_status(Status::Internal("Read router failed"));
        return;
    }
    const Router* router_ptr = router_factory_ptr->Get();

    const auto& drop_pct = neptune_syncer_ptr_->Get_Drop_PCT();

    // partition_id : server_endpoint : requests
    std::unordered_map<uint64_t, std::unordered_map<std::string, BCache2ExecuteContext*>>
        server_requests;
    bool force_primary = false;
    for (size_t i = 0; i < requests.size(); ++i) {
        if (requests[i]->cmd_id == 0) {
            Command* command = cmd_manager_->GetCommand(requests[i]->method);
            force_primary = command->IsWrite();
        } else {
            const CmdManager::CmdInfo* command = CmdManager::GetCmd(requests[i]->cmd_id);
            force_primary = command == nullptr || command->flag == CmdRwFlag::kWrite;
        }
        PartitionPickOptions pick_opts = client_opts_->partition_pick_opts;
        if (force_primary) {
            pick_opts.policy = PartitionPickOptions::Policy::kPrimary;
        } else {
            pick_opts.affinity_vdc = neptune_syncer_ptr_->next(client_opts_->idc);
            LOG_DEBUG("Next DC").put("affinity_vdc ", pick_opts.affinity_vdc);
            // now ad just need read drop(they can stop write)
            // TODO(guogaofeng): support drop write
            if (drop_pct > 0) {
                   if (static_cast<uint64_t>(butil::fast_rand_in(0, 100)) < drop_pct) {
                        LOG_INFO_SAMPLE("Neptune will drop Read Req")
                        .put("Drop Pct", drop_pct);
                        ctrl->set_status(Status::Internal("Neptune will drop Read Req"));
                        return;
                   }
            }
        }
        // TODO(wuzhenyu) refactor
        // - circuit-break and health check
        // - retry
        // - backup request
        // - partition pick policy
        uint64_t partition_id = 0;
        std::string server_endpoint;
        Status status = router_ptr->GetServerEndpoint(requests[i]->key, pick_opts, &server_endpoint,
                                                      &partition_id);
        if (!status.ok()) {
            meta_syncer_ptr_->StandaloneMode(this);
            LOG_WARNING("BatchExecute get server failed")
                .put("Key", requests[i]->key)
                .put("Command", requests[i]->cmd_id)
                .put("Method", static_cast<int>(requests[i]->method));
            ctrl->set_status(status);
            return;
        }
        BYTE_ASSERT(!server_endpoint.empty());
        uint64_t version = router_ptr->GetVersion();
        LOG_DEBUG("BatchExecute request")
            .put("Index", i)
            .put("Partition", partition_id)
            .put("Endpoint", server_endpoint);

        auto iter = server_requests.find(partition_id);
        if (iter == server_requests.end()) {
            iter = server_requests
                       .emplace(partition_id,
                                std::unordered_map<std::string, BCache2ExecuteContext*>{})
                       .first;
        }

        auto requests_iter = iter->second.find(server_endpoint);
        if (requests_iter == iter->second.end()) {
            uint64_t trace_id = ((option.trace_id == 0) ? GenRandom() : option.trace_id);
            uint64_t timeout_ms =
                (ctrl->timeout_ms() == 0) ? options_.io_timeout_ms : ctrl->timeout_ms();
            std::unique_ptr<BCache2ExecuteContext> backend_context(
                new BCache2ExecuteContext(this, execute_context, trace_id, timeout_ms,
                                          option.event_replication_mode));
            backend_context->batch_request_.set_partition_id(partition_id);
            backend_context->batch_request_.set_load_version(version);
            backend_context->batch_request_.set_pin_primary(force_primary);
            backend_context->endpoint_ = server_endpoint;
            backend_context->batch_response_.reset(new BatchExecuteCmdResponse());

            iter->second.insert(std::make_pair(server_endpoint, backend_context.get()));
            execute_context->requests.emplace_back(std::move(backend_context));
            requests_iter = iter->second.find(server_endpoint);
        }

        requests_iter->second->batch_request_.mutable_request()->AddAllocated(&requests[i]->input);
        // responses[i]->status = Status::OK();
        requests_iter->second->responses_.emplace_back(responses[i]);
    }

    done.Release();

    for (auto same_partition_requests : server_requests) {
        for (auto backend_request : same_partition_requests.second) {
            backend_request.second->Deliver();
        }
    }
}

TableCore::BCache2ExecuteContext::BCache2ExecuteContext(TableCore* table,
                                                        BatchExecuteContext* context,
                                                        uint64_t trace_id, uint64_t timeout_ms,
                                                        EventReplicationMode event_replication_mode)
    : trace_id_(trace_id), table_(table), context_(context) {
    auto opt = batch_request_.mutable_opt();
    opt->set_trace_id(trace_id_);
    opt->set_event_replication_mode(ToProtoEventReplicationMode(event_replication_mode));
    opt->set_version(BCACHE2_VERSION);
    cntl_.set_timeout_ms(timeout_ms);
}

TableCore::BCache2ExecuteContext::~BCache2ExecuteContext() {
    size_t request_size = batch_request_.mutable_request()->size();
    for (size_t i = 0; i < request_size; ++i) {
        batch_request_.mutable_request()->ReleaseLast();
    }
}

void TableCore::BCache2ExecuteContext::OnResponse(bool success) {
    if (UNLIKELY(channel_context_ == nullptr)) {
        return;
    }
    if (LIKELY(success)) {
        channel_context_->failed_counter = 0;
    } else {
        if (channel_context_->failed_counter++ == 0) {
            channel_context_->first_failed_time_ms = butil::gettimeofday_ms();
        } else if (channel_context_->first_failed_time_ms +
                       table_->options_.continuous_failed_time_ms <
                   butil::gettimeofday_ms()) {
            channel_context_->failed_counter = 0;
            table_->meta_syncer_ptr_->StandaloneMode(table_);
        }
    }
}

// send requests
void TableCore::BCache2ExecuteContext::Deliver() {
    auto* closure = NewClosure(this, &BCache2ExecuteContext::Run);
    ScopedCallback done(closure);

    butil::DoublyBufferedData<BackendServerPool>::ScopedPtr server_pool_ptr;
    if (table_->server_pool_.Read(&server_pool_ptr) != 0) {
        cntl_.SetFailed("Read server pool failed, no send");
        return;
    }

    channel_context_ = server_pool_ptr->GetServer(endpoint_);
    if (channel_context_ == nullptr) {
        cntl_.SetFailed("Get endpoint channel failed, no send");
        table_->meta_syncer_ptr_->StandaloneMode(table_);
        return;
    }
    done.Release();
    delete closure;

    LOG_DEBUG("BCache2 deliver").put("Endpoint", endpoint_).put("Timeout", cntl_.timeout_ms());
    bcache2::ServerService_Stub stub(&channel_context_->channel);
    stub.BatchExecuteCmd(&cntl_, &batch_request_, batch_response_.get(), this);
}

void TableCore::BCache2ExecuteContext::Run() {
    auto func = [this] {
        {
            if (++(context_->done) < context_->requests.size()) {
                return;
            }
        }
        ScopedCallback done(context_->callback);
    };
    ScopedCallback done(NewFuncClosure(std::move(func)));
    LOG_DEBUG("BCache2 callback").put("Endpoint", endpoint_);

    if (cntl_.Failed()) {
        LOG_WARNING("Request server failed")
            .put("Error", cntl_.ErrorText())
            .put("Table", table_->combine_name_)
            .put("Endpoint", endpoint_)
            .put("Traceid", trace_id_);
        for (size_t i = 0; i < responses_.size(); ++i) {
            std::string error_msg =
                "Request server failed" + cntl_.ErrorText() + " endpoint=" + endpoint_;
            responses_[i]->output->mutable_status()->CopyFrom(
                Status::Internal(error_msg).ToRpcStatus());
        }
        OnResponse(false);
        return;
    }
    if (batch_response_->status().code() != Code::kOK) {
        LOG_WARNING("Request server resp failed")
            .put("Code", batch_response_->status().code())
            .put("Msg", batch_response_->status().message())
            .put("Table", table_->combine_name_)
            .put("Endpoint", endpoint_)
            .put("Version", batch_request_.load_version())
            .put("Traceid", trace_id_);

        if (batch_response_->status().code() == Code::kTopomError) {
            table_->meta_syncer_ptr_->StandaloneMode(table_);
        } else {
            OnResponse(false);
        }
        for (size_t i = 0; i < responses_.size(); ++i) {
            responses_[i]->output->mutable_status()->CopyFrom(
                Status::Internal("Request server resp failed").ToRpcStatus());
        }
        return;
    }

    OnResponse(true);

    auto resp_size = batch_response_->mutable_response()->size();
    if (resp_size != static_cast<int>(responses_.size())) {
        LOG_WARNING("Request server resp size check failed")
            .put("Request", responses_.size())
            .put("Response", resp_size)
            .put("Table", table_->combine_name_)
            .put("Endpoint", endpoint_)
            .put("Traceid", trace_id_);
        for (size_t i = 0; i < responses_.size(); ++i) {
            responses_[i]->output->mutable_status()->CopyFrom(
                Status::Internal("Request server resp size check failed").ToRpcStatus());
        }
        return;
    }

    for (int i = 0; i < resp_size; ++i) {
        responses_[i]->output.reset(batch_response_->mutable_response(i));
        if (batch_response_->mutable_response(i)->status().code() != Code::kOK) {
            LOG_WARNING("Request server execute failed")
                .put("Endpoint", endpoint_)
                .put("Msg", batch_response_->mutable_response(i)->status().message())
                .put("Traceid", trace_id_);
            responses_[i]->output->mutable_status()->CopyFrom(
                batch_response_->mutable_response(i)->status());
        }
    }
    for (int i = 0; i < resp_size; ++i) {
        batch_response_->mutable_response()->ReleaseLast();
    }
}

Status TableImpl::OpenPipeline(Pipeline** pipeline) {
    Pipeline* pipeline_impl =
        new PipelineImpl(this, GetBatchRequestMetrics(), GetBatchSizeMetric());
    *pipeline = pipeline_impl;
    LOG_DEBUG("Table open pipeline success");
    return Status::OK();
}

Status TableImpl::HGet(const std::string& key, const std::string& field, std::string* value,
                       const RequestOptions& option) {
    SYNC_CALL_CMD(AsyncHGet, key, field, value, option);
}

Status TableImpl::HSet(const std::string& key, const std::string& field, const std::string& value,
                       const RequestOptions& option) {
    SYNC_CALL_CMD(AsyncHSet, key, field, value, option);
}

Status TableImpl::HDel(const std::string& key, const std::string& field,
                       const RequestOptions& option) {
    SYNC_CALL_CMD(AsyncHDel, key, field, option);
}

Status TableImpl::Get(const std::string& key, std::string* value, const RequestOptions& option) {
    SYNC_CALL_CMD(AsyncGet, key, value, option);
}

Status TableImpl::Set(const std::string& key, const std::string& value,
                      const RequestOptions& option) {
    SYNC_CALL_CMD(AsyncSet, key, value, option);
}

void TableImpl::AsyncHGet(Controller* ctrl, const std::string& key, const std::string& field,
                          std::string* value, const RequestOptions& option,
                          Closure<void>* callback) {
    Request* req = new Request();
    Response* resp = new Response();
    req->method = Command::OpType::kOpHashGet;

    req->input.mutable_hash_request()->mutable_get_request()->set_key(key);
    req->input.mutable_hash_request()->mutable_get_request()->set_field(field);
    req->key = key;

    auto func = [req, resp, value] {
        CALLBACK(req, resp);
        *value = resp->output->hash_response().get_response().value();
    };

    Execute(ctrl, req, resp, callback, NewFuncClosure(std::move(func)), option);
}

void TableImpl::AsyncHSet(Controller* ctrl, const std::string& key, const std::string& field,
                          const std::string& value, const RequestOptions& option,
                          Closure<void>* callback) {
    Request* req = new Request();
    Response* resp = new Response();
    req->method = Command::OpType::kOpHashSet;

    req->input.mutable_hash_request()->mutable_set_request()->set_key(key);
    req->input.mutable_hash_request()->mutable_set_request()->set_field(field);
    req->input.mutable_hash_request()->mutable_set_request()->set_value(value);
    req->key = key;

    auto func = [req, resp] { CALLBACK(req, resp); };

    Execute(ctrl, req, resp, callback, NewFuncClosure(std::move(func)), option);
}

void TableImpl::AsyncHDel(Controller* ctrl, const std::string& key, const std::string& field,
                          const RequestOptions& option, Closure<void>* callback) {
    Request* req = new Request();
    Response* resp = new Response();
    req->method = Command::OpType::kOpHashDel;

    req->input.mutable_hash_request()->mutable_del_request()->set_key(key);
    req->input.mutable_hash_request()->mutable_del_request()->set_field(field);
    req->key = key;

    auto func = [req, resp] { CALLBACK(req, resp); };

    Execute(ctrl, req, resp, callback, NewFuncClosure(std::move(func)), option);
}

void TableImpl::AsyncGet(Controller* ctrl, const std::string& key, std::string* value,
                         const RequestOptions& option, Closure<void>* callback) {
    Request* req = new Request();
    Response* resp = new Response();
    req->method = Command::OpType::kOpStringGet;

    req->input.mutable_string_request()->mutable_get_request()->set_key(key);
    req->key = key;

    auto func = [req, resp, value] {
        CALLBACK(req, resp);
        *value = resp->output->string_response().get_response().value();
    };

    Execute(ctrl, req, resp, callback, NewFuncClosure(std::move(func)), option);
}

void TableImpl::AsyncSet(Controller* ctrl, const std::string& key, const std::string& value,
                         const RequestOptions& option, Closure<void>* callback) {
    Request* req = new Request();
    Response* resp = new Response();
    req->method = Command::OpType::kOpStringSet;

    req->input.mutable_string_request()->mutable_set_request()->set_key(key);
    req->input.mutable_string_request()->mutable_set_request()->set_value(value);
    req->key = key;

    auto func = [req, resp] { CALLBACK(req, resp); };

    Execute(ctrl, req, resp, callback, NewFuncClosure(std::move(func)), option);
}

Status TableImpl::Del(const std::string& key, const RequestOptions& option) {
    SYNC_CALL_CMD(AsyncDel, key, option);
}

Status TableImpl::Expire(const std::string& key, uint64_t ttl_ms, const RequestOptions& option) {
    SYNC_CALL_CMD(AsyncExpire, key, ttl_ms, option);
}

Status TableImpl::Ttl(const std::string& key, uint64_t* ttl_ms, const RequestOptions& option) {
    SYNC_CALL_CMD(AsyncTtl, key, ttl_ms, option);
}

Status TableImpl::SetEx(const std::string& key, const std::string& value, uint64_t ttl_ms,
                        const RequestOptions& option) {
    SYNC_CALL_CMD(AsyncSetEx, key, value, ttl_ms, option);
}

void TableImpl::AsyncDel(Controller* ctrl, const std::string& key, const RequestOptions& option,
                         Closure<void>* callback) {
    Request* req = new Request();
    Response* resp = new Response();
    req->method = Command::OpType::kOpDel;

    req->input.mutable_common_request()->mutable_del_object_request()->set_key(key);
    req->key = key;

    auto func = [req, resp] { CALLBACK(req, resp); };

    Execute(ctrl, req, resp, callback, NewFuncClosure(std::move(func)), option);
}

void TableImpl::AsyncExpire(Controller* ctrl, const std::string& key, uint64_t ttl_ms,
                            const RequestOptions& option, Closure<void>* callback) {
    Request* req = new Request();
    Response* resp = new Response();
    req->method = Command::OpType::kOpExpire;

    req->input.mutable_common_request()->mutable_expire_request()->set_key(key);
    req->input.mutable_common_request()->mutable_expire_request()->set_ttl_ms(ttl_ms);
    req->key = key;

    auto func = [req, resp] { CALLBACK(req, resp); };

    Execute(ctrl, req, resp, callback, NewFuncClosure(std::move(func)), option);
}

void TableImpl::AsyncTtl(Controller* ctrl, const std::string& key, uint64_t* ttl_ms,
                         const RequestOptions& option, Closure<void>* callback) {
    Request* req = new Request();
    Response* resp = new Response();
    req->method = Command::OpType::kOpTtl;

    req->input.mutable_common_request()->mutable_ttl_request()->set_key(key);
    req->key = key;

    auto func = [req, resp, ttl_ms] {
        CALLBACK(req, resp);
        *ttl_ms = resp->output->common_response().ttl_response().ttl_ms();
    };

    Execute(ctrl, req, resp, callback, NewFuncClosure(std::move(func)), option);
}

void TableImpl::AsyncSetEx(Controller* ctrl, const std::string& key, const std::string& value,
                           uint64_t ttl_ms, const RequestOptions& option, Closure<void>* callback) {
    Request* req = new Request();
    Response* resp = new Response();
    req->method = Command::OpType::kOpStringSetEx;

    req->input.mutable_string_request()->mutable_setex_request()->set_key(key);
    req->input.mutable_string_request()->mutable_setex_request()->set_value(value);
    req->input.mutable_string_request()->mutable_setex_request()->set_ttl_ms(ttl_ms);
    req->key = key;

    auto func = [req, resp] { CALLBACK(req, resp); };

    Execute(ctrl, req, resp, callback, NewFuncClosure(std::move(func)), option);
}

// Risk批量写入命令
Status TableImpl::RiskHset(const RiskHsetRequest& req, RiskHsetResponse* resp,
                           const RequestOptions& option) {
    SYNC_CALL_CMD(AsyncRiskHset, req, resp, option);
}

Status TableImpl::RiskHquery(const RiskHqueryRequest& req, RiskHqueryResponse* resp,
                             const RequestOptions& option) {
    SYNC_CALL_CMD(AsyncRiskHquery, req, resp, option);
}
Status TableImpl::RiskFolSet(const RiskFolSetRequest& req, RiskFolSetResponse* resp,
                             const RequestOptions& option) {
    SYNC_CALL_CMD(AsyncRiskFolSet, req, resp, option);
}

Status TableImpl::RiskFolQuery(const RiskFolQueryRequest& req, RiskFolQueryResponse* resp,
                               const RequestOptions& option) {
    SYNC_CALL_CMD(AsyncRiskFolQuery, req, resp, option);
}

Status TableImpl::RiskCPCSet(const RiskCPCSetRequest& req, RiskCPCSetResponse* resp,
                             const RequestOptions& option) {
    SYNC_CALL_CMD(AsyncRiskCPCSet, req, resp, option);
}

Status TableImpl::RiskCPCQuery(const RiskCPCQueryRequest& req, RiskCPCQueryResponse* resp,
                               const RequestOptions& option) {
    SYNC_CALL_CMD(AsyncRiskCPCQuery, req, resp, option);
}

Status TableImpl::RiskManager(const RiskManagerRequest& req, RiskManagerResponse* resp,
                              const RequestOptions& option) {
    SYNC_CALL_CMD(AsyncRiskManager, req, resp, option);
}

void TableImpl::AsyncRiskHset(Controller* ctrl, const RiskHsetRequest& risk_hset_req,
                              RiskHsetResponse* risk_hset_resp, const RequestOptions& option,
                              Closure<void>* callback) {
    Request* req = new Request();
    Response* resp = new Response();
    req->key = risk_hset_req.key();
    req->cmd_id = MakeCmdId(bcache2::Module::RISK, bcache2::risk::HSET);
    req->input.set_module_id(bcache2::Module::RISK);
    req->input.set_function_id(bcache2::risk::HSET);
    std::string request_bytes;
    risk_hset_req.SerializeToString(&request_bytes);
    req->input.set_request_bytes(request_bytes);
    auto func = [this, req, resp, risk_hset_resp] { CALLBACK(req, resp); };
    Execute(ctrl, req, resp, callback, NewFuncClosure(std::move(func)), option);
}

void TableImpl::AsyncRiskHquery(Controller* ctrl, const RiskHqueryRequest& risk_hquery_req,
                                RiskHqueryResponse* risk_hquery_resp, const RequestOptions& option,
                                Closure<void>* callback) {
    Request* req = new Request();
    Response* resp = new Response();

    req->key = risk_hquery_req.key();
    req->cmd_id = MakeCmdId(bcache2::Module::RISK, bcache2::risk::HQUERY);
    req->input.set_module_id(bcache2::Module::RISK);
    req->input.set_function_id(bcache2::risk::HQUERY);
    std::string request_bytes;
    risk_hquery_req.SerializeToString(&request_bytes);
    req->input.set_request_bytes(request_bytes);

    CoSyncClosure sync;
    Execute(ctrl, req, resp, callback, &sync, option);
    sync.Wait();
    CALLBACK(req, resp);
    std::string respBytes = resp->output->response_bytes();
    risk_hquery_resp->ParseFromString(respBytes);
}

void TableImpl::AsyncRiskFolSet(Controller* ctrl, const RiskFolSetRequest& risk_fol_req,
                                RiskFolSetResponse* risk_fol_resp, const RequestOptions& option,
                                Closure<void>* callback) {
    Request* req = new Request();
    Response* resp = new Response();

    req->key = risk_fol_req.key();
    req->cmd_id = MakeCmdId(bcache2::Module::RISK, bcache2::risk::FOLSET);
    req->input.set_module_id(bcache2::Module::RISK);
    req->input.set_function_id(bcache2::risk::FOLSET);
    std::string request_bytes;
    risk_fol_req.SerializeToString(&request_bytes);
    req->input.set_request_bytes(request_bytes);

    auto func = [this, req, resp, risk_fol_resp] { CALLBACK(req, resp); };
    Execute(ctrl, req, resp, callback, NewFuncClosure(std::move(func)), option);
}

void TableImpl::AsyncRiskFolQuery(Controller* ctrl, const RiskFolQueryRequest& risk_fol_req,
                                  RiskFolQueryResponse* risk_fol_resp, const RequestOptions& option,
                                  Closure<void>* callback) {
    Request* req = new Request();
    Response* resp = new Response();

    req->key = risk_fol_req.key();
    req->cmd_id = MakeCmdId(bcache2::Module::RISK, bcache2::risk::FOLQUERY);
    req->input.set_module_id(bcache2::Module::RISK);
    req->input.set_function_id(bcache2::risk::FOLQUERY);
    std::string request_bytes;
    risk_fol_req.SerializeToString(&request_bytes);
    req->input.set_request_bytes(request_bytes);

    CoSyncClosure sync;
    Execute(ctrl, req, resp, callback, &sync, option);
    sync.Wait();
    CALLBACK(req, resp);
    std::string respBytes = resp->output->response_bytes();
    risk_fol_resp->ParseFromString(respBytes);
}

void TableImpl::AsyncRiskCPCSet(Controller* ctrl, const RiskCPCSetRequest& risk_cpcset_req,
                                RiskCPCSetResponse* risk_cpcset_resp, const RequestOptions& option,
                                Closure<void>* callback) {
    Request* req = new Request();
    Response* resp = new Response();
    std::string key = risk_cpcset_req.key();
    req->key = risk_cpcset_req.key();
    req->cmd_id = MakeCmdId(bcache2::Module::RISK, bcache2::risk::CPCSET);
    req->input.set_module_id(bcache2::Module::RISK);
    req->input.set_function_id(bcache2::risk::CPCSET);
    std::string request_bytes;
    risk_cpcset_req.SerializeToString(&request_bytes);
    req->input.set_request_bytes(request_bytes);
    auto func = [this, req, resp, risk_cpcset_resp] { CALLBACK(req, resp); };
    Execute(ctrl, req, resp, callback, NewFuncClosure(std::move(func)), option);
}

void TableImpl::AsyncRiskCPCQuery(Controller* ctrl, const RiskCPCQueryRequest& risk_cpcquery_req,
                                  RiskCPCQueryResponse* risk_cpcquery_resp,
                                  const RequestOptions& option, Closure<void>* callback) {
    Request* req = new Request();
    Response* resp = new Response();
    std::string key = risk_cpcquery_req.key();
    req->key = risk_cpcquery_req.key();
    req->cmd_id = MakeCmdId(bcache2::Module::RISK, bcache2::risk::CPCQUERY);
    req->input.set_module_id(bcache2::Module::RISK);
    req->input.set_function_id(bcache2::risk::CPCQUERY);
    std::string request_bytes;
    risk_cpcquery_req.SerializeToString(&request_bytes);
    req->input.set_request_bytes(request_bytes);
    CoSyncClosure sync;
    Execute(ctrl, req, resp, callback, &sync, option);
    sync.Wait();
    CALLBACK(req, resp);
    std::string respBytes = resp->output->response_bytes();
    risk_cpcquery_resp->ParseFromString(respBytes);
}

void TableImpl::AsyncRiskManager(Controller* ctrl, const RiskManagerRequest& risk_manager_req,
                                 RiskManagerResponse* risk_manager_resp,
                                 const RequestOptions& option, Closure<void>* callback) {
    Request* req = new Request();
    Response* resp = new Response();

    req->key = risk_manager_req.key();
    req->cmd_id = MakeCmdId(bcache2::Module::RISK, bcache2::risk::MANAGER);
    req->input.set_module_id(bcache2::Module::RISK);
    req->input.set_function_id(bcache2::risk::MANAGER);
    std::string request_bytes;
    risk_manager_req.SerializeToString(&request_bytes);
    req->input.set_request_bytes(request_bytes);

    CoSyncClosure sync;
    Execute(ctrl, req, resp, callback, &sync, option);
    sync.Wait();
    CALLBACK(req, resp);
    std::string respBytes = resp->output->response_bytes();
    risk_manager_resp->ParseFromString(respBytes);
}

}  // namespace client
}  // namespace bcache2
