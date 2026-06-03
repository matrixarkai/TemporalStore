// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "client/meta_syncer.h"

#include <bthread/timer_thread.h>
#include <butil/containers/doubly_buffered_data.h>
#include <google/protobuf/util/json_util.h>

#include <cstdlib>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bthread/bthread.h"
#include "client/client_impl.h"
#include "client/router.h"
#include "client/server_pool.h"
#include "common/bthread_closure.h"
#include "common/function_closure.h"
#include "common/logging.h"
#include "common/scoped_invoker.h"
#include "common/status.h"
#include "common/sync_closure.h"
#include "protocol/master.pb.h"

namespace bcache2 {
namespace client {

struct LookupTask {
    service_discovery::Consul* consul = nullptr;
    std::string master_consul;
    std::vector<service_discovery::Endpoint> master_endpoints;
};

static void* LookupConfigserver(void* arg) {
    LookupTask* task = static_cast<LookupTask*>(arg);
    Status status = task->consul->Lookup(
        task->master_consul, service_discovery::Consul::AddrFamily::Auto, &task->master_endpoints);
    if (!status.ok()) {
        LOG_WARNING("Failed to lookup").put("Name", task->master_consul).put("Status", status);
    }
    return nullptr;
}

static bool is_idc_internal(const std::string& idc,
                    const std::vector<std::string>& dc_vec) {
    for (const auto& dc : dc_vec) {
        if (idc.compare(dc) == 0) {
            return true;
        }
    }
    return false;
}

Status MetaSyncer::Init() {
    BackendServerPool::Options options;
    options.type = BackendServerPool::ServerType::kMaster;
    options.channel_options.max_retry = 3;
    options.channel_options.protocol = "h2:grpc";
    options.channel_options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
    master_pool_.reset(new BackendServerPool());
    master_pool_->Init(options);

    // metrics
    metrics_manager_.reset(new MetricsManager({}, "metasyncer"));

    bthread::TimerThreadOptions timer_thread_options;
    int ret = timer_thread_.start(&timer_thread_options);
    if (ret != 0) {
        return Status::Internal("Timer thread start failed ");
    }
    MetaSyncSchedule();
    return Status::OK();
}

Status MetaSyncer::OpenTable(TableCore* table) {
    std::lock_guard<bthread::Mutex> lock(mutex_);

    std::string table_name = table->GetTableCombineName();
    auto iter = tables_.find(table_name);
    if (iter != tables_.end()) {
        LOG_WARNING("Table already exists").put("Table", table_name);
        return Status::AlreadyExists("Table already exists in metasyncer");
    }

    TableNode table_node;
    table_node.table = table;

    MetricsEnv::MetricsTags common_tags = {{"table_name", table_name}};
    table_node.request_metrics.reset(
        new RequestMetrics(metrics_manager_.get(), kMetricsMetaSyncerRequest, common_tags));
    table_node.update_metrics =
        metrics_manager_->Get<MetricsEnv::Counter>(kMetricsTopomUpdate, common_tags);

    Status status = UpdateTableMeta(&table_node, options_.max_redirect_times);
    if (!status.ok()) {
        LOG_WARNING("MetaSyncer update meta to open table failed")
            .put("Table", table_name)
            .put("Msg", status.ToString());
        return status;
    }

    tables_.insert(std::make_pair(std::move(table_name), std::move(table_node)));
    LOG_DEBUG("MetaSyncer open table success").put("Table", table_name);
    return Status::OK();
}

Status MetaSyncer::CloseTable(TableCore* table) {
    std::lock_guard<bthread::Mutex> lock(mutex_);

    std::string table_name = table->GetTableCombineName();
    auto iter = tables_.find(table_name);
    if (iter == tables_.end()) {
        LOG_WARNING("Table not found").put("Table", table_name);
        return Status::NotFound("Table not found in metasyncer");
    }

    tables_.erase(iter);
    LOG_DEBUG("MetaSyncer close table success").put("Table", table_name);
    return Status::OK();
}

void MetaSyncer::MetaSyncSchedule() {
    LOG_CALL_DEBUG();
    std::lock_guard<bthread::Mutex> lock(mutex_);

    auto func = [this] {
        auto fn = [](void* data) {
            auto ptr = static_cast<MetaSyncer*>(data);
            ptr->MetaSyncSchedule();
        };
        uint64_t task_id = timer_thread_.schedule(
            fn, reinterpret_cast<void*>(this),
            butil::milliseconds_to_timespec(butil::gettimeofday_ms() + options_.timer_interval_ms));
        BYTE_ASSERT(task_id != 0);
    };
    ScopedCallback done(NewFuncClosure(std::move(func)));

    RefreshMasterPool();
    for (auto& table : tables_) {
        if (!table.second.is_standalone) {
            UpdateTableMeta(&table.second, options_.max_redirect_times);
        } else {
            LOG_INFO("Skip update table in standalone mode")
                .put("Table", table.second.table->GetTableCombineName());
        }
    }
}

void MetaSyncer::RefreshMasterPool() {
    Status status;
    std::string master_consul;
    std::set<std::string> master_addrs;
    std::vector<std::string> dc_vec = { "lf", "hl", "lq", "yg" };
    std::vector<service_discovery::Endpoint> master_endpoints;
    if (!options_.consul.empty()) {
        // update master_addrs from consul
        if ((options_.consul.find(".service.") == std::string::npos) &&
               is_idc_internal(options_.idc, dc_vec)) {
            for (const auto& dc : dc_vec) {
                master_consul = options_.consul + ".service." + dc;
                LOG_DEBUG("Will lookup").put("Name", master_consul);

                status = consul_.Lookup(master_consul,
                    service_discovery::Consul::AddrFamily::Auto, &master_endpoints);
                if (!status.ok()) {
                    LOG_WARNING("Failed to lookup").put("Name",
                        master_consul).put("Status", status);
                    continue;
                }

                for (const auto& endpoint : master_endpoints) {
                    master_addrs.insert(endpoint.host + ":" + std::to_string(endpoint.port - 1000));
                }
                master_endpoints.clear();
            }
        } else {
            status = consul_.Lookup(master_consul,
                service_discovery::Consul::AddrFamily::Auto, &master_endpoints);
            if (!status.ok()) {
                LOG_WARNING("Failed to lookup").put("Name", master_consul).put("Status", status);
            }

            for (const auto& endpoint : master_endpoints) {
                master_addrs.insert(endpoint.host + ":" + std::to_string(endpoint.port - 1000));
            }
            master_endpoints.clear();
        }

        // transform master_endpoints to master_addrs
        // NOTE: configserver RPC port = http port - 1000

        if (master_addrs.empty()) {
            LOG_ERROR("Consul addr empty").put("Consul", options_.consul);
        } else {
            std::string consul_string;  // for logging only
            for (const auto& addr : master_addrs) {
                consul_string += addr + ",";
            }
            LOG_INFO("Refresh consul succeed")
                .put("Consul", options_.consul)
                .put("Addrs", consul_string);
        }
    }
    if (!options_.endpoint.empty()) {
        master_addrs.insert(options_.endpoint);
    }
    if (!master_addrs.empty()) {
        Status status = master_pool_->AddServers(master_addrs);
        if (status.ok()) {
            master_pool_->CleanupPool(master_addrs);
        }
    }
    return;
}

Status MetaSyncer::UpdateTableMeta(TableNode* table_node, int redirect_times) {
    BYTE_ASSERT(table_node != nullptr);
    BYTE_ASSERT(table_node->table != nullptr);
    LOG_DEBUG("Update table meta").put("Table", table_node->table->GetTableCombineName());

    std::shared_ptr<BackendServerPool::ChannelContext> master_channel_context = nullptr;
    if (table_node->master_addr.empty()) {
        master_channel_context = master_pool_->GetServerByRandom();
    } else {
        master_channel_context = master_pool_->GetServer(table_node->master_addr);
    }
    if (master_channel_context == nullptr) {
        LOG_WARNING("Master channel not found").put("Master", table_node->master_addr);
        table_node->master_addr.clear();
        return Status::Internal("Master channel not found");
    }

    GetTableTopoRequest request;
    GetTableTopoResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(options_.meta_fetch_timeout_ms);

    // todo: traceid
    request.mutable_opt()->set_version(options_.client_version);
    request.set_namespace_(table_node->table->GetNameSpace());
    request.set_table_name(table_node->table->GetTableName());
    request.set_old_topo_version(table_node->topo.topo_version());
    request.set_host(options_.host);
    request.set_idc(options_.idc);
    request.set_compress(true);

    bcache2::MasterService_Stub stub(&master_channel_context->channel);
    TimeCost cost;
    stub.GetTableTopo(&cntl, &request, &response, NULL);
    table_node->request_metrics->Set(!cntl.Failed(), cost.GetElapsedInUs(), request.ByteSize(),
                                     response.ByteSize(), kInternal);
    if (cntl.Failed()) {
        table_node->master_addr.clear();
        LOG_WARNING("Master get table topo error")
            .put("Table", table_node->table->GetTableCombineName())
            .put("Msg", cntl.ErrorText())
            .put("Remote", cntl.remote_side());
        return Status::Internal("cntl failed " + cntl.ErrorText());
    }

    std::string response_str;
    google::protobuf::util::MessageToJsonString(response, &response_str);
    LOG_INFO("Get topom response succeed")
        .put("Table", table_node->table->GetTableCombineName())
        .put("Remote", cntl.remote_side());

    if (response.status().code() != Code::kOK) {
        LOG_WARNING("Master get table topo return error")
            .put("Table", table_node->table->GetTableCombineName())
            .put("Code", response.status().code())
            .put("Msg", response.status().message())
            .put("Remote", cntl.remote_side());
        return Status::Internal("Get topom failed " + response.status().message());
    }

    if (!response.redirect_endpoint().empty()) {
        if (redirect_times == 0) {
            table_node->master_addr.clear();
            LOG_ERROR("Master redirect more!")
                .put("Table", table_node->table->GetTableCombineName())
                .put("Remote", cntl.remote_side());
            return Status::Internal("redirect more");
        }
        table_node->master_addr = response.redirect_endpoint();
        master_pool_->AddServer(table_node->master_addr);
        LOG_DEBUG("Master redirect").put("Redirect", table_node->master_addr);
        return UpdateTableMeta(table_node, redirect_times - 1);
    }

    if (!response.serialized_topo().empty()) {
        LOG_DEBUG("got serialized topo, try to unserialized it and overwrite");
        GetTableTopoResponse response2;
        response2.ParseFromString(response.serialized_topo());
        return HandleTableMetaResponse(table_node, std::move(response2));
    }
    return HandleTableMetaResponse(table_node, std::move(response));
}

Status MetaSyncer::HandleTableMetaResponse(TableNode* table_node, GetTableTopoResponse response) {
    if (response.topo_version() <= table_node->topo.topo_version()) {
        LOG_INFO("Skip update topom")
            .put("Table", table_node->table->GetTableCombineName())
            .put("RequestVersion", table_node->topo.topo_version())
            .put("ResponseVersion", response.topo_version());
        return Status::OK();
    }

    Status status = table_node->table->UpdateTopo(response);
    if (!status.ok()) {
        LOG_WARNING("MetaSyncer update table meta failed")
            .put("Table", table_node->table->GetTableCombineName())
            .put("Msg", status.ToString());
        return status;
    }
    table_node->update_metrics->get()->Add(1);

    // table topo must be modified successfully
    table_node->topo = std::move(response);
    table_node->last_update_time_ms = butil::gettimeofday_ms();
    LOG_INFO("Update table topom succeed").put("Table", table_node->table->GetTableCombineName());
    return Status::OK();
}

Status MetaSyncer::StandaloneMode(TableCore* table) {
    std::lock_guard<bthread::Mutex> lock(mutex_);

    std::string table_name = table->GetTableCombineName();
    auto iter = tables_.find(table_name);
    if (iter == tables_.end()) {
        LOG_WARNING("Table not found").put("Table_name", table_name);
        return Status::NotFound("Table not found in metasyncer");
    }
    if (iter->second.is_standalone ||
        iter->second.last_update_time_ms + options_.standalone_interval_delta_ms >
        butil::gettimeofday_ms()) {
        return Status::OK();
    }
    LOG_INFO("Enter into standalone mode").put("Table", table_name);
    iter->second.is_standalone = true;
    iter->second.standalone_interval_ms = 0;
    StandaloneSchedule(table_name, 0);
    return Status::OK();
}

void MetaSyncer::StandaloneSchedule(const std::string& table_name, int64_t interval_ms) {
    struct StandaloneContext {
        MetaSyncer* meta_syncer = nullptr;
        std::string table_name;
    };

    StandaloneContext* context = new StandaloneContext();
    context->meta_syncer = this;
    context->table_name = table_name;

    auto fn = [](void* data) {
        StandaloneContext* ptr = static_cast<StandaloneContext*>(data);
        std::unique_ptr<StandaloneContext> scoped_ptr(ptr);
        ptr->meta_syncer->StandaloneMetaSync(ptr->table_name);
    };
    uint64_t task_id = timer_thread_.schedule(
        fn, reinterpret_cast<void*>(context),
        butil::milliseconds_to_timespec(butil::gettimeofday_ms() + interval_ms));
    BYTE_ASSERT(task_id != 0);
}

void MetaSyncer::StandaloneMetaSync(const std::string& table_name) {
    std::lock_guard<bthread::Mutex> lock(mutex_);
    auto iter = tables_.find(table_name);
    if (iter == tables_.end()) {
        LOG_WARNING("Table not found").put("Table_name", table_name);
        return;
    }
    RefreshMasterPool();
    Status status = UpdateTableMeta(&iter->second, options_.max_redirect_times);
    LOG_INFO("Update table in standalone mode")
        .put("Table", table_name)
        .put("IntervalMs", iter->second.standalone_interval_ms)
        .put("Status", status.ToString());
    if (status.ok()) {
        iter->second.is_standalone = false;
    } else {
        iter->second.standalone_interval_ms += options_.standalone_interval_delta_ms;
        if (iter->second.standalone_interval_ms > options_.timer_interval_ms) {
            iter->second.standalone_interval_ms = options_.timer_interval_ms;
        }
        StandaloneSchedule(table_name, iter->second.standalone_interval_ms);
    }
}

}  // namespace client
}  // namespace bcache2
