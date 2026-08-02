// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "client/swig_client.h"

#include <byte/include/macros.h>
#include <memory>
#include <regex>

#include "client/bcache2.h"
#include "client/client_impl.h"
#include "common/logging.h"
#include "common/coclosure.h"
#include "common/scoped_invoker.h"
#include "common/string_utils.h"

namespace bcache2 {
namespace swig {
namespace {

class ConsoleLogger : public byte::Logger {
 public:
    explicit ConsoleLogger(byte::LogLevel level) : byte::Logger(level) {}

    void Write(const char* data, int length) override {
        fprintf(stderr, "LogLevel %d:\n%s", level_, std::string(data, length).c_str());
    }

    void Flush() override {}

    void Close() override {}
};

}  // namespace

struct ExecutionContext : public Controller {
    std::vector<client::TableCore::Request> raw_requests;
    std::vector<client::TableCore::Response> raw_responses;
    std::vector<client::TableCore::Request*> requests;
    std::vector<client::TableCore::Response*> responses;

    bcache2::Controller internal_ctrl;
    TimeCost cost;
    Closure<void>* callback = nullptr;
};

class TableImpl : public Table {
 public:
    TableImpl(client::Client* client, client::TableCore* table) : client_(client), table_(table) {}
    virtual ~TableImpl() { client_->CloseTable(dynamic_cast<client::Table*>(table_.get())); }

    void BatchExecute(Controller* ctrl, std::vector<Execution>* executions) override {
        ExecutionContext context;
        context.timeout_ms = ctrl->timeout_ms;
        context.trace_id = ctrl->trace_id;
        BatchExecute(&context, executions, nullptr);
        ctrl->status = context.status;
    }

    void BatchExecute(ExecutionContext* context, std::vector<Execution>* executions,
                      Closure<void>* callback) {
        context->raw_requests.resize(executions->size());
        context->raw_responses.resize(executions->size());
        context->requests.resize(executions->size());
        context->responses.resize(executions->size());

        // build batch request
        context->internal_ctrl.set_trace_id(context->trace_id);
        context->internal_ctrl.set_timeout_ms(context->timeout_ms);
        for (size_t i = 0; i < executions->size(); ++i) {
            Execution& execution = (*executions)[i];
            context->raw_requests[i].cmd_id = execution.cmd;
            context->raw_requests[i].key = execution.partition_key.data_;
            context->raw_requests[i].input.set_module_id(GetModuleId(execution.cmd));
            context->raw_requests[i].input.set_function_id(GetFunctionId(execution.cmd));
            context->raw_requests[i].input.set_request_bytes(std::move(execution.request.data_));
            context->requests[i] = &context->raw_requests[i];
            context->responses[i] = &context->raw_responses[i];
        }

        // execute
        bcache2::CoSyncClosure sync;
        context->callback = callback != nullptr ? callback : &sync;
        client::RequestOptions options;
        options.event_replication_mode =
            static_cast<client::EventReplicationMode>(context->event_replication_mode);
        table_->BatchExecute(&context->internal_ctrl, context->requests, context->responses,
                             NewClosure(this, &TableImpl::OnBatchExecuteDone, context, executions),
                             options);
        if (callback == nullptr) {
            sync.Wait();
        }
    }

    void OnBatchExecuteDone(ExecutionContext* context, std::vector<Execution>* executions) {
        ScopedCallback done(context->callback);
        uint64_t request_bytes = 0;
        uint64_t response_bytes = 0;
        for (size_t i = 0; i < executions->size(); ++i) {
            request_bytes += context->requests[i]->input.ByteSize();
            response_bytes +=
                context->responses[i]->output ? context->responses[i]->output->ByteSize() : 0;
        }

        // batch metric
        table_->GetBatchRequestMetrics()->Set(
            context->internal_ctrl.status().ok(), context->cost.GetElapsedInUs(), request_bytes,
            response_bytes, context->internal_ctrl.status().errorcode());
        table_->GetBatchSizeMetric()->get()->Set(executions->size());

        if (!context->internal_ctrl.status().ok()) {
            LOG_ERROR_SAMPLE("Batch execute failed")
                .put("TraceId", context->trace_id)
                .put("Error", context->internal_ctrl.status());
            context->status = Status(context->internal_ctrl.status().errorcode(),
                                     context->internal_ctrl.status().ToString());
            return;
        }

        // parse response
        for (size_t i = 0; i < executions->size(); ++i) {
            // set result status
            if (context->raw_responses[i].output->status().code() != 0) {
                (*executions)[i].status =
                    Status(context->raw_responses[i].output->status().code(),
                           context->raw_responses[i].output->status().message());
            } else if (context->raw_responses[i].output->response_status().code() != 0) {
                (*executions)[i].status =
                    Status(context->raw_responses[i].output->response_status().code(),
                           context->raw_responses[i].output->response_status().message());
            }

            // metric
            RequestMetrics* metrics = table_->GetMetric(context->raw_requests[i].cmd_id);
            if (LIKELY(metrics != nullptr)) {
                metrics->Set((*executions)[i].status.ok(), context->cost.GetElapsedInUs(),
                    context->raw_requests[i].input.ByteSize(),
                    context->raw_responses[i].output->ByteSize(),
                    (*executions)[i].status.code());
            }
            if (!(*executions)[i].status.ok() &&
                (*executions)[i].status.code() != BCACHE2_NOT_FOUND) {
                LOG_WARNING("Execute failed")
                    .put("TraceId", context->trace_id)
                    .put("Index", i)
                    .put("ParitionKey", DebugRawString(context->raw_requests[i].key))
                    .put("Cmd", context->raw_requests[i].cmd_id)
                    .put("Request", context->raw_requests[i].input.ShortDebugString())
                    .put("Response", context->raw_responses[i].output->ShortDebugString());
                continue;
            }

            (*executions)[i].response.data_ =
                std::move(*context->raw_responses[i].output->mutable_response_bytes());

            LOG_DEBUG("Execute info in batch")
                .put("TraceId", context->trace_id)
                .put("Index", i)
                .put("ParitionKey", DebugRawString(context->raw_requests[i].key))
                .put("Cmd", context->raw_requests[i].cmd_id)
                .put("Request", context->raw_requests[i].input.ShortDebugString())
                .put("Response", context->raw_responses[i].output->ShortDebugString());
        }

        context->status = Status();
    }

 private:
    std::unique_ptr<client::Client> client_;
    std::unique_ptr<client::TableCore> table_;
};

class ClientImpl : public Client {
 public:
    explicit ClientImpl(const ClientOptions& options) : options_(options) {}

    Status OpenTable(const std::string& uri,
            const TableOptions& options, Table** table) override {
        LOG_CALL_INFO().put("Uri", uri);

        // parse schema
        std::string::size_type base = 0;
        std::string::size_type pos = uri.find("://", base);
        if (pos == std::string::npos) {
            LOG_ERROR("Miss schema").put("Uri", uri);
            return Status(-1, "Uri format error: miss schema");
        }
        std::string schema = uri.substr(base, pos + 3 - base);
        base = pos + 3;

        // parse cluster
        pos = uri.find("/", base);
        if (pos == std::string::npos) {
            LOG_ERROR("Miss cluster").put("Uri", uri);
            return Status(-1, "Uri format error: miss cluster");
        }
        std::string cluster = uri.substr(base, pos - base);
        base = pos + 1;

        // parse namespace
        pos = uri.find("/", base);
        if (pos == std::string::npos) {
            LOG_ERROR("Miss namespace").put("Uri", uri);
            return Status(-1, "Uri format error: miss namespace");
        }
        std::string ns = uri.substr(base, pos - base);
        base = pos + 1;

        // parse namespace
        std::string tablename = uri.substr(base);

        // generate psm
        std::string bcache2_psm = "bcache2.unknown.psm";
        if (cluster.find("bcache2.metaserver.") != std::string::npos) {
            std::string cluster_only = cluster.substr(strlen("bcache2.metaserver."),
                std::string::npos);
            bcache2_psm = "bcache2_" + cluster_only +
                "." + ns + "." + tablename;
        }

        LOG_INFO("Parse uri ok")
            .put("Schema", schema)
            .put("IDC", options_.idc)
            .put("Psm", bcache2_psm)
            .put("Cluster", cluster)
            .put("Namespace", ns)
            .put("Table", tablename);

        std::string master_consul;
        std::string master_addr;
        if (schema == "consul://") {
            master_consul = cluster;
        } else if (schema == "tcp://") {
            master_addr = cluster;
        } else {
            LOG_ERROR("Schemma error").put("Uri", uri).put("Schema", schema);
            return Status(-1, "Schema error");
        }

        client::Client* tmp_client = nullptr;
        client::ClientOptions client_options;
        client_options.bcache2_psm = bcache2_psm;
        client_options.psm = options_.psm;
        client_options.host = options_.host;
        client_options.idc = options_.idc;
        client_options.log_dir = options_.log_dir;
        client_options.log_level = bcache2::client::LogLevel(options_.log_level);
        client_options.meta_sync_interval_ms = options_.meta_sync_interval_ms;
        client_options.topo_error_retry_interval_ms = options_.topo_error_retry_interval_ms;
        client_options.meta_fetch_timeout_ms = options_.meta_fetch_timeout_ms;
        client_options.master_consul = master_consul;
        client_options.master_addr = master_addr;
        if (schema == "tcp://") {
            client_options.af = client::AddressFamily::kIp4;
        }
        if (options_.pin_primary) {
            client_options.partition_pick_opts.policy =
                client::PartitionPickOptions::Policy::kPrimary;
        }
        bcache2::Status status = client::Client::Create(client_options, &tmp_client);
        if (!status.ok()) {
            LOG_ERROR("Create client failed").put("Error", status);
            return Status(status.errorcode(), status.ToString());
        }
        std::unique_ptr<client::Client> client(tmp_client);

        client::Table* tmp_table = nullptr;
        client::TableOptions table_options;
        table_options.io_timeout_ms = options.io_timeout_ms;
        table_options.connect_timeout_ms = options.connect_timeout_ms;
        table_options.continuous_failed_time_ms = options.continuous_failed_time_ms;
        status = client->OpenTable(ns, tablename, table_options, &tmp_table);
        if (!status.ok()) {
            LOG_ERROR("Open table failed").put("Error", status);
            return Status(status.errorcode(), status.ToString());
        }

        client::TableCore* table_core = dynamic_cast<client::TableCore*>(tmp_table);
        BYTE_ASSERT(table_core != nullptr);

        *table = new TableImpl(client.release(), table_core);
        return Status();
    }

 private:
    ClientOptions options_;
};

Status Client::Create(const ClientOptions& options, Client** client) {
    LOG_CALL_INFO()
        .put("Psm", options.psm)
        .put("Host", options.psm)
        .put("Idc", options.idc)
        .put("LogDir", options.log_dir)
        .put("LogLevel", static_cast<int>(options.log_level))
        .put("LogConsole", options.log_console)
        .put("MetaSyncIntervalMs", options.meta_sync_interval_ms);
    *client = new ClientImpl(options);
    if (options.log_console) {
        std::vector<byte::Logger*> loggers;
        for (int lv = byte::LOG_LEVEL_DEBUG; lv <= byte::LOG_LEVEL_FATAL; lv++) {
            byte::Logger* logger = new ConsoleLogger(static_cast<byte::LogLevel>(lv));
            loggers.push_back(logger);
        }
        byte::SetUserCustomizedLogger(loggers);
    }
    return Status();
}

}  // namespace swig
}  // namespace bcache2

#ifdef __cplusplus
extern "C" {
#endif

struct bcache2_options : public bcache2::swig::ClientOptions {};

bcache2_options_t* bcache2_options_init() { return new bcache2_options; }

void bcache2_options_destory(bcache2_options_t* options) { delete options; }

void bcache2_options_set(bcache2_options_t* options, const char* name, const char* value) {
    std::string option = name;
    if (option == "psm") {
        options->psm = value;
    } else if (option == "host") {
        options->host = value;
    } else if (option == "idc") {
        options->idc = value;
    } else if (option == "log_dir") {
        options->log_dir = value;
    } else if (option == "log_level") {
        options->log_level = bcache2::swig::LogLevel(atoi(value));
    } else if (option == "log_console") {
        options->log_console = std::string(value) == "true";
    } else if (option == "meta_sync_interval_ms") {
        options->meta_sync_interval_ms = atoi(value);
    } else if (option == "topo_error_retry_interval_ms") {
        options->topo_error_retry_interval_ms = atoi(value);
    } else if (option == "meta_fetch_timeout_ms") {
        options->meta_fetch_timeout_ms = atoi(value);
    } else if (option == "pin_primary") {
        options->pin_primary = std::string(value) == "true" || std::string(value) == "1";
    }
}

struct bcache2_table_options : public bcache2::swig::TableOptions {};

bcache2_table_options_t* bcache2_tableoptions_init() { return new bcache2_table_options; }

void bcache2_tableoptions_destory(bcache2_table_options_t* options) { delete options; }

void bcache2_tableoptions_set(bcache2_table_options_t* options, const char* name,
                              const char* value) {
    std::string option = name;
    if (option == "io_timeout_ms") {
        options->io_timeout_ms = atol(value);
    } else if (option == "connect_timeout_ms") {
        options->connect_timeout_ms = atol(value);
    } else if (option == "continuous_failed_time_ms") {
        options->continuous_failed_time_ms = atol(value);
    }
}

struct bcache2_execution : public bcache2::swig::ExecutionContext {
    std::vector<bcache2::swig::Execution> executions;
};

bcache2_execution_t* bcache2_execution_init(int64_t trace_id, int64_t timeout) {
    bcache2_execution* execution = new bcache2_execution;
    execution->trace_id = trace_id;
    execution->timeout_ms = timeout;
    return execution;
}

void bcache2_execution_destory(bcache2_execution_t* execution) { delete execution; }

void bcache2_execution_add_request(bcache2_execution_t* execution, uint32_t cmd,
                                   data_t partition_key, data_t request) {
    bcache2::swig::Execution exe;
    exe.cmd = cmd;
    exe.partition_key = bcache2::swig::Bytes(partition_key.data, partition_key.size);
    exe.request = bcache2::swig::Bytes(request.data, request.size);
    execution->executions.push_back(exe);
}

int bcache2_execution_get_status(bcache2_execution_t* execution, int request_index) {
    return execution->executions[request_index].status.code();
}

const char* bcache2_execution_get_message(bcache2_execution_t* execution, int request_index) {
    return execution->executions[request_index].status.message().c_str();
}

data_t bcache2_execution_get_response(bcache2_execution_t* execution, int request_index) {
    data_t response;
    response.data = execution->executions[request_index].response.data();
    response.size = execution->executions[request_index].response.size();
    return response;
}

namespace {

bcache2::swig::Client* global_client = nullptr;

}  // namespace

void bcache2_init(bcache2_options_t* options) {
    BYTE_ASSERT(global_client == nullptr) << "has been inited";
    bcache2::swig::Status status = bcache2::swig::Client::Create(*options, &global_client);
    assert(status.ok());
}

void bcache2_destory() {
    delete global_client;
    global_client = nullptr;
}

struct bcache2_table {
    bcache2::swig::TableImpl* table = nullptr;
};

// only support consul. bcache2_${cluster}.${namespace}.${table}
std::string bcache2_psm(const char* str) {
    std::string str1(str);
    std::smatch match;
    auto search_res = std::regex_search(str1, match,
        std::regex(R"(bcache2_(\w+)\.(\w+)\.(\w+))"));
    if (!search_res || (match.size() != 4)) {
        LOG_ERROR("Regex_search fail").put("Str", str);
        return "";
    }
    std::string res = "consul://bcache2.metaserver." + std::string(match[1])
            + "/" + std::string(match[2]) + "/" + std::string(match[3]);
    return res;
}

int bcache2_open(const char* uri, bcache2_table_options_t* options, bcache2_table_t** table) {
    bcache2::swig::Status status;
    bcache2_table_t* tmp_table = new bcache2_table;
    bcache2::swig::Table* tmp_swig_table = nullptr;
    if (std::string(uri).find("://") == std::string::npos) {
        const std::string& uri_new = bcache2_psm(uri);
        if (uri_new.compare("") == 0) {
            LOG_ERROR("Bcache2_psm fail");
            delete tmp_table;
            return BCACHE2_INVALID_ARGUMENT;
        }

        status = global_client->OpenTable(uri_new, *options, &tmp_swig_table);
        if (status.code() != BCACHE2_OK) {
            delete tmp_table;
            return status.code();
        }
    } else {
        status = global_client->OpenTable(uri, *options, &tmp_swig_table);
        if (status.code() != BCACHE2_OK) {
            delete tmp_table;
            return status.code();
        }
    }
    tmp_table->table = dynamic_cast<bcache2::swig::TableImpl*>(tmp_swig_table);
    *table = tmp_table;
    return status.code();
}

void bcache2_close(bcache2_table_t* table) {
    delete table->table;
    delete table;
}

void bcache2_execute(bcache2_table_t* table, bcache2_execution_t* execution,
                     bcache2_callback_t callback, void* callback_args) {
    Closure<void>* done = callback != nullptr ? NewClosure(callback, callback_args) : nullptr;
    table->table->BatchExecute(execution, &execution->executions, done);
}

#ifdef __cplusplus
}
#endif
