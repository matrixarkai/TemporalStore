// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "extension/control_state/interface.pb.h"
#include "protocol/server.pb.h"

namespace bcache2 {
namespace client {

enum class LogLevel {
    kAll,
    kDebug,
    kInfo,
    kWarning,
    kError,
    kFatal,
};

typedef bcache2::control_state::HsetRequest ControlStateHsetRequest;
typedef bcache2::control_state::HsetResponse ControlStateHsetResponse;
typedef bcache2::control_state::HqueryRequest ControlStateHqueryRequest;
typedef bcache2::control_state::HqueryResponse ControlStateHqueryResponse;
typedef bcache2::control_state::FolSetRequest ControlStateFolSetRequest;
typedef bcache2::control_state::FolSetResponse ControlStateFolSetResponse;
typedef bcache2::control_state::FolQueryRequest ControlStateFolQueryRequest;
typedef bcache2::control_state::FolQueryResponse ControlStateFolQueryResponse;
typedef bcache2::control_state::CPCSetRequest ControlStateCPCSetRequest;
typedef bcache2::control_state::CPCSetResponse ControlStateCPCSetResponse;
typedef bcache2::control_state::CPCQueryRequest ControlStateCPCQueryRequest;
typedef bcache2::control_state::CPCQueryResponse ControlStateCPCQueryResponse;
typedef bcache2::control_state::ManagerRequest ControlStateManagerRequest;
typedef bcache2::control_state::ManagerResponse ControlStateManagerResponse;

class Pipeline;

enum class EventReplicationMode {
    kInherit = 0,
    kAsyncStorage = 1,
    kSyncStorage = 2,
    kRaft = 3,
};

struct RequestOptions {
    uint64_t trace_id = 0;
    EventReplicationMode event_replication_mode = EventReplicationMode::kInherit;

    static RequestOptions ForReplication(EventReplicationMode mode, uint64_t trace_id = 0) {
        RequestOptions options;
        options.trace_id = trace_id;
        options.event_replication_mode = mode;
        return options;
    }

    RequestOptions& UseAsyncStorage() {
        event_replication_mode = EventReplicationMode::kAsyncStorage;
        return *this;
    }

    RequestOptions& UseSyncStorage() {
        event_replication_mode = EventReplicationMode::kSyncStorage;
        return *this;
    }

    RequestOptions& UseRaft() {
        event_replication_mode = EventReplicationMode::kRaft;
        return *this;
    }
};

// NOTE: deprecated, please use swig_client
class Table {
 public:
    Table() {}
    virtual ~Table() {}
    virtual Status OpenPipeline(Pipeline** pipeline) = 0;
    virtual Status Del(const std::string& key, const RequestOptions& option) = 0;
    Status Del(const std::string& key);
    virtual Status Expire(const std::string& key, uint64_t ttl, const RequestOptions& option) = 0;
    Status Expire(const std::string& key, uint64_t ttl);
    virtual Status Ttl(const std::string& key, uint64_t* ttl, const RequestOptions& option) = 0;
    Status Ttl(const std::string& key, uint64_t* ttl);
    virtual Status SetEx(const std::string& key, const std::string& value, uint64_t ttl,
                         const RequestOptions& option) = 0;
    Status SetEx(const std::string& key, const std::string& value, uint64_t ttl);
    virtual Status Get(const std::string& key, std::string* value,
                       const RequestOptions& option) = 0;
    Status Get(const std::string& key, std::string* value);
    virtual Status Set(const std::string& key, const std::string& value,
                       const RequestOptions& option) = 0;
    Status Set(const std::string& key, const std::string& value);
    virtual Status HGet(const std::string& key, const std::string& field, std::string* value,
                        const RequestOptions& option) = 0;
    Status HGet(const std::string& key, const std::string& field, std::string* value);
    virtual Status HSet(const std::string& key, const std::string& field, const std::string& value,
                        const RequestOptions& option) = 0;
    Status HSet(const std::string& key, const std::string& field, const std::string& value);
    virtual Status HDel(const std::string& key, const std::string& field,
                        const RequestOptions& option) = 0;
    Status HDel(const std::string& key, const std::string& field);
    /*
    以下命令为：字节电商平台治理相关的操作命令
    ControlState写入命令,用于DC, COUNT, SUM,MIN,MAX操作所对应的写入
    */
    Status ControlStateHset(const ControlStateHsetRequest& req, ControlStateHsetResponse* resp);
    virtual Status ControlStateHset(const ControlStateHsetRequest& req, ControlStateHsetResponse* resp,
                            const RequestOptions& option) = 0;
    // First OR Last操作对应的写入命令
    Status ControlStateFolSet(const ControlStateFolSetRequest& req, ControlStateFolSetResponse* resp);
    virtual Status ControlStateFolSet(const ControlStateFolSetRequest& req, ControlStateFolSetResponse* resp,
                              const RequestOptions& option) = 0;

    // ControlStateDC查询命令,用于DC,COUNT,SUM,MIN,MAX操作
    Status ControlStateHquery(const ControlStateHqueryRequest& req, ControlStateHqueryResponse* resp);
    virtual Status ControlStateHquery(const ControlStateHqueryRequest& req, ControlStateHqueryResponse* resp,
                              const RequestOptions& option) = 0;
    // First OR Last操作对应的查询命令
    Status ControlStateFolQuery(const ControlStateFolQueryRequest& req, ControlStateFolQueryResponse* resp);
    virtual Status ControlStateFolQuery(const ControlStateFolQueryRequest& req, ControlStateFolQueryResponse* resp,
                                const RequestOptions& option) = 0;
};

struct TableOptions {
    int64_t io_timeout_ms = 200;
    int64_t connect_timeout_ms = 200;
    int64_t continuous_failed_time_ms = 10000;
};

enum class AddressFamily {
    kIp4,
    kIp6,
};

struct PartitionPickOptions {
    enum class Policy {
        // Prefer to deliver to partitions which are located in my VDC
        kVdcAffinity,
        // Prefer to deliver to primary partition
        kPrimary,
    };

    Policy policy = Policy::kVdcAffinity;
    std::string affinity_vdc;  // if empty we use ClientOptions::idc
};

struct ClientOptions {
    std::string psm;
    std::string cluster;
    // TODO(guogaofeng) open mul table
    std::string bcache2_psm;
    std::string host = "127.0.0.1";
    std::string idc = "vdc";  // vdc here
    std::string log_dir = "./";
    LogLevel log_level = LogLevel::kWarning;
    int64_t meta_sync_interval_ms = 1000 * 60 * 10;
    int64_t topo_error_retry_interval_ms = 1000 * 5;
    int64_t meta_fetch_timeout_ms = 2000;
    std::string master_consul;
    std::string master_addr;

    // define the protocol which is used to connect to backend servers
    AddressFamily af = AddressFamily::kIp6;

    PartitionPickOptions partition_pick_opts;
};

class Client {
 public:
    Client() {}
    virtual ~Client() {}

    static Status Create(const ClientOptions& options, Client** client);

    virtual Status OpenTable(const std::string& name_space, const std::string& table_name,
                             const TableOptions& options, Table** table) = 0;

    virtual Status CloseTable(Table* table) = 0;
};

class Pipeline : public Table {
 public:
    Pipeline() {}
    virtual ~Pipeline() {}
    virtual std::vector<Status> Sync(const RequestOptions& option) { return std::vector<Status>(); }
    std::vector<Status> Sync() { return Sync(RequestOptions()); }
};

}  // namespace client
}  // namespace bcache2
