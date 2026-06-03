// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <stdint.h>

#include <string>
#include <utility>
#include <vector>

namespace bcache2 {
namespace swig {

class Status {
 public:
    Status() {}
    Status(int code, std::string message) : code_(code), message_(std::move(message)) {}

    bool ok() const { return code_ == 0; }
    int code() const { return code_; }
    const std::string& message() const { return message_; }

 private:
    int code_ = 0;
    std::string message_;
    // ALLOW_COPY_AND_ASSIGN
};

class Bytes {
 public:
    Bytes() {}
    Bytes(const void* indata, int inlen) : data_(static_cast<const char*>(indata), inlen) {}
    const void* data() const { return data_.data(); }
    int size() const { return static_cast<int>(data_.size()); }
    std::string& raw() { return data_; }

 private:
    friend class TableImpl;

    std::string data_;
    // ALLOW_COPY_AND_ASSIGN
};

struct Controller {
    uint64_t trace_id = 0;
    int64_t timeout_ms = 5000;  // 5s
    Status status;
};

struct Execution {
    uint32_t cmd = 0;
    Bytes partition_key;
    Bytes request;
    Status status;
    Bytes response;
};

class Table {
 public:
    virtual ~Table() {}
    virtual void BatchExecute(Controller* ctrl, std::vector<Execution>* executions) = 0;
};

enum class LogLevel {
    kAll,
    kDebug,
    kInfo,
    kWarning,
    kError,
    kFatal,
};

// Keep exactly the same as google api code，See
// https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto
enum class Code {
    OK = 0,
    CANCELLED = 1,
    UNKNOWN = 2,
    INVALID_ARGUMENT = 3,
    DEADLINE_EXCEEDED = 4,
    NOT_FOUND = 5,
    ALREADY_EXISTS = 6,
    PERMISSION_DENIED = 7,
    RESOURCE_EXHAUSTED = 8,
    FAILED_PRECONDITION = 9,
    ABORTED = 10,
    OUT_OF_RANGE = 11,
    UNIMPLEMENTED = 12,
    INTERNAL = 13,
    UNAVAILABLE = 14,
    DATA_LOSS = 15,
    UNAUTHENTICATED = 16,
};

// TODO(zhangyuan.42): Improve cmd define
enum class OpType {
    kOpHashGet,
    kOpHashSet,
    kOpHashDel,
    kOpFeatureAdd,
    kOpFeatureQuery,
    kOpStringGet,
    kOpStringSet,
    kOpStringSetEx,
    kOpIPSAdd,
    kOpIPSQuery,
    kOpIPSRemove,
    kOpDel,
    kOpExpire,
    kOpTtl,
};

struct ClientOptions {
    std::string psm;
    std::string host = "127.0.0.1";
    std::string idc = "vdc";
    std::string log_dir = "./";
    LogLevel log_level = LogLevel::kInfo;
    bool log_console = false;
    int64_t meta_sync_interval_ms = 1000 * 60 * 10;
    int64_t topo_error_retry_interval_ms = 1000 * 5;
    int64_t meta_fetch_timeout_ms = 2000;
    bool pin_primary = false;
};

struct TableOptions {
    int64_t io_timeout_ms = 200;
    int64_t connect_timeout_ms = 200;
    int64_t continuous_failed_time_ms = 10000;
};

class Client {
 public:
    static Status Create(const ClientOptions& options, Client** client);

    virtual ~Client() {}
    virtual Status OpenTable(const std::string& uri, const TableOptions& options,
                             Table** table) = 0;
};

}  // namespace swig
}  // namespace bcache2
