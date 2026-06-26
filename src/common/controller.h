// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <utility>

#include "byte/thread/async_thread.h"
#include "common/status.h"

namespace bcache2 {

// timeout and trace
class Controller {
 public:
    Controller() {}
    explicit Controller(uint64_t trace_id) : trace_id_(trace_id) {}

    ~Controller() {}

    void Reset() {
        timeout_ms_ = 5000;  // 5s
        event_replication_mode_ = 0;
        status_ = Status::OK();
    }

    void set_trace_id(uint64_t trace_id) { trace_id_ = trace_id; }
    uint64_t trace_id() const { return trace_id_; }

    void set_event_replication_mode(int mode) { event_replication_mode_ = mode; }
    int event_replication_mode() const { return event_replication_mode_; }

    void set_timeout_ms(uint64_t timeout_ms) { timeout_ms_ = timeout_ms; }
    uint64_t timeout_ms() const { return timeout_ms_; }

    void set_status(Status status) { status_ = std::move(status); }
    const Status& status() const { return status_; }

 private:
    uint64_t timeout_ms_ = 5000;  // 5s
    uint64_t trace_id_ = 0;
    int event_replication_mode_ = 0;
    Status status_;
};

}  // namespace bcache2
