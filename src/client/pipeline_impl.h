// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>
#include <vector>

#include "client/client_impl.h"

namespace bcache2 {
namespace client {

class PipelineImpl : public TableImpl {
 public:
    PipelineImpl(TableImpl* table, RequestMetrics* request_metrics,
                 MetricsEnv::HistogramHolder* batch_size);
    virtual ~PipelineImpl();
    void Execute(Controller* ctrl, Request* request, Response* response, Closure<void>* callback,
                 Closure<void>* post_execute, const RequestOptions& option) override;
    std::vector<Status> Sync(const RequestOptions& option) override;

 private:
    TableImpl* table_ = nullptr;
    std::vector<Request*> requests_;
    std::vector<Response*> responses_;
    std::vector<Closure<void>*> post_executes_;
    RequestMetrics* request_metrics_ = nullptr;
    MetricsEnv::HistogramHolder* batch_size_ = nullptr;
};

}  // namespace client
}  // namespace bcache2
