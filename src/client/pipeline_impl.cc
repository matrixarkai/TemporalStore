// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "client/pipeline_impl.h"

#include "common/sync_closure.h"

namespace bcache2 {
namespace client {

PipelineImpl::PipelineImpl(TableImpl* table, RequestMetrics* request_metrics,
                           MetricsEnv::HistogramHolder* batch_size)
    : TableImpl(table), table_(table), request_metrics_(request_metrics), batch_size_(batch_size) {}

void PipelineImpl::Execute(Controller* ctrl, Request* request, Response* response,
                           Closure<void>* callback, Closure<void>* post_execute,
                           const RequestOptions& option) {
    requests_.emplace_back(request);
    responses_.emplace_back(response);
    post_executes_.emplace_back(post_execute);
    ctrl->set_status(Status::OK());
    callback->Run();
}

std::vector<Status> PipelineImpl::Sync(const RequestOptions& option) {
    if (requests_.empty()) {
        LOG_WARNING("Pipeline empty").put("Table", table_->GetTableCombineName());
        return std::vector<Status>();
    }
    batch_size_->get()->Set(requests_.size());
    SyncClosure sync;
    Controller ctrl;
    TimeCost cost;
    table_->BatchExecute(&ctrl, requests_, responses_, &sync, option);
    sync.Wait();
    request_metrics_->Set(ctrl.status().ok(), cost.GetElapsedInUs(), 0, 0,
                          ctrl.status().errorcode());
    std::vector<Status> result(requests_.size(), ctrl.status());
    if (ctrl.status().ok()) {
        for (size_t i = 0; i < requests_.size(); i++) {
            Command* command = table_->GetCommand(requests_[i]->method);
            RequestMetrics* metrics = command->GetMetrics();
            if (responses_[i]->output->status().code() != Code::kOK) {
                result[i] = Status::FromRpcStatus(responses_[i]->output->status());
            }
            metrics->Set(result[i].ok(), cost.GetElapsedInUs(), requests_[i]->input.ByteSize(),
                        responses_[i]->output->ByteSize(), result[i].errorcode());
            if (responses_[i]->output->response_status().code() != Code::kOK) {
                result[i] = Status::FromRpcStatus(responses_[i]->output->response_status());
            }
        }
    }
    for (auto post_execute : post_executes_) {
        post_execute->Run();
    }
    post_executes_.clear();
    requests_.clear();
    responses_.clear();
    return result;
}

PipelineImpl::~PipelineImpl() { Sync(RequestOptions()); }

}  // namespace client
}  // namespace bcache2
