// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "stream/log_based_env.h"

#include <byte/base/closure.h>
#include <byte/include/assert.h>

#include <memory>
#include <utility>
#include <vector>

#include "common/function_closure.h"
#include "common/logging.h"
#include "common/scoped_invoker.h"
#include "stream/log_based_stream.h"
#include "stream/log_based_stream_reader.h"

namespace bcache2 {
namespace stream {

LogBasedEnv::LogBasedEnv() {}

LogBasedEnv::~LogBasedEnv() {}

void LogBasedEnv::Init(const Options& options) {
    options_ = options;
    background_pool_ = options.background_pool;
    store_layer_ = options.store_layer;
}

void LogBasedEnv::SetCondition(Controller* ctrl, const Condition& condition,
                               const std::string& condition_name,
                               const ConditionData& condition_data) {
    Store::SetConditionOptions options;
    options.condition = condition;
    store_layer_->SetCondition(ctrl, condition_name, condition_data, options);
}

void LogBasedEnv::GetCondition(Controller* ctrl, const std::string& condition_name,
                               ConditionData* condition_data) {
    store_layer_->StatCondition(ctrl, condition_name, condition_data);
}

void LogBasedEnv::OpenStream(Controller* ctrl, const Condition& condition, const std::string& uri,
                             const OpenOptions& options, Stream** stream) {
    std::unique_ptr<Stream> tmp_stream;
    if (options.readonly) {
        tmp_stream.reset(new StreamReaderImpl(this, uri, options.metrics_manager));
    } else {
        tmp_stream.reset(new StreamImpl(this, uri, condition, options.rep_policy, options.token,
                                        options.metrics_manager));
    }

    Status status = tmp_stream->Load();
    if (!status.ok()) {
        LOG_ERROR("Open stream failed").put("Uri", uri).put("Error", status.ToString());
        ctrl->set_status(status);
        return;
    }

    *stream = tmp_stream.release();
    LOG_INFO("Open stream success").put("Uri", uri).put("Stream", *stream);
    ctrl->set_status(Status::OK());
}

void LogBasedEnv::DeleteStream(Controller* ctrl, const Condition& condition, const std::string& uri,
                               const DeleteOptions& options) {
    std::vector<Store::BlobInfo> blobs;
    store_layer_->List(ctrl, uri, &blobs);
    if (!ctrl->status().ok()) {
        LOG_ERROR("List blobs failed").put("Uri", uri).put("Error", ctrl->status().ToString());
        return;
    }

    for (auto& entry : blobs) {
        std::string name = uri + entry.name;
        Store::DeleteOptions delete_options;
        delete_options.condition = condition;
        store_layer_->Delete(ctrl, name, delete_options);
        if (!ctrl->status().ok() && !ctrl->status().IsStoreNotFound()) {
            LOG_ERROR("Delete blob failed").put("Uri", uri).put("Error", ctrl->status().ToString());
            ctrl->set_status(Status::Internal("Delete blob failed"));
        }
    }

    LOG_INFO("Delete stream ok").put("Uri", uri);
}

}  // namespace stream
}  // namespace bcache2
