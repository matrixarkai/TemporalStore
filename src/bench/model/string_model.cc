// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "bench/model/string_model.h"

#include <utility>

#include "bench/model/nil_model.h"
#include "extension/modules.pb.h"

namespace bcache2 {
namespace bench {

Status StringModel::Apply(const ApplyOptions& opts, const Operation& op,
                          std::vector<std::unique_ptr<Model>>* next_states) const {
    if (op.module_id() != Module::COMMON && op.module_id() != Module::STRING) {
        return Status::FailedPrecondition("Cross model type");
    }

    if (op.module_id() == Module::COMMON) {
        std::vector<ModelProperty> next_properties;
        Status status = property_.Apply(opts, op, &next_properties);
        if (!status.ok()) {
            return status;
        }

        BYTE_ASSERT(next_properties.size() > 0);
        for (auto& next_property : next_properties) {
            if (next_property.IsNil(op.start_time_us(), op.end_time_us()) ==
                NilStatus::DefinitelyNil) {
                next_states->emplace_back(new NilModel());
            } else {
                std::unique_ptr<StringModel> next_model(new StringModel(*this));
                next_model->property_ = std::move(next_property);
                next_states->emplace_back(std::move(next_model));
            }
        }

        return Status::OK();
    }

    if (op.module_id() == Module::STRING) {
        switch (op.function_id()) {
        case str2::SET: {
            str2::SetRequest req;
            str2::SetResponse resp;
            BYTE_ASSERT(req.ParseFromString(op.request_bytes()));
            BYTE_ASSERT(resp.ParseFromString(op.response_bytes()));
            return ApplyInternal(opts, req, resp, op, next_states);
        }
        case str2::SETEX: {
            str2::SetexRequest req;
            str2::SetexResponse resp;
            BYTE_ASSERT(req.ParseFromString(op.request_bytes()));
            BYTE_ASSERT(resp.ParseFromString(op.response_bytes()));
            return ApplyInternal(opts, req, resp, op, next_states);
        }
        case str2::GET: {
            str2::GetRequest req;
            str2::GetResponse resp;
            BYTE_ASSERT(req.ParseFromString(op.request_bytes()));
            BYTE_ASSERT(resp.ParseFromString(op.response_bytes()));
            return ApplyInternal(opts, req, resp, op, next_states);
        }
        default:
            return Status::FailedPrecondition("Cmd not support");
        }
    }

    return Status::FailedPrecondition("Cmd not support");
}

// TODO(wangtai.10): support nn&nx flags
Status StringModel::ApplyInternal(const ApplyOptions& opts, const str2::SetRequest& request,
                                  const str2::SetResponse& response, const Operation& op,
                                  std::vector<std::unique_ptr<Model>>* next_states) const {
    next_states->clear();
    switch (op.code()) {
    case kOK: {
        // request success
        DoApplyValue(opts, op.start_time_us(), op.end_time_us(), request.value(), 0, next_states);
        return Status::OK();
    }

    default: {
        // request failure

        // maybe not execute
        if (property_.IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNil) {
            next_states->emplace_back(new NilModel());
        } else {
            next_states->emplace_back(new StringModel(*this));
        }

        // maybe executed
        DoApplyValue(opts, op.start_time_us(), op.end_time_us(), request.value(), 0, next_states);
        return Status::OK();
    }
    }
}

Status StringModel::ApplyInternal(const ApplyOptions& opts, const str2::SetexRequest& request,
                                  const str2::SetexResponse& response, const Operation& op,
                                  std::vector<std::unique_ptr<Model>>* next_states) const {
    next_states->clear();
    switch (op.code()) {
    case kOK: {
        // request success
        DoApplyValue(opts, op.start_time_us(), op.end_time_us(), request.value(), request.ttl_ms(),
                     next_states);
        return Status::OK();
    }

    default: {
        // request failure

        // maybe not execute
        if (property_.IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNil) {
            next_states->emplace_back(new NilModel());
        } else {
            next_states->emplace_back(new StringModel(*this));
        }

        // maybe executed
        DoApplyValue(opts, op.start_time_us(), op.end_time_us(), request.value(), request.ttl_ms(),
                     next_states);
        return Status::OK();
    }
    }
}

Status StringModel::ApplyInternal(const ApplyOptions& opts, const str2::GetRequest& request,
                                  const str2::GetResponse& response, const Operation& op,
                                  std::vector<std::unique_ptr<Model>>* next_states) const {
    next_states->clear();
    switch (op.code()) {
    case kOK: {
        // request success
        if (property_.IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNil) {
            return Status::Internal("Model is nil but request success");
        }
        if (value_ != response.value()) {
            return Status::Internal("Value not match");
        }
        next_states->emplace_back(new StringModel(*this));
        return Status::OK();
    }

    case kNotFound: {
        // key not found
        if (property_.IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNonNil) {
            return Status::Internal("Model is nil but request success");
        }
        next_states->emplace_back(new NilModel());
        return Status::OK();
    }

    default: {
        // request failure
        if (property_.IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNil) {
            next_states->emplace_back(new NilModel());
        } else {
            next_states->emplace_back(new StringModel(*this));
        }
        return Status::OK();
    }
    }
}

void StringModel::DoApplyValue(const ApplyOptions& opts, uint64_t start_ts_us, uint64_t end_ts_us,
                               const std::string& value, uint64_t ttl_ms,
                               std::vector<std::unique_ptr<Model>>* next_states) const {
    NilStatus nil_status = property_.IsNil(start_ts_us, end_ts_us);
    switch (nil_status) {
    case NilStatus::DefinitelyNil: {
        // model is nil, clear property and set value
        std::unique_ptr<StringModel> new_model(new StringModel());
        new_model->value_ = value;
        new_model->property_.SetNonNil();
        if (ttl_ms > 0) {
            new_model->property_.SetTtl(start_ts_us, end_ts_us, ttl_ms,
                                        opts.max_expire_ambiguous_time_ms);
        }
        next_states->emplace_back(std::move(new_model));
        break;
    }
    case NilStatus::DefinitelyNonNil: {
        // model is non-nil, inherit property and set value
        std::unique_ptr<StringModel> new_model(new StringModel(*this));
        new_model->value_ = value;
        if (ttl_ms > 0) {
            new_model->property_.SetTtl(start_ts_us, end_ts_us, ttl_ms,
                                        opts.max_expire_ambiguous_time_ms);
        }
        next_states->emplace_back(std::move(new_model));
        break;
    }
    case NilStatus::Ambiguous: {
        // maybe non-nil, inherit property and set value
        std::unique_ptr<StringModel> new_model(new StringModel(*this));
        new_model->value_ = value;
        if (ttl_ms > 0) {
            new_model->property_.SetTtl(start_ts_us, end_ts_us, ttl_ms,
                                        opts.max_expire_ambiguous_time_ms);
        }
        next_states->emplace_back(std::move(new_model));

        // maybe nil, clear property and set value
        new_model.reset(new StringModel());
        new_model->value_ = value;
        new_model->property_.SetNonNil();
        if (ttl_ms > 0) {
            new_model->property_.SetTtl(start_ts_us, end_ts_us, ttl_ms,
                                        opts.max_expire_ambiguous_time_ms);
        }
        next_states->emplace_back(std::move(new_model));
        break;
    }
    }
}

}  // namespace bench
}  // namespace bcache2
