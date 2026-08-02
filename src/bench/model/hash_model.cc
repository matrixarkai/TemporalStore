// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "bench/model/hash_model.h"

#include <utility>

#include "bench/model/nil_model.h"
#include "extension/modules.pb.h"

namespace bcache2 {
namespace bench {

Status HashModel::Apply(const ApplyOptions& opts, const Operation& op,
                        std::vector<std::unique_ptr<Model>>* next_states) const {
    if (op.module_id() != Module::COMMON && op.module_id() != Module::HASH) {
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
                std::unique_ptr<HashModel> next_model(new HashModel(*this));
                next_model->property_ = std::move(next_property);
                next_states->emplace_back(std::move(next_model));
            }
        }

        return Status::OK();
    }

    if (op.module_id() == Module::HASH) {
        switch (op.function_id()) {
        case hash2::GET: {
            hash2::GetRequest req;
            hash2::GetResponse resp;
            BYTE_ASSERT(req.ParseFromString(op.request_bytes()));
            BYTE_ASSERT(resp.ParseFromString(op.response_bytes()));
            return ApplyInternal(opts, req, resp, op, next_states);
        }
        case hash2::SET: {
            hash2::SetRequest req;
            hash2::SetResponse resp;
            BYTE_ASSERT(req.ParseFromString(op.request_bytes()));
            BYTE_ASSERT(resp.ParseFromString(op.response_bytes()));
            return ApplyInternal(opts, req, resp, op, next_states);
        }
        case hash2::DEL: {
            hash2::DelRequest req;
            hash2::DelResponse resp;
            BYTE_ASSERT(req.ParseFromString(op.request_bytes()));
            BYTE_ASSERT(resp.ParseFromString(op.response_bytes()));
            return ApplyInternal(opts, req, resp, op, next_states);
        }
        case hash2::GETALL: {
            hash2::GetAllRequest req;
            hash2::GetAllResponse resp;
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

Status HashModel::ApplyInternal(const ApplyOptions& opts, const hash2::SetRequest& request,
                                const hash2::SetResponse& response, const Operation& op,
                                std::vector<std::unique_ptr<Model>>* next_states) const {
    next_states->clear();
    switch (op.code()) {
    case kOK: {
        // request success
        DoApplyValue(opts, op.start_time_us(), op.end_time_us(), request.field(), request.value(),
                     false, next_states);
        return Status::OK();
    }

    default: {
        // request failure

        // maybe not execute
        if (property_.IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNil) {
            next_states->emplace_back(new NilModel());
        } else {
            next_states->emplace_back(new HashModel(*this));
        }

        // maybe executed
        DoApplyValue(opts, op.start_time_us(), op.end_time_us(), request.field(), request.value(),
                     false, next_states);
        return Status::OK();
    }
    }
}

void HashModel::DoApplyValue(const ApplyOptions& opts, uint64_t start_ts_us, uint64_t end_ts_us,
                             const std::string& field, const std::string& value, bool del,
                             std::vector<std::unique_ptr<Model>>* next_states) const {
    NilStatus nil_status = property_.IsNil(start_ts_us, end_ts_us);
    switch (nil_status) {
    case NilStatus::DefinitelyNil: {
        // model is nil, clear property and set value
        if (del) {
            next_states->emplace_back(new NilModel());
        } else {
            std::unique_ptr<HashModel> new_model(new HashModel());
            new_model->value_[field] = value;
            new_model->property_.SetNonNil();
            next_states->emplace_back(std::move(new_model));
        }
        break;
    }
    case NilStatus::DefinitelyNonNil: {
        // model is non-nil, inherit property and set value
        std::unique_ptr<HashModel> new_model(new HashModel(*this));
        if (del) {
            new_model->value_.erase(field);
        } else {
            new_model->value_[field] = value;
        }

        if (new_model->value_.empty()) {
            next_states->emplace_back(new NilModel());
        } else {
            next_states->emplace_back(std::move(new_model));
        }
        break;
    }
    case NilStatus::Ambiguous: {
        // maybe non-nil, inherit property and set value
        std::unique_ptr<HashModel> new_model(new HashModel(*this));
        if (del) {
            new_model->value_.erase(field);
        } else {
            new_model->value_[field] = value;
        }
        if (new_model->value_.empty()) {
            next_states->emplace_back(new NilModel());
        } else {
            next_states->emplace_back(std::move(new_model));
        }

        // maybe nil, clear property and set value
        if (del) {
            next_states->emplace_back(new NilModel());
        } else {
            std::unique_ptr<HashModel> new_model(new HashModel());
            new_model->value_[field] = value;
            new_model->property_.SetNonNil();
            next_states->emplace_back(std::move(new_model));
        }
        break;
    }
    }
}

Status HashModel::ApplyInternal(const ApplyOptions& opts, const hash2::GetRequest& request,
                                const hash2::GetResponse& response, const Operation& op,
                                std::vector<std::unique_ptr<Model>>* next_states) const {
    next_states->clear();
    switch (op.code()) {
    case kOK: {
        // request success
        if (property_.IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNil) {
            return Status::Internal("Model is nil but request success");
        }

        auto it = value_.find(request.field());
        if (response.exist() && it == value_.end()) {
            return Status::Internal("Field lost");
        }
        if (!response.exist() && it != value_.end()) {
            return Status::Internal("Field lost");
        }
        if (response.exist() && it != value_.end()) {
            if (it->second != response.value()) {
                return Status::Internal("Value not match");
            }
        }

        next_states->emplace_back(new HashModel(*this));
        return Status::OK();
    }

    case kNotFound: {
        // key not found
        if (property_.IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNonNil) {
            return Status::Internal("Model is not nil but not found");
        }
        next_states->emplace_back(new NilModel());
        return Status::OK();
    }

    default: {
        // request failure
        if (property_.IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNil) {
            next_states->emplace_back(new NilModel());
        } else {
            next_states->emplace_back(new HashModel(*this));
        }
        return Status::OK();
    }
    }
}

Status HashModel::ApplyInternal(const ApplyOptions& opts, const hash2::DelRequest& request,
                                const hash2::DelResponse& response, const Operation& op,
                                std::vector<std::unique_ptr<Model>>* next_states) const {
    next_states->clear();
    switch (op.code()) {
    case kOK: {
        // request success
        DoApplyValue(opts, op.start_time_us(), op.end_time_us(), request.field(), "", true,
                     next_states);
        return Status::OK();
    }

    case kNotFound: {
        // key not found
        if (property_.IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNonNil) {
            return Status::Internal("Model is not nil but not found");
        }
        next_states->emplace_back(new NilModel());
        return Status::OK();
    }

    default: {
        // request failure

        // maybe not execute
        if (property_.IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNil) {
            next_states->emplace_back(new NilModel());
        } else {
            next_states->emplace_back(new HashModel(*this));
        }

        // maybe executed
        DoApplyValue(opts, op.start_time_us(), op.end_time_us(), request.field(), "", true,
                     next_states);
        return Status::OK();
    }
    }
}

Status HashModel::ApplyInternal(const ApplyOptions& opts, const hash2::GetAllRequest& request,
                                const hash2::GetAllResponse& response, const Operation& op,
                                std::vector<std::unique_ptr<Model>>* next_states) const {
    next_states->clear();
    switch (op.code()) {
    case kOK: {
        // request success
        if (property_.IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNil) {
            return Status::Internal("Model is nil but request success");
        }

        if (value_.size() != static_cast<size_t>(response.values().size())) {
            return Status::Internal("Field num not match");
        }
        for (int i = 0; i < response.fields().size(); ++i) {
            auto it = value_.find(response.fields(i));
            if (it == value_.end()) {
                return Status::Internal("Field not found");
            }
            if (it->second != response.values(i)) {
                return Status::Internal("Value not match");
            }
        }

        next_states->emplace_back(new HashModel(*this));
        return Status::OK();
    }

    case kNotFound: {
        // key not found
        if (property_.IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNonNil) {
            return Status::Internal("Model is not nil but not found");
        }
        next_states->emplace_back(new NilModel());
        return Status::OK();
    }

    default: {
        // request failure
        if (property_.IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNil) {
            next_states->emplace_back(new NilModel());
        } else {
            next_states->emplace_back(new HashModel(*this));
        }
        return Status::OK();
    }
    }
}

}  // namespace bench
}  // namespace bcache2
