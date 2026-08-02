// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "bench/model/model.h"

#include "extension/modules.pb.h"

namespace bcache2 {
namespace bench {

Status ModelProperty::Apply(const Model::ApplyOptions& opts, const Operation& op,
                            std::vector<ModelProperty>* next_properties) const {
    if (op.module_id() != Module::COMMON) {
        return Status::FailedPrecondition("Invalid module type");
    }

    switch (op.function_id()) {
    case common2::DEL_OBJECT: {
        common2::DelObjectRequest req;
        common2::DelObjectResponse resp;
        BYTE_ASSERT(req.ParseFromString(op.request_bytes()));
        BYTE_ASSERT(resp.ParseFromString(op.response_bytes()));
        return ApplyInternal(opts, req, resp, op, next_properties);
    }
    case common2::EXPIRE: {
        common2::ExpireRequest req;
        common2::ExpireResponse resp;
        BYTE_ASSERT(req.ParseFromString(op.request_bytes()));
        BYTE_ASSERT(resp.ParseFromString(op.response_bytes()));
        return ApplyInternal(opts, req, resp, op, next_properties);
    }
    case common2::TTL: {
        common2::TtlRequest req;
        common2::TtlResponse resp;
        BYTE_ASSERT(req.ParseFromString(op.request_bytes()));
        BYTE_ASSERT(resp.ParseFromString(op.response_bytes()));
        return ApplyInternal(opts, req, resp, op, next_properties);
    }
    }

    return Status::FailedPrecondition("Cmd not support");
}

Status ModelProperty::ApplyInternal(const Model::ApplyOptions& opts,
                                    const common2::DelObjectRequest& request,
                                    const common2::DelObjectResponse& response, const Operation& op,
                                    std::vector<ModelProperty>* next_properties) const {
    next_properties->clear();
    switch (op.code()) {
    case kOK: {
        // request success
        if (IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNil) {
            return Status::Internal("Model is nil but request success");
        }

        next_properties->emplace_back(ModelProperty());
        return Status::OK();
    }

    case kNotFound: {
        // key not found
        if (IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNonNil) {
            return Status::Internal("Model is non-nil but not found");
        }
        next_properties->emplace_back(ModelProperty());
        return Status::OK();
    }

    default: {
        // request failure

        // maybe not execute
        if (IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNil) {
            next_properties->emplace_back(ModelProperty());
        } else {
            next_properties->emplace_back(ModelProperty(*this));
        }

        // maybe executed
        if (IsNil(op.start_time_us(), op.end_time_us()) != NilStatus::DefinitelyNil) {
            next_properties->emplace_back(ModelProperty());
        }
        return Status::OK();
    }
    }
}

Status ModelProperty::ApplyInternal(const Model::ApplyOptions& opts,
                                    const common2::ExpireRequest& request,
                                    const common2::ExpireResponse response, const Operation& op,
                                    std::vector<ModelProperty>* next_properties) const {
    next_properties->clear();
    switch (op.code()) {
    case kOK: {
        // request success
        if (IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNil) {
            return Status::Internal("Model is nil but request success");
        }

        ModelProperty property = *this;
        property.SetTtl(op.start_time_us(), op.end_time_us(), request.ttl_ms(),
                        opts.max_expire_ambiguous_time_ms);
        next_properties->emplace_back(property);
        return Status::OK();
    }

    case kNotFound: {
        // key not found
        if (IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNonNil) {
            return Status::Internal("Model is non-nil but not found");
        }
        next_properties->emplace_back(ModelProperty());
        return Status::OK();
    }

    default: {
        // request failure

        // maybe not execute
        if (IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNil) {
            next_properties->emplace_back(ModelProperty());
        } else {
            next_properties->emplace_back(ModelProperty(*this));
        }

        // maybe executed
        if (IsNil(op.start_time_us(), op.end_time_us()) != NilStatus::DefinitelyNil) {
            ModelProperty property = *this;
            property.SetTtl(op.start_time_us(), op.end_time_us(), request.ttl_ms(),
                            opts.max_expire_ambiguous_time_ms);
            next_properties->emplace_back(property);
        }
        return Status::OK();
    }
    }
}

Status ModelProperty::ApplyInternal(const Model::ApplyOptions& opts,
                                    const common2::TtlRequest& request,
                                    const common2::TtlResponse& response, const Operation& op,
                                    std::vector<ModelProperty>* next_properties) const {
    next_properties->clear();
    switch (op.code()) {
    case kOK: {
        // request success
        if (IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNil) {
            return Status::Internal("Model is nil but request success");
        }

        if (ttl_ms_ > 0 && response.ttl_ms() == 0) {
            return Status::Internal("Ttl lost");
        }
        if (ttl_ms_ == 0 && response.ttl_ms() > 0) {
            return Status::Internal("Ttl not match");
        }
        if (ttl_ms_ > 0 && response.ttl_ms() > 0) {
            uint64_t req_min_expire_ts_us = op.start_time_us() + response.ttl_ms() * 1000 <
                                                    opts.max_expire_ambiguous_time_ms * 1000
                                                ? 0
                                                : op.start_time_us() + response.ttl_ms() * 1000 -
                                                      opts.max_expire_ambiguous_time_ms * 1000;
            uint64_t req_max_expire_ts_us = op.start_time_us() + response.ttl_ms() * 1000 +
                                            opts.max_expire_ambiguous_time_ms * 1000;
            if (req_min_expire_ts_us > max_expire_ts_us_ ||
                req_max_expire_ts_us < min_expire_ts_us_) {
                return Status::Internal("Ttl not match");
            }
        }

        // calibration ttl
        ModelProperty property = *this;
        property.SetTtl(op.start_time_us(), op.end_time_us(), response.ttl_ms(),
                        opts.max_expire_ambiguous_time_ms);
        next_properties->emplace_back(property);
        return Status::OK();
    }

    case kNotFound: {
        // key not found
        if (IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNonNil) {
            return Status::Internal("Model is non-nil but not found");
        }
        next_properties->emplace_back(ModelProperty());
        return Status::OK();
    }

    default: {
        // request failure
        if (IsNil(op.start_time_us(), op.end_time_us()) == NilStatus::DefinitelyNil) {
            next_properties->emplace_back(ModelProperty());
        } else {
            next_properties->emplace_back(ModelProperty(*this));
        }
        return Status::OK();
    }
    }
}

}  // namespace bench
}  // namespace bcache2
