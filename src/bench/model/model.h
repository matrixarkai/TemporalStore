// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/assert.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "extension/common/interface.pb.h"
#include "protocol/bench.pb.h"

namespace bcache2 {
namespace bench {

// model interface
class Model {
 public:
    struct ApplyOptions {
        uint64_t max_expire_ambiguous_time_ms = 0;
    };

    Model() = default;
    virtual ~Model() = default;

    virtual Status Apply(const ApplyOptions& opts, const Operation& op,
                         std::vector<std::unique_ptr<Model>>* next_states) const = 0;
    virtual std::string ToString() const = 0;
};

inline std::ostream& operator<<(std::ostream& os, const Model& model) {
    return os << model.ToString();
}

enum class NilStatus {
    DefinitelyNil,
    Ambiguous,
    DefinitelyNonNil,
};

class ModelProperty {
 public:
    ModelProperty() : nil_(true) {}

    NilStatus IsNil(uint64_t start_ts_us, uint64_t end_ts_us) const {
        if (nil_) {
            return NilStatus::DefinitelyNil;
        }

        if (ttl_ms_ == 0) {
            return NilStatus::DefinitelyNonNil;
        }

        if (start_ts_us > max_expire_ts_us_) {
            return NilStatus::DefinitelyNil;
        }

        if (end_ts_us < min_expire_ts_us_) {
            return NilStatus::DefinitelyNonNil;
        }

        return NilStatus::Ambiguous;
    }

    void SetNonNil() { nil_ = false; }
    void SetTtl(uint64_t start_ts_us, uint64_t end_ts_us, uint64_t ttl_ms,
                uint64_t max_expire_ambiguous_time_ms) {
        if (ttl_ms == 0) {
            ttl_ms_ = min_expire_ts_us_ = max_expire_ts_us_ = 0;
        } else {
            ttl_ms_ = ttl_ms;
            min_expire_ts_us_ =
                start_ts_us + ttl_ms * 1000 < max_expire_ambiguous_time_ms * 1000
                    ? 0
                    : start_ts_us + ttl_ms * 1000 - max_expire_ambiguous_time_ms * 1000;
            max_expire_ts_us_ = end_ts_us + ttl_ms * 1000 + max_expire_ambiguous_time_ms * 1000;
        }
    }

    Status Apply(const Model::ApplyOptions& opts, const Operation& op,
                 std::vector<ModelProperty>* next_properties) const;
    std::string ToString() const;

 private:
    Status ApplyInternal(const Model::ApplyOptions& opts, const common2::DelObjectRequest& request,
                         const common2::DelObjectResponse& response, const Operation& op,
                         std::vector<ModelProperty>* next_properties) const;
    Status ApplyInternal(const Model::ApplyOptions& opts, const common2::ExpireRequest& request,
                         const common2::ExpireResponse response, const Operation& op,
                         std::vector<ModelProperty>* next_properties) const;
    Status ApplyInternal(const Model::ApplyOptions& opts, const common2::TtlRequest& request,
                         const common2::TtlResponse& response, const Operation& op,
                         std::vector<ModelProperty>* next_properties) const;

    bool nil_ = false;
    uint64_t ttl_ms_ = 0;
    uint64_t min_expire_ts_us_ = 0;
    uint64_t max_expire_ts_us_ = 0;
};

inline std::string ModelProperty::ToString() const {
    std::stringstream ss;
    ss << "ModelProperty{";
    ss << "Nil=" << (nil_ ? "True" : "False") << ", TtlMs=" << ttl_ms_
       << ", MinExpireTsUs=" << min_expire_ts_us_ << ", MaxExpireTsUs=" << max_expire_ts_us_;
    ss << "}";
    return ss.str();
}

}  // namespace bench
}  // namespace bcache2
