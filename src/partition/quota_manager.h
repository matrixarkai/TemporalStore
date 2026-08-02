// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>

#include "common/token_bucket.h"
#include "protocol/config.pb.h"

namespace bcache2 {
namespace partition {

enum class QuotaType { WriteQuota, ReadQuota };

class QuotaManager {
 public:
    explicit QuotaManager(const LimitConfig& limiter_config) { ResetInternal(limiter_config); }
    QuotaManager() {}
    ~QuotaManager() {}

    void UpdateConfig(const LimitConfig& limiter_config) { ResetInternal(limiter_config); }

    bool ConsumeQuota(QuotaType quota_type) {
        switch (quota_type) {
        case QuotaType::WriteQuota:
            if (!write_limiter_) return true;
            return write_limiter_->ConsumeWithoutWait(1);
        case QuotaType::ReadQuota:
            if (!read_limiter_) return true;
            return read_limiter_->ConsumeWithoutWait(1);
        }
        return true;
    }

 private:
    void ResetInternal(const LimitConfig& limiter_config) {
        if (limiter_config.write_limiter().qps().value() != 0) {
            write_limiter_.reset(
                new SimpleTokenBucket(limiter_config.write_limiter().qps().value(),
                                      limiter_config.write_limiter().burst().value()));
        } else {
            write_limiter_.reset();
        }
        if (limiter_config.read_limiter().qps().value() != 0) {
            read_limiter_.reset(
                new SimpleTokenBucket(limiter_config.read_limiter().qps().value(),
                                      limiter_config.read_limiter().burst().value()));
        } else {
            read_limiter_.reset();
        }
    }
    std::unique_ptr<SimpleTokenBucket> write_limiter_{nullptr};
    std::unique_ptr<SimpleTokenBucket> read_limiter_{nullptr};
};

}  // namespace partition
}  // namespace bcache2
