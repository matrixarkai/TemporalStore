// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "common/token_bucket.h"

namespace bcache2 {

SimpleTokenBucket::SimpleTokenBucket(uint64_t rate, uint64_t burst) : rate_(rate), burst_(burst) {
    if (rate == 0) {
        return;
    }

    time_per_token_ = 1000 * 1000 * 1000 / rate;
    time_per_burst_ = time_per_token_ * burst;
}

bool SimpleTokenBucket::ConsumeWithoutWait(uint64_t tokens, uint64_t now) {
    uint64_t new_time = CalcFutureTime(tokens, now);
    if (new_time > now) {
        return false;
    }

    last_time_ = new_time;
    return true;
}

uint64_t SimpleTokenBucket::GetLeftToken(uint64_t now) {
    if (now <= last_time_) {
        return 0;
    }
    if (now >= last_time_ + time_per_burst_) {
        return burst_;
    }
    return (now - last_time_) / time_per_token_;
}

uint64_t SimpleTokenBucket::CalcFutureTime(uint64_t tokens, uint64_t now) {
    uint64_t need_time = tokens * time_per_token_;
    if (now < time_per_burst_) {
        return now;
    }

    uint64_t min_time = now - time_per_burst_;
    uint64_t new_time = need_time;  // first init as the incr value
    if (min_time > last_time_) {
        new_time += min_time;
    } else {
        new_time += last_time_;
    }
    return new_time;
}
}  // namespace bcache2
