// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once
#include <byte/include/assert.h>

#include <string>

#include "common/status.h"
#include "protocol/bench.pb.h"
#include "protocol/server.pb.h"

namespace bcache2 {
namespace bench {

class TimeInterval {
 public:
    TimeInterval() = default;
    ~TimeInterval() = default;

    void reset() {
        start_time_ms_ = end_time_ms_ = ttl_ = UINT64_MAX;
    }

    void reset(uint64_t op_start_time_ms, uint64_t op_end_time_ms, uint64_t ttl_ms) {
        if (ttl_ms > 0) {
            start_time_ms_ = addInterval(op_start_time_ms, ttl_ms);
            end_time_ms_ = addInterval(op_end_time_ms, ttl_ms);
            ttl_ = ttl_ms;
        } else {
            reset();
        }
    }

    int match(const TimeInterval& t, uint64_t ambiguous_ms) const {
        // TTL cannot be greater than or equal to the set value
        if (t.ttl_ > ttl_) {
            return -1;
        // TODO(lzq): Optimize code
        } else if (addInterval(end_time_ms_, ambiguous_ms) <
                        addInterval(t.start_time_ms_, -ambiguous_ms) &&
                   addInterval(start_time_ms_, -ambiguous_ms) >
                        addInterval(t.end_time_ms_, ambiguous_ms)) {
            return -1;
        }
        return 0;
    }

    int compare(uint64_t time_point) const {
        if (time_point > end_time_ms_) {
            return -1;
        } else if (time_point < start_time_ms_) {
            return 1;
        }
        return 0;
    }

    uint64_t addInterval(uint64_t time, int64_t interval) const {
        if (interval >= 0) {
            return (UINT64_MAX - interval > time ? time + interval : UINT64_MAX);
        } else {
            interval = - interval;
            return (uint64_t(interval) < time ? time - interval : 0);
        }
    }

 private:
    uint64_t ttl_ = UINT64_MAX;
    uint64_t start_time_ms_ = UINT64_MAX;
    uint64_t end_time_ms_ = UINT64_MAX;
};

}  // namespace bench
}  // namespace bcache2
