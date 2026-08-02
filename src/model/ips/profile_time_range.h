// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <string>

// #include "bcache/common/logger.h"
// #include "bcache/server/ips_interface/ips_define.h"
#include "model/ips/ips_define.h"

namespace bcache2 {
namespace ips {

// Time range for profile
class IpsTimeRange {
 public:
    static const int64_t UPPER_BOUND = INT64_MAX;
    static const int64_t LOWER_BOUND = -INT64_MAX;
    static const int64_t TIME_UNIT = 1;
    static const int64_t DAY_MICROSECOND = 86400000000;

 public:
    IpsTimeRange() : s_ts_us_(UPPER_BOUND), e_ts_us_(LOWER_BOUND) {}

    // s_ts_us_ > e_ts_us_
    IpsTimeRange(int64_t s, int64_t e) : s_ts_us_(s), e_ts_us_(e) { assert(s >= e); }

    void set(int64_t s, int64_t e) {
        s_ts_us_ = s;
        e_ts_us_ = e;
    }

    void SetStartTsMicros(int64_t s) { s_ts_us_ = s; }

    bool IsCoveredBy(const IpsTimeRange& larger) const {
        return (larger.s_ts_us_ >= s_ts_us_) && (larger.e_ts_us_ <= e_ts_us_);
    }

    // now -> prev -> this
    bool IsAfter(const IpsTimeRange& prev) const { return prev.e_ts_us_ >= s_ts_us_; }

    // 判断是不是this和other两个time_range是不是下面的类型：e1 < s2 < s1
    // other:     s1----------e1
    // this:              s2-------e2
    bool IsAfterOverlap(const IpsTimeRange& other) const {
        return (other.e_ts_us_ < s_ts_us_) && (other.e_ts_us_ > e_ts_us_);
    }

    // now -> this -> next
    bool IsBefore(const IpsTimeRange& next) const { return next.IsAfter(*this); }

    bool IsOverlapping(const IpsTimeRange& other) const {
        return (!IsAfter(other)) && (!IsBefore(other));
    }

    void TryExtend(const IpsTimeRange& other) {
        if (s_ts_us_ < other.s_ts_us_) {
            s_ts_us_ = other.s_ts_us_;
        }
        if (e_ts_us_ > other.e_ts_us_) {
            e_ts_us_ = other.e_ts_us_;
        }
    }

    // return valuse follow as:
    // -1: ts, [st_ts_us_, e_ts_us_)
    // 0: [st_ts_us_, ts, e_ts_us_)
    // 1: [st_ts_us_, e_ts_us_), ts
    int Compare(const int64_t ts) const {
        if (s_ts_us_ < ts) {
            return -1;
        } else if (e_ts_us_ >= ts) {
            return 1;
        } else {
            return 0;
        }
    }

    int64_t GetStartTsMicros() const { return s_ts_us_; }

    int64_t GetEndTsMicros() const { return e_ts_us_; }

    int64_t GetMidTsMs() const { return (s_ts_us_ + e_ts_us_) / 2; }

    IpsTimeRange& operator=(const IpsTimeRange& range) {
        s_ts_us_ = range.s_ts_us_;
        e_ts_us_ = range.e_ts_us_;
        return *this;
    }

    std::string ToString() {
        return "[" + ConvertTimestampToReadableFormat(e_ts_us_) + ", " +
               ConvertTimestampToReadableFormat(s_ts_us_) + "]";
    }

 private:
    int64_t s_ts_us_;  // start timestamp
    int64_t e_ts_us_;  // end timestamp
};

}  // namespace ips
}  // namespace bcache2
