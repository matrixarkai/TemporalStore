// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
// #include "bcache/server/ips_interface/profile_time_dimension.h"

#include "model/ips/profile_time_dimension.h"

#include <cinttypes>
#include <memory>

#include "model/ips/profile_parse_time_range.h"
#include "model/ips/profile_time_range.h"
namespace bcache2 {
namespace ips {

TimeDimension::TimeDimension(const TimeDimension& td) {
    for (const auto& node : td.node_list_) {
        node_list_.emplace_back(node);
    }
    start_time_type_ = td.start_time_type_;
    ResetCursor();
}

bool TimeDimension::Init(const rapidjson::Value& val, const std::string& start_time_type) {
    if (start_time_type == "absolute") {
        start_time_type_ = IP_ABSOLUTE_TIME;
    } else if (start_time_type == "relative") {
        start_time_type_ = IP_RELATIVE_TIME;
    } else {
        // BC_ERROR("TimeDimension::init invalid compress start time type: {}",
        // start_time_type.c_str());
        return false;
    }

    std::vector<time_snap> v;
    ParseTimeSnapConfigFromJson(val, &v);

    CompactIntervalVec compact_intervals;
    for (const auto& range : v) {
        int64_t precision = range.precision;
        int64_t start = range.start;
        int64_t end = range.end;

        if (start < 0 || start >= end || precision > (end - start)) {
            // BC_ERROR("invalid compact range， start_ts: {}, end_ts: {}, presicion: {}", start,
            // end,
            //          precision);
            return false;
        }

        int64_t cur_start = start;
        while (cur_start < end) {
            int64_t cur_end = cur_start + precision;
            if (cur_end > end) {
                cur_end = end;
            }

            compact_intervals.emplace_back(std::make_pair(cur_start, cur_end));
            cur_start = cur_end;
        }
    }

    bool is_valid = true;
    for (size_t i = 0; i < compact_intervals.size(); ++i) {
        auto const& cur_range = compact_intervals[i];
        int64_t cur_start = cur_range.first;
        int64_t cur_end = cur_range.second;
        if (i != 0) {
            int64_t last_end = compact_intervals[i - 1].second;
            if (last_end != cur_start) {
                is_valid = false;
                // BC_WARN("invalid range: last_end: {}, cur_start: {}", last_end, cur_start);
            }
        }
        if (cur_start >= cur_end) {
            is_valid = false;
            // BC_WARN("invalid range: cur_start: {}, cur_end: {}", cur_start, cur_end);
        }
    }
    if (is_valid) {
        ReplaceCompactIntervals(std::make_shared<CompactIntervalVec>(std::move(compact_intervals)));
    }
    return is_valid;
}

bool TimeDimensionNode::GetNextTimeRange(const int64_t now, IpsTimeRange* range) {
    if (cursor_ >= e_ts_ms_ || cursor_ > now) {
        return false;
    }

    int64_t start = now - cursor_;
    cursor_ += precision_;
    int64_t end = now - cursor_;

    if (cursor_ < 0 || cursor_ > now) {
        end = 0;
    }
    range->set(start, end);
    return true;
}

Status TimeDimension::AddRange(int64_t s_ts_ms, int64_t e_ts_ms, int64_t precision) {
    if (e_ts_ms < 0 || s_ts_ms > e_ts_ms || precision > (e_ts_ms - s_ts_ms)) {
        // BC_ERROR_DEFAULT_RATE_LIMIT("invalid time range, start_ts: {}, end_ts: {}, presicion:
        // {}", s_ts_ms,
        //                             e_ts_ms, precision);
        return Status::InvalidArgument("invaild");
    }
    if (!node_list_.empty()) {
        if (node_list_.back().e_ts_ms_ != s_ts_ms) {
            // BC_ERROR_DEFAULT_RATE_LIMIT("invalid time range, start_ts: {}, end_ts: {}, presicion:
            // {}",
            //                             s_ts_ms, e_ts_ms, precision);

            return Status::InvalidArgument("invaild");
        }
    }
    TimeDimensionNode node(s_ts_ms, e_ts_ms, precision);
    // BC_ERROR_DEFAULT_RATE_LIMIT("add one time range for compact, start_ts: {}, end_ts: {},
    // presicion: {}",
    //                             s_ts_ms, e_ts_ms, precision);

    node_list_.emplace_back(node);
    return Status::OK();
}

bool TimeDimension::GetNextTimeRange(int64_t now, IpsTimeRange* range) {
    while (iter_ != node_list_.end()) {
        if (iter_->GetNextTimeRange(now, range)) {
            return true;
        }
        iter_++;
    }
    return false;
}

void TimeDimension::ResetCursor() {
    iter_ = node_list_.begin();
    for (auto& node : node_list_) {
        node.cursor_ = node.s_ts_ms_;
    }
}

}  // namespace ips
}  // namespace bcache2
