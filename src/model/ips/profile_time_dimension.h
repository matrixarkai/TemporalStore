// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once
// #include <idl/instance_profile_idl/instance/instance_profile_types.h>
#include <butil/containers/doubly_buffered_data.h>

// #include <butil/third_party/rapidjson/document.h>
#include <rapidjson/document.h>

#include <list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "model/ips/ips_define.h"
#include "model/ips/profile_time_range.h"
// #include "bcache/common/status.h"
// #include "bcache/server/ips_interface/ips_define.h"
// #include "bcache/server/ips_interface/profile_time_range.h"

namespace bcache2 {
namespace ips {

// typedef idl::data::instance::ErrorCode ErrorCode;

// One node share the same precision
class TimeDimensionNode {
    friend class TimeDimension;

 public:
    TimeDimensionNode(int64_t s, int64_t e, int64_t p)
        : s_ts_ms_(s), e_ts_ms_(e), precision_(p), cursor_(s) {}

    TimeDimensionNode(const TimeDimensionNode& node)
        : TimeDimensionNode(node.s_ts_ms_, node.e_ts_ms_, node.precision_) {}

    bool GetNextTimeRange(const int64_t now, IpsTimeRange* range);

 private:
    int64_t s_ts_ms_;
    int64_t e_ts_ms_;
    int64_t precision_;
    int64_t cursor_;
};

// Control compaction range
class TimeDimension {
 public:
    using CompactIntervalVec = std::vector<std::pair<int64_t, int64_t>>;
    using CompactIntervalVecPtr = std::shared_ptr<CompactIntervalVec>;

    TimeDimension() {}
    TimeDimension(const TimeDimension& td);

    // Init time dimension fron json
    bool Init(const rapidjson::Value& val, const std::string& start_time_type);

    DateType GetStartTimeType() const { return start_time_type_; }

    // Called when being inited
    Status AddRange(int64_t s_ts_ms, int64_t e_ts_ms, int64_t precision);

    // Range iterator
    bool GetNextTimeRange(const int64_t now, IpsTimeRange* range);

    // Reset iterator value
    void ResetCursor();

    size_t GetCompactRangeTotalSize() const {
        return compact_intervals_->size();
    }

    void ReplaceCompactIntervals(CompactIntervalVecPtr new_compact_intervals) {
        assert(new_compact_intervals->size() > 0);
        compact_intervals_ = new_compact_intervals;
    }

    std::pair<int64_t, int64_t> GetCompactIntervals(size_t index) const {
        if (UNLIKELY(index >= compact_intervals_->size())) {
            return {-1, -1};
        } else {
            return compact_intervals_->at(index);
        }
    }

    CompactIntervalVecPtr GetCompactRange() const {
        return compact_intervals_;
    }

 private:
    // Iterator when GetNextTimeRange() is called
    std::list<TimeDimensionNode>::iterator iter_;
    std::list<TimeDimensionNode> node_list_;
    // Compress start time type
    DateType start_time_type_;

    CompactIntervalVecPtr compact_intervals_ = nullptr;
};

}  // namespace ips
}  // namespace bcache2
