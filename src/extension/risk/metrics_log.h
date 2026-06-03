// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/strings/string_view.h>

#include <string>
#include <vector>

#include "common/logging.h"
#include "common/macros.h"
#include "common/time.h"

namespace bcache2 {
namespace risk {

const int64_t NeedLogLongCostNs = 100 * 1000;  // 100us

class RiskTimerLogger {
 public:
    struct CheckPoint {
        absl::string_view name;
        uint64_t time_point_ns = 0;

        CheckPoint(const absl::string_view& name, uint64_t time_point_ns)
            : name(name), time_point_ns(time_point_ns) {}
    };
    RiskTimerLogger(const absl::string_view& method, const absl::string_view& key, int operType)
        : method_(method), key_(key), oper_type_(operType) {
        logThresholdNs_ = NeedLogLongCostNs;
        AddCheckPoint("start");
    }
    RiskTimerLogger(const absl::string_view& method, const absl::string_view& key,
        int operType, int64_t logThresholdNs)
        : method_(method), key_(key), oper_type_(operType), logThresholdNs_(logThresholdNs) {
        AddCheckPoint("start");
    }
    int AddCheckPoint(const absl::string_view& name) {
        events_.emplace_back(CheckPoint(name, bcache2::GetCurrentTimeInNs()));
        return events_.size() - 1;;
    }
    ~RiskTimerLogger() {
        int64_t end = bcache2::GetCurrentTimeInNs();
        if (end - static_cast<int64_t>(events_[0].time_point_ns) > logThresholdNs_) {
            events_.emplace_back(CheckPoint("end", end));
            LOG(WARNING) << "[risk]long operate found, method=" << method_ << " key=" << key_
            << " opType=" << oper_type_ <<  " cost=" << costToString() << std::endl;
        }
    }

 private:
    std::string costToString() const {
        std::ostringstream oss;
        oss << "[ ";
        for (size_t i = 1; i < events_.size(); ++i) {
            oss << events_[i].name << ":" << events_[i].time_point_ns - events_[i - 1].time_point_ns
                << " ";
        }
        oss << " total:" << events_[events_.size() - 1].time_point_ns - events_[0].time_point_ns
            << " ]";
        return oss.str();
    }

 private:
    std::vector<CheckPoint> events_;
    absl::string_view method_;
    absl::string_view key_;
    int oper_type_;
    int64_t logThresholdNs_;

    DISALLOW_COPY_AND_ASSIGN(RiskTimerLogger);
};

}  // namespace risk
}  // namespace bcache2
