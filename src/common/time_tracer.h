// Copyright (c) 2020-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/strings/string_view.h>
#include <byte/include/macros.h>

#include <sstream>
#include <string>
#include <vector>

#include "common/time.h"

namespace bcache2 {

class TimeTracer {
 public:
    struct Event {
        absl::string_view name;
        uint64_t time_point_ns = 0;

        Event(const absl::string_view& name, uint64_t time_point_ns)
            : name(name), time_point_ns(time_point_ns) {}
    };

    TimeTracer() { events_.emplace_back("start", GetCurrentTimeInNs()); }
    ~TimeTracer() {}

    // in most cases, name is a pure string
    // e.g. tracer.AddEvent("load_index")
    void AddEvent(const absl::string_view& name) {
        events_.emplace_back(name, GetCurrentTimeInNs());
    }

    // TODO(wangtai.10): better format performance
    std::string ToString() const {
        std::ostringstream oss;
        oss << "cost_ns=[ ";
        for (size_t i = 1; i < events_.size(); ++i) {
            oss << events_[i].name << ":" << events_[i].time_point_ns - events_[i - 1].time_point_ns
                << " ";
        }
        oss << "total:" << TotalSpentNs() << " ]";
        return oss.str();
    }

    uint64_t TotalSpentNs() const { return GetCurrentTimeInNs() - events_[0].time_point_ns; }
    uint64_t TotalSpentUs() const { return TotalSpentNs() / 1000; }
    uint64_t TotalSpentMs() const { return TotalSpentUs() / 1000; }
    uint64_t TotalSpentS() const { return TotalSpentMs() / 1000; }

 private:
    std::vector<Event> events_;

    DISALLOW_COPY_AND_ASSIGN(TimeTracer);
};

inline std::ostream& operator<<(std::ostream& os, const TimeTracer& tracer) {
    return os << tracer.ToString();
}

}  // namespace bcache2
