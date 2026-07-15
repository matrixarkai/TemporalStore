// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/macros.h>

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "extension/control_state/define.h"
#include "extension/control_state/interface.pb.h"
#include "extension/control_state/window.h"

namespace bcache2 {
namespace control_state_tool {

inline int64_t parseWindowDurationFromPrecision(control_state::ControlStatePrecision precision) {
    switch (precision) {
    case control_state::OneSecond:
        return 1;
    case control_state::FiveSeconds:
        return 5;
    case control_state::TenSeconds:
        return 10;
    case control_state::OneMinute:
        return 60;
    case control_state::FiveMinutes:
        return 300;
    case control_state::TenMinutes:
        return 600;
    case control_state::OneHour:
        return 3600;
    case control_state::OneDay:
        return 86400;
    default:
        return -1;
    }
}

struct PrecisionInfo {
    PrecisionInfo(control_state::ControlStatePrecision w, int l)
        : level(l), window(w), windowDuration(parseWindowDurationFromPrecision(w)) {
        if (windowDuration == -1) {
            windowDuration = 0;
            LOG(WARNING) << "[control_state]Gen PrecisionInfo failed, precision not handle, got = " << w
                         << std::endl;
        }
    }
    int level;
    control_state::ControlStatePrecision window;
    int64_t windowDuration;
};

class ControlStateGlobalData {
 public:
    static ControlStateGlobalData& getSingleton() {
        static ControlStateGlobalData _singleton_instance;
        return _singleton_instance;
    }

    const std::vector<const PrecisionInfo*> getPrecisionLevelInfo(control_state::ControlStatePrecision precision) {
        return precisionMap[precision];
    }
    const int64_t getTimeZoneOffsetSecond() { return timeZoneOffset; }

 private:
    ControlStateGlobalData() {
        initPrecisionMap();
        initTimeZoneOffset();
    }
    ~ControlStateGlobalData() {}
    void initPrecisionMap() {
        static const std::unordered_map<control_state::ControlStatePrecision, std::vector<control_state::ControlStatePrecision>>
            preMap = {
                {control_state::OneSecond, {control_state::OneSecond, control_state::OneMinute}},
                {control_state::FiveSeconds, {control_state::FiveSeconds, control_state::FiveMinutes}},
                {control_state::TenSeconds, {control_state::TenSeconds, control_state::TenMinutes}},
                {control_state::OneMinute, {control_state::OneMinute, control_state::OneHour}},
                {control_state::FiveMinutes, {control_state::FiveMinutes, control_state::OneHour}},
                {control_state::TenMinutes, {control_state::TenMinutes, control_state::OneHour}},
                {control_state::OneHour, {control_state::OneHour, control_state::OneDay}},
                {control_state::OneDay, {control_state::OneDay}},
            };
        for (auto it = preMap.begin(); it != preMap.end(); ++it) {
            std::vector<const PrecisionInfo*> vec;
            for (std::size_t i = 0; i < it->second.size(); ++i) {
                vec.emplace_back(new PrecisionInfo(it->second[i], i));
            }
            precisionMap.insert(std::make_pair(it->first, vec));
        }
    }
    void initTimeZoneOffset() {
        time_t cur_time = time(0);
        tm cur_tm;
        tm utc_tm;
        localtime_r(&cur_time, &cur_tm);
        gmtime_r(&cur_time, &utc_tm);
        time_t utc_time = mktime(&utc_tm);
        // 不处理夏令时
        timeZoneOffset = cur_time - utc_time;
    }

 private:
    // 拆分窗口时的精度信息
    std::unordered_map<control_state::ControlStatePrecision, const std::vector<const PrecisionInfo*>> precisionMap;
    // 时区偏移
    int64_t timeZoneOffset = 0;

    DISALLOW_COPY_AND_ASSIGN(ControlStateGlobalData);
};

}  // namespace control_state_tool
}  // namespace bcache2
