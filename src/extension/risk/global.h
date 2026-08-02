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
#include "extension/risk/define.h"
#include "extension/risk/interface.pb.h"
#include "extension/risk/window.h"

namespace bcache2 {
namespace risk_tool {

inline int64_t parseWindowDurationFromPrecision(risk::RiskPrecision precision) {
    switch (precision) {
    case risk::OneSecond:
        return 1;
    case risk::FiveSeconds:
        return 5;
    case risk::TenSeconds:
        return 10;
    case risk::OneMinute:
        return 60;
    case risk::FiveMinutes:
        return 300;
    case risk::TenMinutes:
        return 600;
    case risk::OneHour:
        return 3600;
    case risk::OneDay:
        return 86400;
    default:
        return -1;
    }
}

struct PrecisionInfo {
    PrecisionInfo(risk::RiskPrecision w, int l)
        : level(l), window(w), windowDuration(parseWindowDurationFromPrecision(w)) {
        if (windowDuration == -1) {
            windowDuration = 0;
            LOG(WARNING) << "[risk]Gen PrecisionInfo failed, precision not handle, got = " << w
                         << std::endl;
        }
    }
    int level;
    risk::RiskPrecision window;
    int64_t windowDuration;
};

class RiskGlobalData {
 public:
    static RiskGlobalData& getSingleton() {
        static RiskGlobalData _singleton_instance;
        return _singleton_instance;
    }

    const std::vector<const PrecisionInfo*> getPrecisionLevelInfo(risk::RiskPrecision precision) {
        return precisionMap[precision];
    }
    const int64_t getTimeZoneOffsetSecond() { return timeZoneOffset; }

 private:
    RiskGlobalData() {
        initPrecisionMap();
        initTimeZoneOffset();
    }
    ~RiskGlobalData() {}
    void initPrecisionMap() {
        static const std::unordered_map<risk::RiskPrecision, std::vector<risk::RiskPrecision>>
            preMap = {
                {risk::OneSecond, {risk::OneSecond, risk::OneMinute}},
                {risk::FiveSeconds, {risk::FiveSeconds, risk::FiveMinutes}},
                {risk::TenSeconds, {risk::TenSeconds, risk::TenMinutes}},
                {risk::OneMinute, {risk::OneMinute, risk::OneHour}},
                {risk::FiveMinutes, {risk::FiveMinutes, risk::OneHour}},
                {risk::TenMinutes, {risk::TenMinutes, risk::OneHour}},
                {risk::OneHour, {risk::OneHour, risk::OneDay}},
                {risk::OneDay, {risk::OneDay}},
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
    std::unordered_map<risk::RiskPrecision, const std::vector<const PrecisionInfo*>> precisionMap;
    // 时区偏移
    int64_t timeZoneOffset = 0;

    DISALLOW_COPY_AND_ASSIGN(RiskGlobalData);
};

}  // namespace risk_tool
}  // namespace bcache2
