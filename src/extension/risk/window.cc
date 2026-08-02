// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "extension/risk/window.h"

#include <regex>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "extension/risk/define.h"
#include "extension/risk/global.h"

namespace bcache2 {
namespace risk_tool {

// ============ 方法实现 ============
std::string buildFieldPrefix(int64_t start, risk::RiskPrecision window) {
    return std::to_string(window) + std::to_string(start);
}
std::pair<int64_t, risk::RiskPrecision> getWindowPair(int64_t* start, const PrecisionInfo* pinfo) {
    std::pair<int64_t, risk::RiskPrecision> res = std::make_pair(
        (*start) - RiskGlobalData::getSingleton().getTimeZoneOffsetSecond(), pinfo->window);
    *start += pinfo->windowDuration;
    return res;
}

// TODO(qiankaihua): mktime 修改成 timestamp 取模的形式, 需要处理时区
time_t fixTimeWithPrecision(const int64_t occurTime, const std::string& key,
                            const risk::RiskPrecision riskPrecision) {
    tm t;
    if (occurTime == 0) {  // 选择当前时间
        time_t timestamp;
        time(&timestamp);
        localtime_r(&timestamp, &t);
    } else {
        localtime_r(&occurTime, &t);
    }
    switch (riskPrecision) {
    case risk::OneDay:
        t.tm_hour = t.tm_min = t.tm_sec = 0;
        break;
    case risk::OneHour:
        t.tm_min = t.tm_sec = 0;
        break;
    case risk::TenMinutes:
        t.tm_sec = 0;
        t.tm_min = t.tm_min - t.tm_min % 10;
        break;
    case risk::FiveMinutes:
        t.tm_sec = 0;
        t.tm_min = t.tm_min - t.tm_min % 5;
        break;
    case risk::OneMinute:
        t.tm_sec = 0;
        break;
    case risk::TenSeconds:
        t.tm_sec = t.tm_sec - t.tm_sec % 10;
        break;
    case risk::FiveSeconds:
        t.tm_sec = t.tm_sec - t.tm_sec % 5;
        break;
    case risk::OneSecond:
        break;
    default:
        LOG(WARNING) << "[risk]key = " << key << " "
                     << "fixTime failed, precision not handle, got = " << riskPrecision
                     << std::endl;
        return -1;
    }
    return mktime(&t);
}
// 时间修正, now 和 request 不能为空
time_t fixTime(const tm& now, const risk::Window& window, const risk::RiskPrecision riskPrecision,
               bool isEnd, bool needSplit) {
    tm t = tm{now};
    int64_t w = 0;
    switch (riskPrecision) {
    case risk::OneDay:
        t.tm_hour = t.tm_min = t.tm_sec = 0;
        break;
    case risk::OneHour:
        t.tm_min = t.tm_sec = 0;
        break;
    case risk::TenMinutes:
        t.tm_sec = 0;
        t.tm_min = t.tm_min - t.tm_min % 10;
        break;
    case risk::FiveMinutes:
        t.tm_sec = 0;
        t.tm_min = t.tm_min - t.tm_min % 5;
        break;
    case risk::OneMinute:
        t.tm_sec = 0;
        break;
    case risk::TenSeconds:
        t.tm_sec = t.tm_sec - t.tm_sec % 10;
        break;
    case risk::FiveSeconds:
        t.tm_sec = t.tm_sec - t.tm_sec % 5;
        break;
    case risk::OneSecond:
        break;
    default:
        // RISK_LOG(WARNING, request) << "fixTime failed, precision not handle, got = "
        //     << request.precision() << std::endl;
        return -1;
    }
    if (isEnd) {
        if (window.end() == 0) {
            // 窗口结束, 并且以当前时间结束, fix 之后加上一次精度
            return mktime(&t) + parseWindowDurationFromPrecision(riskPrecision);
        } else {
            w = -window.end() - 1;
        }
    } else {
        w = -window.start();
    }
    // tm 结构体可以减到负数, 最终转换为时间戳的时候没有影响
    // (目前仅测试了 tm_mday, tm_hour, tm_min, tm_sec 四个字段正确)
    switch (window.unit()) {
    case risk::Day:
        t.tm_mday -= w;
        break;
    case risk::Hour:
        t.tm_hour -= w;
        break;
    case risk::Minute:
        t.tm_min -= w;
        break;
    case risk::Second:
        t.tm_sec -= w;
        break;
    default:
        // RISK_LOG(WARNING, request) << "fixTime failed, unit not handle, got = "
        //     << request.unit() << std::endl;
        return -1;
    }
    return mktime(&t);
}
// 获取窗口起始、结束时间点 -> [start, end)
int getWindowTimes(const risk::Window& window, const risk::RiskPrecision riskPrecision,
                   time_t timestamp, bool needSplit, int64_t* start, int64_t* end) {
    tm t;
    localtime_r(&timestamp, &t);
    *start = fixTime(t, window, riskPrecision, false, needSplit);
    *end = fixTime(t, window, riskPrecision, true, needSplit);
    if (*start == -1 || *end == -1) {
        // RISK_LOG(WARNING, request) << "getWindowTimes failed, now = " << timestamp
        //             << " start = " << *start << " end = " << *end << std::endl;
        return -1;
    }
    return 0;
}
// 将需要读取的 key list 列表转换成区间形式, 方便 range 查询
int buildWindowRange(const std::vector<std::pair<int64_t, risk::RiskPrecision>>& windowSlice,
                     std::vector<RiskQueryRange>* resRange) {
    if (windowSlice.size() <= 0) return 0;
    risk::RiskPrecision lastWindow = windowSlice[0].second;
    int64_t start_at = windowSlice[0].first;
    for (std::size_t i = 1; i < windowSlice.size(); ++i) {
        if (windowSlice[i].second != lastWindow) {
            (*resRange).emplace_back(
                std::move(RiskQueryRange(buildFieldPrefix(start_at, lastWindow),
                                         buildFieldPrefix(windowSlice[i].first, lastWindow))));
            lastWindow = windowSlice[i].second;
            start_at = windowSlice[i].first;
        }
    }
    // 最后一个的end需要再增加一个精度区间长度
    (*resRange).emplace_back(
        std::move(RiskQueryRange(buildFieldPrefix(start_at, lastWindow),
                                 buildFieldPrefix(windowSlice[windowSlice.size() - 1].first +
                                                      parseWindowDurationFromPrecision(lastWindow),
                                                  lastWindow))));
    return 0;
}
// 拆分时间切片, start, end 都是修复之后的时间, start < end
// 需要保证 start/end 的时间是能被最小精度整除的
// (包含当前的情况end不需要整除条件),
// 如果计算包含当前, 则 isFromNow 传入 true
// 传入的时间是当前时区的时间戳
int splitTimeSlice(const risk::Window& window, const risk::RiskPrecision riskPrecision,
                   int64_t start, int64_t end, std::vector<RiskQueryRange>* resRange) {
    start += RiskGlobalData::getSingleton().getTimeZoneOffsetSecond();
    end += RiskGlobalData::getSingleton().getTimeZoneOffsetSecond();
    auto storeLevel = RiskGlobalData::getSingleton().getPrecisionLevelInfo(riskPrecision);
    if (storeLevel.size() <= 0) {
        // RISK_LOG(WARNING, request) << "splitTimeSlice failed, get storeLevel failed, precision =
        // "
        //     << request.precision() << std::endl;
        return -1;
    }
    std::vector<std::pair<int64_t, risk::RiskPrecision>> windowSlice;
    windowSlice.clear();
    // 贪心计算
    auto nowPinfo = storeLevel[0];
    // 精度从小到大填充
    int64_t left = 0;
    bool stopLoop = false;
    for (std::size_t i = 0; i < storeLevel.size(); ++i) {
        auto pinfo = storeLevel[i];
        left = start % pinfo->windowDuration;
        if (left > 0) {
            for (int j = 0; j + left < pinfo->windowDuration; j += nowPinfo->windowDuration) {
                if (start + nowPinfo->windowDuration > end) {
                    stopLoop = true;
                    break;
                }
                windowSlice.emplace_back(std::move(getWindowPair(&start, nowPinfo)));
            }
        }
        if (stopLoop) {
            break;
        }
        nowPinfo = pinfo;
    }
    // 精度从大到小填充
    for (int i = storeLevel.size() - 1; i >= 0; --i) {
        nowPinfo = storeLevel[i];
        while (start + nowPinfo->windowDuration <= end) {
            windowSlice.emplace_back(std::move(getWindowPair(&start, nowPinfo)));
        }
    }
    return buildWindowRange(windowSlice, resRange);
}
int getWindows(const risk::Window& window, const risk::RiskPrecision riskPrecision,
               std::vector<RiskQueryRange>* resRange, bool needSplit, time_t injectTimestamp) {
    int64_t start_at = 0, end_at = 0;
    if (injectTimestamp == 0) {
        time(&injectTimestamp);
    }
    int ret = getWindowTimes(window, riskPrecision, injectTimestamp, needSplit, &start_at, &end_at);
    if (ret != 0) {
        // RISK_LOG(WARNING, request) << "getWindowTimes failed, ret = " << ret << std::endl;
        return ret;
    }
    if (needSplit) {
        return splitTimeSlice(window, riskPrecision, start_at, end_at, resRange);
    } else {
        (*resRange).emplace_back(std::move(RiskQueryRange(
            buildFieldPrefix(start_at, riskPrecision), buildFieldPrefix(end_at, riskPrecision))));
        return 0;
    }
}

}  // namespace risk_tool
}  // namespace bcache2
