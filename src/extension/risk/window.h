// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "extension/risk/interface.pb.h"

namespace bcache2 {
namespace risk_tool {

// ============ 定义结构体 ============
struct RiskQueryRange {
    std::string begin;
    std::string end;
    RiskQueryRange(const std::string& b, const std::string& e) {
        begin = b;
        end = e;
    }
};

// ============ 实现方法 ============
int getWindows(const risk::Window& window, const risk::RiskPrecision riskPrecision,
               std::vector<RiskQueryRange>* resRange, bool needSplit, time_t injectTimestamp);

// ============ 内部方法 ============
time_t fixTimeWithPrecision(const int64_t occurTime, const std::string& key,
                            const risk::RiskPrecision riskPrecision);

std::string buildFieldPrefix(int64_t start, risk::RiskPrecision window);
time_t fixTime(const tm& now, const risk::Window& window, const risk::RiskPrecision riskPrecision,
               bool isEnd, bool needSplit);

int getWindowTimes(const risk::Window& window, const risk::RiskPrecision riskPrecision,
                   time_t timestamp, bool needSplit, int64_t* start, int64_t* end);
int buildWindowRange(const std::vector<std::pair<int64_t, risk::RiskPrecision>>& windowSlice,
                     std::vector<RiskQueryRange>* resRange);
int splitTimeSlice(const risk::Window& window, const risk::RiskPrecision riskPrecision,
                   int64_t start, int64_t end, std::vector<RiskQueryRange>* resRange);
}  // namespace risk_tool
}  // namespace bcache2
