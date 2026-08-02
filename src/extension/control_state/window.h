// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "extension/control_state/interface.pb.h"

namespace bcache2 {
namespace control_state_tool {

// ============ 定义结构体 ============
struct ControlStateQueryRange {
    std::string begin;
    std::string end;
    ControlStateQueryRange(const std::string& b, const std::string& e) {
        begin = b;
        end = e;
    }
};

// ============ 实现方法 ============
int getWindows(const control_state::Window& window, const control_state::ControlStatePrecision control_statePrecision,
               std::vector<ControlStateQueryRange>* resRange, bool needSplit, time_t injectTimestamp);

// ============ 内部方法 ============
time_t fixTimeWithPrecision(const int64_t occurTime, const std::string& key,
                            const control_state::ControlStatePrecision control_statePrecision);

std::string buildFieldPrefix(int64_t start, control_state::ControlStatePrecision window);
time_t fixTime(const tm& now, const control_state::Window& window, const control_state::ControlStatePrecision control_statePrecision,
               bool isEnd, bool needSplit);

int getWindowTimes(const control_state::Window& window, const control_state::ControlStatePrecision control_statePrecision,
                   time_t timestamp, bool needSplit, int64_t* start, int64_t* end);
int buildWindowRange(const std::vector<std::pair<int64_t, control_state::ControlStatePrecision>>& windowSlice,
                     std::vector<ControlStateQueryRange>* resRange);
int splitTimeSlice(const control_state::Window& window, const control_state::ControlStatePrecision control_statePrecision,
                   int64_t start, int64_t end, std::vector<ControlStateQueryRange>* resRange);
}  // namespace control_state_tool
}  // namespace bcache2
