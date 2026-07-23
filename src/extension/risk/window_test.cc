// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "extension/risk/window.h"

#include <gtest/gtest.h>
#include <stdio.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace bcache2 {
namespace risk_tool {

// 所有区间都为左闭右开区间
TEST(FixTime, RiskModule) {
    // 2022-11-28 23:59:59
    time_t timestamp = 1669651199;
    tm t;
    localtime_r(&timestamp, &t);
    // 精度为 1h
    {
        bcache2::risk::Window window;
        window.set_end(-2);
        window.set_start(-10);
        window.set_unit(risk::Day);
        auto start = fixTime(t, window, risk::OneHour, false, true);
        auto end = fixTime(t, window, risk::OneHour, true, true);
        // 2022-11-27 23:00:00
        ASSERT_EQ(end, 1669561200);
        // 2022-11-18 23:00:00
        ASSERT_EQ(start, 1668783600);
    }
    {
        bcache2::risk::Window window;
        window.set_end(0);
        window.set_start(-10);
        window.set_unit(risk::Day);
        auto start = fixTime(t, window, risk::OneHour, false, true);
        auto end = fixTime(t, window, risk::OneHour, true, true);
        // 2022-11-29 00:00:00
        ASSERT_EQ(end, 1669651200);
        // 2022-11-18 23:00:00
        ASSERT_EQ(start, 1668783600);
    }
    // 精度为 1s
    {
        bcache2::risk::Window window;
        window.set_end(-2);
        window.set_start(-10);
        window.set_unit(risk::Minute);
        auto start = fixTime(t, window, risk::OneSecond, false, true);
        auto end = fixTime(t, window, risk::OneSecond, true, true);
        // 2022-11-28 23:58:59
        ASSERT_EQ(end, 1669651139);
        // 2022-11-28 23:49:59
        ASSERT_EQ(start, 1669650599);
    }
    {
        bcache2::risk::Window window;
        window.set_end(0);
        window.set_start(-10);
        window.set_unit(risk::Minute);
        auto start = fixTime(t, window, risk::OneSecond, false, true);
        auto end = fixTime(t, window, risk::OneSecond, true, true);
        // 2022-11-29 00:00:00
        ASSERT_EQ(end, 1669651200);
        // 2022-11-28 23:49:59
        ASSERT_EQ(start, 1669650599);
    }
    // 精度为 1s unit 为 s
    {
        bcache2::risk::Window window;
        window.set_end(-2);
        window.set_start(-10);
        window.set_unit(risk::Second);
        auto start = fixTime(t, window, risk::OneSecond, false, true);
        auto end = fixTime(t, window, risk::OneSecond, true, true);
        // 2022-11-28 23:59:58
        ASSERT_EQ(end, 1669651198);
        // 2022-11-28 23:59:49
        ASSERT_EQ(start, 1669651189);
    }
    {
        bcache2::risk::Window window;
        window.set_end(0);
        window.set_start(-10);
        window.set_unit(risk::Second);
        auto start = fixTime(t, window, risk::OneSecond, false, true);
        auto end = fixTime(t, window, risk::OneSecond, true, true);
        // 2022-11-29 00:00:00
        ASSERT_EQ(end, 1669651200);
        // 2022-11-28 23:59:49
        ASSERT_EQ(start, 1669651189);
    }
    // 精度为 1h unit = h
    {
        bcache2::risk::Window window;
        window.set_end(-1);
        window.set_start(-1);
        window.set_unit(risk::Hour);
        auto start = fixTime(t, window, risk::OneHour, false, true);
        auto end = fixTime(t, window, risk::OneHour, true, true);
        // 2022-11-28 23:00:00
        ASSERT_EQ(end, 1669647600);
        // 2022-11-28 22:00:00
        ASSERT_EQ(start, 1669644000);
    }
    {
        bcache2::risk::Window window;
        window.set_end(0);
        window.set_start(-1);
        window.set_unit(risk::Hour);
        auto start = fixTime(t, window, risk::OneHour, false, true);
        auto end = fixTime(t, window, risk::OneHour, true, true);
        // 2022-11-29 00:00:00
        ASSERT_EQ(end, 1669651200);
        // 2022-11-28 22:00:00
        ASSERT_EQ(start, 1669644000);
    }
    // 精度为 1s unit = s
    {
        bcache2::risk::Window window;
        window.set_end(-1);
        window.set_start(-1);
        window.set_unit(risk::Second);
        auto start = fixTime(t, window, risk::OneSecond, false, true);
        auto end = fixTime(t, window, risk::OneSecond, true, true);
        // 2022-11-28 23:59:59
        ASSERT_EQ(end, 1669651199);
        // 2022-11-28 23:59:58
        ASSERT_EQ(start, 1669651198);
    }
    {
        bcache2::risk::Window window;
        window.set_end(0);
        window.set_start(-1);
        window.set_unit(risk::Second);
        auto start = fixTime(t, window, risk::OneSecond, false, true);
        auto end = fixTime(t, window, risk::OneSecond, true, true);
        // 2022-11-29 00:00:00
        ASSERT_EQ(end, 1669651200);
        // 2022-11-28 23:59:58
        ASSERT_EQ(start, 1669651198);
    }
}

TEST(GetWindows, RiskModule) {
    std::vector<RiskQueryRange> res;
    // 2022-11-28 22:59:50
    time_t timestamp = 1669647590;
    auto printRes = [&res]() -> std::string {
        for (size_t i = 0; i < res.size(); ++i) {
            std::cout << res[i].begin << ' ' << res[i].end << std::endl;
        }
        res.clear();
        return "";
    };
    auto checkAndClearRes = [&res, &printRes](std::vector<RiskQueryRange> want,
                                              std::string caseName) {
        ASSERT_EQ(res.size(), want.size()) << " test_cast_name: " << caseName << printRes();
        for (size_t i = 0; i < res.size(); ++i) {
            ASSERT_EQ(res[i].begin, want[i].begin) << " test_cast_name: " << caseName << printRes();
            ASSERT_EQ(res[i].end, want[i].end) << " test_cast_name: " << caseName << printRes();
        }
        res.clear();
    };
    auto getPrefix = [](int64_t start, int64_t end,
                        risk::RiskPrecision precision) -> RiskQueryRange {
        return {
            std::to_string(precision) + std::to_string(start),
            std::to_string(precision) + std::to_string(end),
        };
    };

    // DC
    {
        bcache2::risk::Window window;

        // 精度 1h 单位 d
        {
            window.set_end(0);
            window.set_start(-10);
            window.set_unit(risk::Day);
            ASSERT_EQ(getWindows(window, risk::OneHour, &res, false, timestamp), 0);
            checkAndClearRes(
                {// 2022-11-18 22:00:00 - 2022-11-28 23:00:00
                 {getPrefix(1668780000, 1669647600, risk::OneHour)}},
                "dc,1h,10d,ns");
            ASSERT_EQ(getWindows(window, risk::OneHour, &res, true, timestamp), 0);
            checkAndClearRes(
                {// 2022-11-18 22:00:00 - 2022-11-18 24:00:00
                 {getPrefix(1668780000, 1668787200, risk::OneHour)},
                 // 2022-11-19 00:00:00 - 2022-11-28 00:00:00
                 {getPrefix(1668787200, 1669564800, risk::OneDay)},
                 // 2022-11-28 00:00:00 - 2022-11-28 23:00:00
                 {getPrefix(1669564800, 1669647600, risk::OneHour)}},
                "dc,1h,10d,s");
        }
        // 精度 1h 单位 h
        {
            window.set_end(0);
            window.set_start(-10);
            window.set_unit(risk::Hour);
            ASSERT_EQ(getWindows(window, risk::OneHour, &res, false, timestamp), 0);
            checkAndClearRes(
                {// 2022-11-28 12:00:00 - 2022-11-28 23:00:00
                 {getPrefix(1669608000, 1669647600, risk::OneHour)}},
                "dc,1h,10h,ns");
            ASSERT_EQ(getWindows(window, risk::OneHour, &res, true, timestamp), 0);
            checkAndClearRes(
                {
                    // 2022-11-28 12:00:00 - 2022-11-28 23:00:00
                    {getPrefix(1669608000, 1669647600, risk::OneHour)},
                },
                "dc,1h,10h,s");
        }
        // 精度 1s 单位 min
        {
            window.set_end(0);
            window.set_start(-10);
            window.set_unit(risk::Minute);
            ASSERT_EQ(getWindows(window, risk::OneSecond, &res, false, timestamp), 0);
            checkAndClearRes(
                {// 2022-11-28 22:49:50 - 2022-11-28 22:59:51
                 {getPrefix(1669646990, 1669647591, risk::OneSecond)}},
                "dc,1s,10min,ns");
            ASSERT_EQ(getWindows(window, risk::OneSecond, &res, true, timestamp), 0);
            checkAndClearRes(
                {
                    // 2022-11-28 22:49:50 - 2022-11-28 22:50:00
                    {getPrefix(1669646990, 1669647000, risk::OneSecond)},
                    // 2022-11-28 22:50:00 - 2022-11-28 22:59:00
                    {getPrefix(1669647000, 1669647540, risk::OneMinute)},
                    // 2022-11-28 22:59:00 - 2022-11-28 22:59:51
                    {getPrefix(1669647540, 1669647591, risk::OneSecond)},
                },
                "dc,1s,10min,s");
        }
        // 精度 1s 单位 s
        {
            window.set_end(0);
            window.set_start(-10);
            window.set_unit(risk::Second);
            ASSERT_EQ(getWindows(window, risk::OneSecond, &res, false, timestamp), 0);
            checkAndClearRes(
                {// 2022-11-28 22:59:40 - 2022-11-28 22:59:51
                 {getPrefix(1669647580, 1669647591, risk::OneSecond)}},
                "dc,1s,10s,ns");
            ASSERT_EQ(getWindows(window, risk::OneSecond, &res, true, timestamp), 0);
            checkAndClearRes(
                {// 2022-11-28 22:59:40 - 2022-11-28 22:59:51
                 {getPrefix(1669647580, 1669647591, risk::OneSecond)}},
                "dc,1s,10s,s");
        }
    }
    // 非 DC
    {
        bcache2::risk::Window window;

        // 精度 1h 单位 d
        {
            window.set_end(0);
            window.set_start(-10);
            window.set_unit(risk::Day);
            ASSERT_EQ(getWindows(window, risk::OneHour, &res, false, timestamp), 0);
            checkAndClearRes(
                {// 2022-11-18 22:00:00 - 2022-11-28 23:00:00
                 {getPrefix(1668780000, 1669647600, risk::OneHour)}},
                "min,1h,10d,ns");
            ASSERT_EQ(getWindows(window, risk::OneHour, &res, true, timestamp), 0);
            checkAndClearRes(
                {// 2022-11-18 22:00:00 - 2022-11-18 24:00:00
                 {getPrefix(1668780000, 1668787200, risk::OneHour)},
                 // 2022-11-19 00:00:00 - 2022-11-28 00:00:00
                 {getPrefix(1668787200, 1669564800, risk::OneDay)},
                 // 2022-11-28 00:00:00 - 2022-11-28 23:00:00
                 {getPrefix(1669564800, 1669647600, risk::OneHour)}},
                "min,1h,10d,s");
        }
        // 精度 1h 单位 h
        {
            window.set_end(0);
            window.set_start(-10);
            window.set_unit(risk::Hour);
            ASSERT_EQ(getWindows(window, risk::OneHour, &res, false, timestamp), 0);
            checkAndClearRes(
                {// 2022-11-28 12:00:00 - 2022-11-28 23:00:00
                 {getPrefix(1669608000, 1669647600, risk::OneHour)}},
                "min,1h,10h,ns");
            ASSERT_EQ(getWindows(window, risk::OneHour, &res, true, timestamp), 0);
            checkAndClearRes(
                {
                    // 2022-11-28 12:00:00 - 2022-11-28 23:00:00
                    {getPrefix(1669608000, 1669647600, risk::OneHour)},
                },
                "min,1h,10h,s");
        }
        // 精度 1s 单位 min
        {
            window.set_end(0);
            window.set_start(-10);
            window.set_unit(risk::Minute);
            ASSERT_EQ(getWindows(window, risk::OneSecond, &res, false, timestamp), 0);
            checkAndClearRes(
                {// 2022-11-28 22:49:50 - 2022-11-28 22:59:51
                 {getPrefix(1669646990, 1669647591, risk::OneSecond)}},
                "min,1s,10min,ns");
            ASSERT_EQ(getWindows(window, risk::OneSecond, &res, true, timestamp), 0);
            checkAndClearRes(
                {
                    // 2022-11-28 22:49:50 - 2022-11-28 22:50:00
                    {getPrefix(1669646990, 1669647000, risk::OneSecond)},
                    // 2022-11-28 22:50:00 - 2022-11-28 22:59:00
                    {getPrefix(1669647000, 1669647540, risk::OneMinute)},
                    // 2022-11-28 22:59:00 - 2022-11-28 22:59:51
                    {getPrefix(1669647540, 1669647591, risk::OneSecond)},
                },
                "min,1s,10min,s");
        }
        // 精度 1s 单位 s
        {
            window.set_end(0);
            window.set_start(-10);
            window.set_unit(risk::Second);
            ASSERT_EQ(getWindows(window, risk::OneSecond, &res, false, timestamp), 0);
            checkAndClearRes(
                {// 2022-11-28 22:59:40 - 2022-11-28 22:59:51
                 {getPrefix(1669647580, 1669647591, risk::OneSecond)}},
                "min,1s,10s,ns");
            ASSERT_EQ(getWindows(window, risk::OneSecond, &res, true, timestamp), 0);
            checkAndClearRes(
                {// 2022-11-28 22:59:40 - 2022-11-28 22:59:51
                 {getPrefix(1669647580, 1669647591, risk::OneSecond)}},
                "min,1s,10s,s");
        }
    }
}

}  // namespace risk_tool
}  // namespace bcache2
