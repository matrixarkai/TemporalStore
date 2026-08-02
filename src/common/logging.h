// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/byte_log.h>
#include <byte/include/macros.h>
#include <gflags/gflags.h>

#include <ostream>

DECLARE_uint64(log_sample_count);

extern thread_local uint64_t g_log_counter;

#define LOG_MESSAGE(level, message) \
    if (level >= byte::GetMinLogLevel()) bcache2::Logger(__FILE__, __LINE__, level, message)

#define LOG_DEBUG(message) LOG_MESSAGE(byte::LOG_LEVEL_DEBUG, message)
#define LOG_INFO(message) LOG_MESSAGE(byte::LOG_LEVEL_INFO, message)
#define LOG_WARNING(message) LOG_MESSAGE(byte::LOG_LEVEL_WARNING, message)
#define LOG_ERROR(message) LOG_MESSAGE(byte::LOG_LEVEL_ERROR, message)
#define LOG_FATAL(message) LOG_MESSAGE(byte::LOG_LEVEL_FATAL, message)

#define LOG_FLUSH() byte::LogFlush()

#define LOG_DEBUG_SAMPLE(message) \
    if (++g_log_counter % FLAGS_log_sample_count == 0) LOG_DEBUG(message)
#define LOG_INFO_SAMPLE(message) \
    if (++g_log_counter % FLAGS_log_sample_count == 0) LOG_INFO(message)
#define LOG_WARNING_SAMPLE(message) \
    if (++g_log_counter % FLAGS_log_sample_count == 0) LOG_WARNING(message)
#define LOG_ERROR_SAMPLE(message) \
    if (++g_log_counter % FLAGS_log_sample_count == 0) LOG_ERROR(message)
#define LOG_FATAL_SAMPLE(message) \
    if (++g_log_counter % FLAGS_log_sample_count == 0) LOG_FATAL(message)

#define SCOPED_LOGGER_1(x, y) x##y
#define SCOPED_LOGGER_2(x, y) SCOPED_LOGGER_1(x, y)
#define SCOPED_LOGGER_3(x) SCOPED_LOGGER_2(x, __COUNTER__)

#define LOG_CALL(level)                                                    \
    bcache2::ScopedLogger SCOPED_LOGGER_3(__scoped_logger)(                \
        __FILE__, __LINE__, byte::LOG_LEVEL_##level, __PRETTY_FUNCTION__); \
    if (byte::LOG_LEVEL_##level >= byte::GetMinLogLevel())                 \
    bcache2::Logger(__FILE__, __LINE__, byte::LOG_LEVEL_##level, __PRETTY_FUNCTION__, true)

#ifndef NDEBUG
#define LOG_CALL_DEBUG()                                                 \
    bcache2::ScopedLogger SCOPED_LOGGER_3(__scoped_logger)(              \
        __FILE__, __LINE__, byte::LOG_LEVEL_DEBUG, __PRETTY_FUNCTION__); \
    if (byte::LOG_LEVEL_##DEBUG >= byte::GetMinLogLevel())               \
    bcache2::Logger(__FILE__, __LINE__, byte::LOG_LEVEL_##DEBUG, __PRETTY_FUNCTION__, true)
#else
#define LOG_CALL_DEBUG()                                   \
    if (byte::LOG_LEVEL_##DEBUG >= byte::GetMinLogLevel()) \
    bcache2::Logger(__FILE__, __LINE__, byte::LOG_LEVEL_##DEBUG, __PRETTY_FUNCTION__, true)
#endif  // NDEBUG

#define LOG_CALL_INFO() LOG_CALL(INFO)
#define LOG_CALL_WARNING() LOG_CALL(WARNING)
#define LOG_CALL_ERROR() LOG_CALL(ERROR)
#define LOG_CALL_FATAL() LOG_CALL(FATAL)

namespace bcache2 {

class Logger {
 public:
    Logger(const char* file, int line, byte::LogLevel level, const char* message)
        : log_messager_(file, line, level, true), stream_(log_messager_.stream()) {
        stream_ << message;
    }

    Logger(const char* file, int line, byte::LogLevel level, const char* func, bool)
        : log_messager_(file, line, level, true), stream_(log_messager_.stream()) {
        stream_ << "Enter function " << func;
    }

    template <typename Value>
    Logger& put(const char* key, const Value& value) {
        stream_ << ", " << key << ":" << value;
        return *this;
    }

    Logger& put(const char* key, uint8_t value) {
        // ostream recognize uint8_t as char
        stream_ << ", " << key << ":" << static_cast<uint16_t>(value);
        return *this;
    }

 private:
    byte::LogMessager log_messager_;
    std::ostream& stream_;

    DISALLOW_COPY_AND_ASSIGN(Logger);
};

class ScopedLogger {
 public:
    ScopedLogger(const char* file, int line, byte::LogLevel level, const char* func)
        : log_messager_(file, line, level, true), stream_(log_messager_.stream()) {
        stream_ << "Exit function " << func;
    }

 private:
    byte::LogMessager log_messager_;
    std::ostream& stream_;

    DISALLOW_COPY_AND_ASSIGN(ScopedLogger);
};

}  // namespace bcache2
