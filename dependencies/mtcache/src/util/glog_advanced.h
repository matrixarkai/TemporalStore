// This file is an improved version of glog.

#pragma once

#include <glog/logging.h>

#include <chrono>

#ifndef _MINLOGLEVEL_GT_INFO
#define _MINLOGLEVEL_GT_INFO (FLAGS_minloglevel > google::GLOG_INFO)
#endif

#ifndef _MINLOGLEVEL_GT_WARNING
#define _MINLOGLEVEL_GT_WARNING (FLAGS_minloglevel > google::GLOG_WARNING)
#endif

#ifndef _MINLOGLEVEL_GT_ERROR
#define _MINLOGLEVEL_GT_ERROR (FLAGS_minloglevel > google::GLOG_ERROR)
#endif

#ifndef _MINLOGLEVEL_GT_FATAL
#define _MINLOGLEVEL_GT_FATAL (FLAGS_minloglevel > google::GLOG_FATAL)
#endif

#ifndef _MINLOGLEVEL_GT_DFATAL
#ifdef NDEBUG
// Treat DFATAL as ERROR in non-debug mode
#define _MINLOGLEVEL_GT_DFATAL (FLAGS_minloglevel > google::GLOG_ERROR)
#else
// Treat DFATAL as FATAL in debug mode
#define _MINLOGLEVEL_GT_DFATAL (FLAGS_minloglevel > google::GLOG_FATAL)
#endif
#endif

/*****************************************
 *
 * Normal loggings
 *
 ****************************************/
#ifndef _LOG_INFO
#define _LOG_INFO      \
  _MINLOGLEVEL_GT_INFO \
  ? (void)0 : google::LogMessageVoidify() & COMPACT_GOOGLE_LOG_INFO.stream()
#endif

#ifndef _LOG_WARNING
#define _LOG_WARNING      \
  _MINLOGLEVEL_GT_WARNING \
  ? (void)0 : google::LogMessageVoidify() & COMPACT_GOOGLE_LOG_WARNING.stream()
#endif

#ifndef _LOG_ERROR
#define _LOG_ERROR      \
  _MINLOGLEVEL_GT_ERROR \
  ? (void)0 : google::LogMessageVoidify() & COMPACT_GOOGLE_LOG_ERROR.stream()
#endif

#ifndef _LOG_FATAL
#define _LOG_FATAL COMPACT_GOOGLE_LOG_FATAL.stream()
#endif

#ifndef _LOG_DFATAL
#define _LOG_DFATAL      \
  _MINLOGLEVEL_GT_DFATAL \
  ? (void)0 : google::LogMessageVoidify() & COMPACT_GOOGLE_LOG_DFATAL.stream()
#endif

#undef LOG
#define LOG(severity) _LOG_##severity

#undef PLOG
#define PLOG(severity)       \
  _MINLOGLEVEL_GT_##severity \
      ? (void)0              \
      : google::LogMessageVoidify() & GOOGLE_PLOG(severity, 0).stream()

#undef SYSLOG
#define SYSLOG(severity)     \
  _MINLOGLEVEL_GT_##severity \
      ? (void)0              \
      : google::LogMessageVoidify() & SYSLOG_##severity(0).stream()

#undef LOG_TO_STRING
#define LOG_TO_STRING(severity, message) \
  _MINLOGLEVEL_GT_##severity             \
      ? (void)0                          \
      : google::LogMessageVoidify() &    \
            LOG_TO_STRING_##severity(static_cast<string*>(message)).stream()

#undef LOG_STRING
#define LOG_STRING(severity, outvec)                            \
  _MINLOGLEVEL_GT_##severity                                    \
      ? (void)0                                                 \
      : google::LogMessageVoidify() &                           \
            LOG_TO_STRING_##severity(                           \
                static_cast<std::vector<std::string>*>(outvec)) \
                .stream()

/*****************************************
 *
 * Conditional loggings
 *
 ****************************************/
#undef LOG_IF
#define LOG_IF(severity, condition)            \
  (!(condition) || _MINLOGLEVEL_GT_##severity) \
      ? (void)0                                \
      : google::LogMessageVoidify() & COMPACT_GOOGLE_LOG_##severity.stream()

#undef PLOG_IF
#define PLOG_IF(severity, condition)           \
  (!(condition) || _MINLOGLEVEL_GT_##severity) \
      ? (void)0                                \
      : google::LogMessageVoidify() & GOOGLE_PLOG(severity, 0).stream()

#undef SYSLOG_IF
#define SYSLOG_IF(severity, condition)         \
  (!(condition) || _MINLOGLEVEL_GT_##severity) \
      ? (void)0                                \
      : google::LogMessageVoidify() & SYSLOG_##severity(0).stream()

/*****************************************
 *
 * Sampling loggings
 *
 ****************************************/
#undef SOME_KIND_OF_LOG_EVERY_N
#define SOME_KIND_OF_LOG_EVERY_N(severity, n, what_to_do)          \
  static int LOG_OCCURRENCES = 0, LOG_OCCURRENCES_MOD_N = 0;       \
  ++LOG_OCCURRENCES;                                               \
  if (++LOG_OCCURRENCES_MOD_N > n) LOG_OCCURRENCES_MOD_N -= n;     \
  if (!(_MINLOGLEVEL_GT_##severity) && LOG_OCCURRENCES_MOD_N == 1) \
  google::LogMessage(__FILE__, __LINE__, google::GLOG_##severity,  \
                     LOG_OCCURRENCES, &what_to_do)                 \
      .stream()

#undef SOME_KIND_OF_LOG_IF_EVERY_N
#define SOME_KIND_OF_LOG_IF_EVERY_N(severity, condition, n, what_to_do) \
  static int LOG_OCCURRENCES = 0, LOG_OCCURRENCES_MOD_N = 0;            \
  ++LOG_OCCURRENCES;                                                    \
  if (++LOG_OCCURRENCES_MOD_N > n) LOG_OCCURRENCES_MOD_N -= n;          \
  if (!(_MINLOGLEVEL_GT_##severity) && (condition) &&                   \
      LOG_OCCURRENCES_MOD_N == 1)                                       \
  google::LogMessage(__FILE__, __LINE__, google::GLOG_##severity,       \
                     LOG_OCCURRENCES, &what_to_do)                      \
      .stream()

#undef SOME_KIND_OF_PLOG_EVERY_N
#define SOME_KIND_OF_PLOG_EVERY_N(severity, n, what_to_do)             \
  static int LOG_OCCURRENCES = 0, LOG_OCCURRENCES_MOD_N = 0;           \
  ++LOG_OCCURRENCES;                                                   \
  if (++LOG_OCCURRENCES_MOD_N > n) LOG_OCCURRENCES_MOD_N -= n;         \
  if (!(_MINLOGLEVEL_GT_##severity) && LOG_OCCURRENCES_MOD_N == 1)     \
  google::ErrnoLogMessage(__FILE__, __LINE__, google::GLOG_##severity, \
                          LOG_OCCURRENCES, &what_to_do)                \
      .stream()

#undef SOME_KIND_OF_LOG_FIRST_N
#define SOME_KIND_OF_LOG_FIRST_N(severity, n, what_to_do)         \
  static int LOG_OCCURRENCES = 0;                                 \
  if (LOG_OCCURRENCES <= n) ++LOG_OCCURRENCES;                    \
  if (!(_MINLOGLEVEL_GT_##severity) && LOG_OCCURRENCES <= n)      \
  google::LogMessage(__FILE__, __LINE__, google::GLOG_##severity, \
                     LOG_OCCURRENCES, &what_to_do)                \
      .stream()

#undef LOG_EVERY_N
#define LOG_EVERY_N(severity, n) \
  SOME_KIND_OF_LOG_EVERY_N(severity, (n), google::LogMessage::SendToLog)

#undef LOG_IF_EVERY_N
#define LOG_IF_EVERY_N(severity, condition, n)            \
  SOME_KIND_OF_LOG_IF_EVERY_N(severity, (condition), (n), \
                              google::LogMessage::SendToLog)

#undef PLOG_EVERY_N
#define PLOG_EVERY_N(severity, n) \
  SOME_KIND_OF_PLOG_EVERY_N(severity, (n), google::LogMessage::SendToLog)

#undef SYSLOG_EVERY_N
#define SYSLOG_EVERY_N(severity, n)       \
  SOME_KIND_OF_LOG_EVERY_N(severity, (n), \
                           google::LogMessage::SendToSyslogAndLog)

#undef LOG_FIRST_N
#define LOG_FIRST_N(severity, n) \
  SOME_KIND_OF_LOG_FIRST_N(severity, (n), google::LogMessage::SendToLog)

// Helper macros for implementing *_EVERY_T macros.
#undef LOG_TIME_PERIOD
#define LOG_TIME_PERIOD LOG_EVERY_N_VARNAME(time_period_, __LINE__)
#undef LOG_PREVIOUS_TIME_RAW
#define LOG_PREVIOUS_TIME_RAW LOG_EVERY_N_VARNAME(prev_time_raw_, __LINE__)
#undef LOG_TIME_DELTA
#define LOG_TIME_DELTA LOG_EVERY_N_VARNAME(delta_time_, __LINE__)
#undef LOG_CURRENT_TIME
#define LOG_CURRENT_TIME LOG_EVERY_N_VARNAME(curr_time_, __LINE__)
#undef LOG_PREVIOUS_TIME
#define LOG_PREVIOUS_TIME LOG_EVERY_N_VARNAME(prev_time_, __LINE__)

// adapted from latest glog
#undef SOME_KIND_OF_LOG_IF_EVERY_T
#define SOME_KIND_OF_LOG_IF_EVERY_T(log, severity, condition, seconds, \
                                    what_to_do)                        \
  /* to support google::COUNTER */                                     \
  static int LOG_OCCURRENCES = 0;                                      \
  ++LOG_OCCURRENCES;                                                   \
  constexpr std::chrono::nanoseconds LOG_TIME_PERIOD =                 \
      std::chrono::duration_cast<std::chrono::nanoseconds>(            \
          std::chrono::duration<double>(seconds));                     \
  /* set to zero such that the first call always prints */             \
  static int64_t LOG_PREVIOUS_TIME_RAW = 0;                            \
  const auto LOG_CURRENT_TIME =                                        \
      std::chrono::duration_cast<std::chrono::nanoseconds>(            \
          std::chrono::steady_clock::now().time_since_epoch());        \
  const auto LOG_PREVIOUS_TIME = LOG_PREVIOUS_TIME_RAW;                \
  const auto LOG_TIME_DELTA =                                          \
      LOG_CURRENT_TIME - std::chrono::nanoseconds(LOG_PREVIOUS_TIME);  \
  if (!(_MINLOGLEVEL_GT_##severity) && (condition) &&                  \
      (LOG_TIME_DELTA >                                                \
       LOG_TIME_PERIOD) && /* update `LOG_PREVIOUS_TIME_RAW` */        \
      (LOG_PREVIOUS_TIME_RAW =                                         \
           std::chrono::duration_cast<std::chrono::nanoseconds>(       \
               LOG_CURRENT_TIME)                                       \
               .count()))                                              \
  log(__FILE__, __LINE__, google::GLOG_##severity, LOG_OCCURRENCES,    \
      &what_to_do)                                                     \
      .stream()

// Print a log every n seconds. First call always prints.
#undef LOG_IF_EVERY_T
#define LOG_IF_EVERY_T(severity, condition, seconds)                   \
  SOME_KIND_OF_LOG_IF_EVERY_T(google::LogMessage, severity, condition, \
                              seconds, google::LogMessage::SendToLog)
#undef LOG_EVERY_T
#define LOG_EVERY_T(severity, seconds) LOG_IF_EVERY_T(severity, true, seconds)

#undef LOG_IF_EVERY_SECOND
#define LOG_IF_EVERY_SECOND(severity, condition) \
  LOG_IF_EVERY_T(severity, condition, 1)
#undef LOG_EVERY_SECOND
#define LOG_EVERY_SECOND(severity) LOG_EVERY_T(severity, 1)

#undef PLOG_IF_EVERY_T
#define PLOG_IF_EVERY_T(severity, condition, seconds)                       \
  SOME_KIND_OF_LOG_IF_EVERY_T(google::ErrnoLogMessage, severity, condition, \
                              seconds, google::LogMessage::SendToLog)
#undef PLOG_EVERY_T
#define PLOG_EVERY_T(severity, seconds) PLOG_IF_EVERY_T(severity, true, seconds)

#undef PLOG_IF_EVERY_SECOND
#define PLOG_IF_EVERY_SECOND(severity, condition) \
  PLOG_IF_EVERY_T(severity, condition, 1)
#undef PLOG_EVERY_SECOND
#define PLOG_EVERY_SECOND(severity) PLOG_EVERY_T(severity, 1)

#undef SYSLOG_IF_EVERY_T
#define SYSLOG_IF_EVERY_T(severity, condition, seconds)                \
  SOME_KIND_OF_LOG_IF_EVERY_T(google::LogMessage, severity, condition, \
                              seconds, google::LogMessage::SendToSyslogAndLog)
#undef SYSLOG_EVERY_T
#define SYSLOG_EVERY_T(severity, seconds) \
  SYSLOG_IF_EVERY_T(severity, true, seconds)

#undef SYSLOG_IF_EVERY_SECOND
#define SYSLOG_IF_EVERY_SECOND(severity, condition) \
  SYSLOG_IF_EVERY_T(severity, condition, 1)
#undef SYSLOG_EVERY_SECOND
#define SYSLOG_EVERY_SECOND(severity) SYSLOG_EVERY_T(severity, 1)

/*****************************************
 *
 * Debugging loggings
 *
 * Only works when compiled with NDEBUG=1
 *
 ****************************************/
#undef DLOG
#ifndef NDEBUG
#define DLOG(severity) LOG(severity)
#else
#define DLOG(severity) \
  true ? (void)0       \
       : google::LogMessageVoidify() & COMPACT_GOOGLE_LOG_##severity.stream()
#endif

#undef DLOG_IF
#ifndef NDEBUG
#define DLOG_IF(severity, condition) LOG_IF(severity, condition)
#else
#define DLOG_IF(severity, condition) \
  true ? (void)0                     \
       : google::LogMessageVoidify() & COMPACT_GOOGLE_LOG_##severity.stream()
#endif

#undef DLOG_EVERY_N
#ifndef NDEBUG
#define DLOG_EVERY_N(severity, n) LOG_EVERY_N(severity, n)
#else
#define DLOG_EVERY_N(severity, n) \
  true ? (void)0                  \
       : google::LogMessageVoidify() & COMPACT_GOOGLE_LOG_##severity.stream()
#endif

#undef DLOG_IF_EVERY_N
#ifndef NDEBUG
#define DLOG_IF_EVERY_N(severity, condition, n) \
  LOG_IF_EVERY_N(severity, condition, n)
#else
#define DLOG_IF_EVERY_N(severity, condition, n) \
  true ? (void)0                                \
       : google::LogMessageVoidify() & COMPACT_GOOGLE_LOG_##severity.stream()
#endif
