file(GLOB HDRS "*.h")
file(GLOB SRCS main.cc flags.cc)
if (CYGWIN)
    add_executable(stream_tool EXCLUDE_FROM_ALL ${HDRS} ${SRCS})
else()
    add_executable(stream_tool ${HDRS} ${SRCS})
endif()
target_link_libraries(stream_tool
    recordstream
    bcache2_proto
    gflags
    brpc-static
    byte
    /usr/lib/x86_64-linux-gnu/libleveldb.so
    /usr/lib/x86_64-linux-gnu/libfmt.so
    OpenSSL::SSL
    OpenSSL::Crypto
    /usr/lib/x86_64-linux-gnu/libabsl_throw_delegate.so
    /usr/lib/x86_64-linux-gnu/libabsl_time_zone.so
    /usr/lib/x86_64-linux-gnu/libabsl_raw_logging_internal.so
    /usr/lib/x86_64-linux-gnu/libabsl_log_severity.so)

# add_executable(stream_tool_test stream_tool_test.cc)
# target_link_libraries(stream_tool_test
#     recordstream
#     test_common
#     gflags
#     gtest)
# target_compile_options(stream_tool_test PUBLIC -fno-access-control)
# gtest_discover_tests(stream_tool_test DISCOVERY_MODE PRE_TEST)
# 
