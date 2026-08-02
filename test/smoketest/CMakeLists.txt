file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_executable(smoketest ${HDRS} ${SRCS})
target_link_libraries(smoketest
    mini_cluster_lib
    client
    serverlib
    test_common
    gtest
    gtest_main
    partition
    recordstream
    bcache2_proto
    bytestore
    byte
    absl_base
    absl_int128
    absl_spinlock_wait
    absl_stacktrace
    absl_str_format_internal
    absl_strings
    absl_str_format
    absl_symbolize
    absl_synchronization
    absl_time
    gflags
    libprotobuf
    brpc-static
    isal-static
    cryptopp-static
    zookeeper_mt)
gtest_discover_tests(smoketest DISCOVERY_MODE PRE_TEST)
target_compile_options(smoketest PUBLIC -fno-access-control)

add_executable(consistency_smoketest "consistency_bench.cc")
target_link_libraries(consistency_smoketest
    mini_cluster_lib
    client
    serverlib
    test_common
    gtest
    gtest_main
    partition
    recordstream
    bcache2_proto
    bytestore
    byte
    absl_base
    absl_int128
    absl_spinlock_wait
    absl_stacktrace
    absl_str_format_internal
    absl_strings
    absl_str_format
    absl_symbolize
    absl_synchronization
    absl_time
    gflags
    libprotobuf
    brpc-static
    isal-static
    cryptopp-static
    zookeeper_mt)
gtest_discover_tests(consistency_smoketest DISCOVERY_MODE PRE_TEST)
target_compile_options(consistency_smoketest PUBLIC -fno-access-control)
