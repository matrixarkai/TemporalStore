file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_executable(bench_test ${HDRS} ${SRCS})
target_link_libraries(bench_test
    mini_cluster_lib
    benchlib
    test_common
    byte
    gflags)
target_compile_options(bench_test PUBLIC -fno-access-control)
gtest_discover_tests(bench_test DISCOVERY_MODE PRE_TEST)
