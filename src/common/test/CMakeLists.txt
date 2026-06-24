file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_executable(common_test ${HDRS} ${SRCS})
target_link_libraries(common_test
    common
    gtest
    test_common)
gtest_discover_tests(common_test DISCOVERY_MODE PRE_TEST)
