file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_executable(proxy_test ${HDRS} ${SRCS})
target_link_libraries(proxy_test
    mini_cluster_lib
    test_common
    )
set_target_properties(proxy_test PROPERTIES CXX_STANDARD 14)
target_compile_options(proxy_test PUBLIC -fno-access-control)
gtest_discover_tests(proxy_test DISCOVERY_MODE PRE_TEST)
