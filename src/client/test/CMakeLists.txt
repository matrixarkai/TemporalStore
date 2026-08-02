file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_executable(client_test ${HDRS} ${SRCS})
target_link_libraries(client_test
    mini_cluster_lib
    client
    common
    test_common)
gtest_discover_tests(client_test DISCOVERY_MODE PRE_TEST)
target_compile_options(client_test PUBLIC -fno-access-control)
