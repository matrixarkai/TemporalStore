file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_executable(server_test ${HDRS} ${SRCS})
target_link_libraries(server_test
    serverlib
    test_common)
target_compile_options(server_test PUBLIC -fno-access-control)
gtest_discover_tests(server_test)
