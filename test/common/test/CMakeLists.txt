file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_executable(test_common_test ${HDRS} ${SRCS})
target_link_libraries(test_common_test
    test_common)