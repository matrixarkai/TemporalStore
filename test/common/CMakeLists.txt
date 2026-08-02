file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_library(test_common ${HDRS} ${SRCS})
target_link_libraries(test_common
    common
    gtest_main
    byte
    gflags
    fiu)

add_subdirectory(test)
