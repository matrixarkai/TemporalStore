file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_library(common ${HDRS} ${SRCS})
target_compile_options(common PRIVATE -Wno-error=cpp)
target_link_libraries(common
    bcache2_proto
    modules_define
    co
    byte
    gflags
    brpc-static
    bcache2_proto)

if (BCACHE2_BUILD_TESTS)
    add_subdirectory(test)
endif()
