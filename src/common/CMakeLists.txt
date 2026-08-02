file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_library(common ${HDRS} ${SRCS})
target_compile_options(common PRIVATE -Wno-error=cpp)
target_link_libraries(common
    PRIVATE
    bcache2_proto
    modules_define
    co
    byte
    gflags
    bcache2_proto)
set_target_properties(common PROPERTIES
    INTERFACE_LINK_LIBRARIES "bcache2_proto;modules_define;byte;gflags")

if (BCACHE2_BUILD_TESTS)
    add_subdirectory(test)
endif()
