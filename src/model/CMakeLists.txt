file(GLOB HDRS "*.h" "ips/*.h")
file(GLOB SRCS "*.cc" "ips/*.cc")
add_library(model ${HDRS} ${SRCS})
add_definitions(-w)
target_link_libraries(model
    bcache2_proto
    byte
    ips_proto_lib
    absl::flat_hash_set
    cpc_model
    /usr/lib/x86_64-linux-gnu/libfmt.so)
if (BCACHE2_BUILD_TESTS)
    add_subdirectory(test)
endif()
add_subdirectory(performance)
add_subdirectory(cpc)
