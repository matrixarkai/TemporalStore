if (BCACHE2_OPEN_SOURCE_SURFACE)
    file(GLOB HDRS "*.h")
    set(SRCS
        flags.cc
        model_context.cc
        model_manager.cc)
else()
    file(GLOB HDRS "*.h" "ips/*.h")
    file(GLOB SRCS "*.cc" "ips/*.cc")
endif()
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
