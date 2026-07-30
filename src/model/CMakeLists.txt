file(GLOB HDRS "*.h" "ips/*.h")
file(GLOB SRCS "*.cc" "ips/*.cc")
if (NOT EXISTS "${CMAKE_BINARY_DIR}/src/extension/control_state/interface.pb.h")
    list(REMOVE_ITEM HDRS "${CMAKE_CURRENT_SOURCE_DIR}/control_state_hash_model.h")
    list(REMOVE_ITEM HDRS "${CMAKE_CURRENT_SOURCE_DIR}/control_state_cpc_model.h")
    list(REMOVE_ITEM SRCS "${CMAKE_CURRENT_SOURCE_DIR}/control_state_hash_model.cc")
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
