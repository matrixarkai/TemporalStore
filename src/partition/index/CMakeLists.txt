file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_library(partition_index ${HDRS} ${SRCS})
target_link_libraries(partition_index
    partition_layout
    bcache2_proto
    recordstream
    byte
    gflags)

add_subdirectory(layout)
if (BCACHE2_BUILD_TESTS)
    add_subdirectory(test)
endif()
