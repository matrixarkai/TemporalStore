file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_library(partition_storage ${HDRS} ${SRCS})
target_link_libraries(partition_storage
    partition_index
    bcache2_proto
    common
    blockcache
    bytestore
    byte
    gflags
    libprotobuf
    brpc-static
    lz4_static)
if (BCACHE2_BUILD_TESTS)
    add_subdirectory(test)
endif()
