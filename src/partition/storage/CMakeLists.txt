file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
list(APPEND SRCS data_raft_consensus.cc)
list(REMOVE_DUPLICATES SRCS)
include_directories(${PROJECT_SOURCE_DIR}/thirdparty/byteraft)
add_library(partition_storage ${HDRS} ${SRCS})
target_link_libraries(partition_storage
    partition_index
    bcache2_proto
    common
    blockcache
    matrixobjectstore
    byte
    byteraft
    /usr/lib/x86_64-linux-gnu/libabsl_time_zone.so
    gflags
    libprotobuf
    brpc-static
    lz4_static)
if (BCACHE2_BUILD_TESTS)
    add_subdirectory(test)
endif()
