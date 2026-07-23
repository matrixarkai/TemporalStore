file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_library(partition ${HDRS} ${SRCS})
target_link_libraries(partition
    common
    partition_index
    partition_storage
    partition_compute
    co
    bytestore
    byte
    gflags
    brpc-static
    blockcache
    fiu
    )

add_subdirectory(index)
add_subdirectory(storage)
add_subdirectory(compute)
if (BCACHE2_BUILD_TESTS)
    add_subdirectory(test)
endif()
