file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_library(rdma_read_cache ${HDRS} ${SRCS})
target_link_libraries(rdma_read_cache
    rdma_read_cache_common
    rdma_read_cache_allocator
    rdma_read_cache_replacement_policy
    rdma_read_cache_index
    rdma_read_cache_storage)

add_subdirectory(common)
add_subdirectory(allocator)
add_subdirectory(replacement_policy)
add_subdirectory(index)
add_subdirectory(storage)
if (BCACHE2_BUILD_TESTS)
    add_subdirectory(test)
endif()
