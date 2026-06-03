file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_library(rdma_read_cache_common ${HDRS} ${SRCS})
target_link_libraries(rdma_read_cache_common 
    rdma_read_cache_allocator
    rdma_read_cache_replacement_policy
    rdma_read_cache_index)
