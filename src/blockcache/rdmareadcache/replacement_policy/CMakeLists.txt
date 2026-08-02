file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_library(rdma_read_cache_replacement_policy ${HDRS} ${SRCS})
target_link_libraries(rdma_read_cache_replacement_policy)