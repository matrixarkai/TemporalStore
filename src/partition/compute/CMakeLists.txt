file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_library(partition_compute ${HDRS} ${SRCS})
target_link_libraries(partition_compute common partition_storage partition_index model)
