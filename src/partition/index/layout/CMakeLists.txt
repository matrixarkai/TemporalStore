file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_library(partition_layout ${HDRS} ${SRCS})
target_link_libraries(partition_layout
    byte)
