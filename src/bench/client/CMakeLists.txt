file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_library(bench_client ${HDRS} ${SRCS})
target_link_libraries(bench_client
    bcache2_proto
    bcache2_thrift
    client
    gflags
    brpc-static)
