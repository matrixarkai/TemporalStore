file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_library(bench_workloads ${HDRS} ${SRCS})
target_link_libraries(bench_workloads
    common
    gflags
    bcache2_proto)
