file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_library(bench_model ${HDRS} ${SRCS})
target_link_libraries(bench_model
    common
    bcache2_proto)
