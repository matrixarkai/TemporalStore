file(GLOB HDRS "*.h")
file(GLOB SRCS "implement.cc")

add_proto_library(temporal_aggregate_proto_lib "*.proto")
add_library(temporal_aggregate_module STATIC ${HDRS} ${SRCS})
target_link_libraries(temporal_aggregate_module
    temporal_aggregate_proto_lib
    partition_compute
    model)

add_module(temporal_aggregate_module temporal_aggregate_proto_lib)

