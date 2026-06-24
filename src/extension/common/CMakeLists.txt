add_proto_library(common_proto_lib "*.proto")

add_library(common_module STATIC implement.cc)
target_link_libraries(common_module common_proto_lib partition_compute model)

add_module(common_module common_proto_lib)
