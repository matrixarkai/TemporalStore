add_proto_library(hash_proto_lib "*.proto")

add_library(hash_module STATIC implement.cc)
target_link_libraries(hash_module hash_proto_lib partition_compute model)

add_module(hash_module hash_proto_lib)
