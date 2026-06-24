add_proto_library(string_proto_lib "*.proto")

add_library(string_module STATIC implement.cc)
target_link_libraries(string_module string_proto_lib partition_compute model)

add_module(string_module string_proto_lib)
