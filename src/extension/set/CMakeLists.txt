add_proto_library(set_proto_lib "*.proto")

if (NOT BCACHE2_OPEN_SOURCE_SURFACE)
    add_library(set_module STATIC implement.cc)
    target_link_libraries(set_module set_proto_lib partition_compute model)

    add_module(set_module set_proto_lib)
endif()
