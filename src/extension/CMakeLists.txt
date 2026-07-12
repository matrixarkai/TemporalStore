add_proto_library(modules_define modules.proto)

add_subdirectory(hash)
add_subdirectory(set)
add_subdirectory(string)
add_subdirectory(feature)
add_subdirectory(context)
add_subdirectory(common)

if (NOT BCACHE2_OPEN_SOURCE_SURFACE)
    add_subdirectory(ips)
    add_subdirectory(risk)
    add_subdirectory(temporal_aggregate)
endif()
