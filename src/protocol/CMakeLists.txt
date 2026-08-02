set(_PROTOBUF_LIBPROTOBUF libprotobuf)
set(_PROTOBUF_PROTOC $<TARGET_FILE:protoc>)
#set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:grpc_cpp_plugin>)
set(_PROTO_GENS_DIR ${CMAKE_CURRENT_BINARY_DIR})
set(_PROTO_GEN_INCLUDE_DIR ${CMAKE_BINARY_DIR}/src/protocol)
file(MAKE_DIRECTORY ${_PROTO_GEN_INCLUDE_DIR})

FILE(GLOB proto_files ${PROJECT_SOURCE_DIR}/src/protocol/*.proto)
FOREACH(proto_file ${proto_files})

    GET_FILENAME_COMPONENT(proto_file_absolute "${proto_file}" ABSOLUTE)
    GET_FILENAME_COMPONENT(proto_file_absolute_path "${proto_file_absolute}" PATH)
    GET_FILENAME_COMPONENT(proto_file_name_we "${proto_file}" NAME_WE)

    # Generated header and source files
    SET(proto_file_name_we_src "${_PROTO_GENS_DIR}/${proto_file_name_we}.pb.cc")
    SET(proto_file_name_we_hdr "${_PROTO_GENS_DIR}/${proto_file_name_we}.pb.h")
    SET(proto_file_include_hdr "${_PROTO_GEN_INCLUDE_DIR}/${proto_file_name_we}.pb.h")

    LIST(APPEND PROTO_SRC "${proto_file_name_we_src}")

    ADD_CUSTOM_COMMAND(
        OUTPUT "${proto_file_name_we_src}" "${proto_file_include_hdr}"
        COMMAND ${_PROTOBUF_PROTOC}
            --cpp_out="${_PROTO_GENS_DIR}"
            -I "${proto_file_absolute_path}"
            -I "/usr/include"
            "${proto_file_absolute}"
        # COMMAND mv ${proto_file_name_we_hdr} ${proto_file_include_hdr}
        DEPENDS "${proto_file_absolute}")

    FILE(COPY ${proto_file_absolute} DESTINATION ${_PROTO_GEN_INCLUDE_DIR})
ENDFOREACH(proto_file)

add_library(bcache2_proto STATIC ${PROTO_SRC})
target_link_libraries(bcache2_proto libprotobuf)
target_include_directories(bcache2_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
