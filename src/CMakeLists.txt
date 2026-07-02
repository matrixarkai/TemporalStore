include_directories(${PROJECT_SOURCE_DIR}
                    ${PROJECT_SOURCE_DIR}/src
                    ${PROJECT_SOURCE_DIR}/thirdparty/byte
                    ${PROJECT_SOURCE_DIR}/thirdparty/byte/byte
                    ${PROJECT_SOURCE_DIR}/thirdparty/byte/thirdparty/abseil-cpp
                    ${PROJECT_SOURCE_DIR}/thirdparty/byte/thirdparty/json/include
                    ${PROJECT_BINARY_DIR}/thirdparty/brpc/output/include
                    ${PROJECT_BINARY_DIR}/_open_source_brpc/output/include
                    ${PROJECT_SOURCE_DIR}/.local/deps-src/brpc/src
                    ${PROJECT_SOURCE_DIR}/.local/cmake-glue/compat-include
                    ${PROJECT_SOURCE_DIR}/thirdparty/byte/thirdparty/ev/include
                    ${PROJECT_SOURCE_DIR}/thirdparty/libfiu/libfiu
                    ${PROJECT_SOURCE_DIR}/thirdparty/byte/thirdparty/gtest/googletest/include
                    ${PROJECT_SOURCE_DIR}/thirdparty/byte/thirdparty/gtest/googlemock/include
                    ${PROJECT_SOURCE_DIR}/thirdparty/thrift/
                    ${PROJECT_SOURCE_DIR}/thirdparty/thrift/lib/cpp/src
                    ${PROJECT_SOURCE_DIR}/thirdparty/byte/thirdparty/spdlog/include
                    ${OBJECT_STORE_COMPAT_INCLUDE_DIR}
                    ${PROJECT_SOURCE_DIR}/thirdparty/rapidjson/include
                    ${OPENSSL_INCLUDE_DIR}
                    ${BoostInstallDir}/include
                    ${PROJECT_SOURCE_DIR}/thirdparty/byte/thirdparty/lz4/lib
                    ${CMAKE_BINARY_DIR}/src
                    ${PROJECT_SOURCE_DIR}/thirdparty/mtcache/src)
if (ENABLE_MTCACHE)
    add_definitions(-DENABLE_MTCACHE)
endif()

add_subdirectory(common)
add_subdirectory(client)
add_subdirectory(partition)
add_subdirectory(protocol)
add_subdirectory(thrift)
add_subdirectory(model)
add_subdirectory(stream)
add_subdirectory(server)
add_subdirectory(bench)
add_subdirectory(blockcache)
add_subdirectory(proxy)
add_subdirectory(metaserver_v2)

set_property(GLOBAL PROPERTY module_list)

function(add_module module proto_lib)
    target_link_libraries(partition ${module})
    target_link_libraries(client ${module})
    target_link_libraries(client ${proto_lib})
    target_link_libraries(bcache2-shared ${proto_lib})
    get_property(tmp GLOBAL PROPERTY module_list)
    set(target_file $<TARGET_FILE:${module}>)
    set(tmp "${tmp} ${target_file}")
    set_property(GLOBAL PROPERTY module_list "${tmp}")
endfunction()

function(add_proto_library libname files)
    set(_PROTOBUF_PROTOC $<TARGET_FILE:protoc>)
    set(_PROTO_GENS_DIR ${CMAKE_CURRENT_BINARY_DIR})
    file(MAKE_DIRECTORY ${_PROTO_GENS_DIR})

    FILE(GLOB proto_files ${files})
    FOREACH(proto_file ${proto_files})
        GET_FILENAME_COMPONENT(proto_file_absolute "${proto_file}" ABSOLUTE)
        GET_FILENAME_COMPONENT(proto_file_name_we "${proto_file}" NAME_WE)

        # Generated header and source files
        SET(proto_file_name_we_src "${_PROTO_GENS_DIR}/${proto_file_name_we}.pb.cc")
        SET(proto_file_name_we_hdr "${_PROTO_GENS_DIR}/${proto_file_name_we}.pb.h")

        LIST(APPEND PROTO_SRC "${proto_file_name_we_src}")

        ADD_CUSTOM_COMMAND(
            OUTPUT "${proto_file_name_we_src}" "${proto_file_name_we_hdr}"
            COMMAND ${_PROTOBUF_PROTOC}
            --cpp_out=${CMAKE_BINARY_DIR}/src
            -I "${PROJECT_SOURCE_DIR}/src"
            -I "/usr/include"
            "${proto_file_absolute}"
            DEPENDS "${proto_file_absolute}")

        FILE(COPY ${proto_file_absolute} DESTINATION ${_PROTO_GENS_DIR})
    ENDFOREACH(proto_file)
    add_library(${libname} STATIC ${PROTO_SRC})
    target_link_libraries(${libname} libprotobuf)
endfunction()

add_subdirectory(extension)

get_property(MODULE_LIST GLOBAL PROPERTY module_list)
message(INFO "${MODULE_LIST}")

target_link_options(partition
    PUBLIC
    "SHELL: -Wl,--whole-archive ${MODULE_LIST} -Wl,--no-whole-archive"
)

target_link_options(client
    PUBLIC
    "SHELL: -Wl,--whole-archive ${MODULE_LIST} -Wl,--no-whole-archive"
)

target_link_options(proxylib
    PUBLIC
    "SHELL: -Wl,--whole-archive ${MODULE_LIST} -Wl,--no-whole-archive"
)

target_link_options(benchlib
    PUBLIC
    "SHELL: -Wl,--whole-archive ${MODULE_LIST} -Wl,--no-whole-archive"
)
