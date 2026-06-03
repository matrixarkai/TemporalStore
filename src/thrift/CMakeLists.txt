set(_THRIFT_BIN $<TARGET_FILE:thrift-compiler>)
set(_THRIFT_GENS_DIR ${CMAKE_CURRENT_BINARY_DIR})
set(_THRIFT_GEN_INCLUDE_DIR ${CMAKE_BINARY_DIR}/src/thrift)
file(MAKE_DIRECTORY ${_THRIFT_GEN_INCLUDE_DIR})

FILE(GLOB thrift_files ${PROJECT_SOURCE_DIR}/src/thrift/*.thrift)
FOREACH(thrift_file ${thrift_files})
    GET_FILENAME_COMPONENT(thrift_file_absolute "${thrift_file}" ABSOLUTE)
    GET_FILENAME_COMPONENT(thrift_file_absolute_path "${thrift_file_absolute}" PATH)
    GET_FILENAME_COMPONENT(thrift_file_name_we "${thrift_file}" NAME_WE)

    # Generated header and source files
    SET(thrift_file_name_we_src "${_THRIFT_GENS_DIR}/${thrift_file_name_we}_types.cpp")
    SET(thrift_file_name_we_hdr "${_THRIFT_GENS_DIR}/${thrift_file_name_we}_types.h")
    SET(thrift_file_include_hdr "${_THRIFT_GEN_INCLUDE_DIR}/${thrift_file_name_we}_types.h")

    LIST(APPEND THRIFT_SRC "${thrift_file_name_we_src}")

    ADD_CUSTOM_COMMAND(
        OUTPUT "${thrift_file_name_we_src}" "${thrift_file_include_hdr}"
        COMMAND ${_THRIFT_BIN} -r -out ${_THRIFT_GENS_DIR} --gen cpp "${thrift_file_absolute}"
        DEPENDS "${thrift_file_absolute}")

    FILE(COPY ${thrift_file_absolute} DESTINATION ${_THRIFT_GEN_INCLUDE_DIR})
ENDFOREACH(thrift_file)

add_library(bcache2_thrift STATIC ${THRIFT_SRC})
target_compile_options(bcache2_thrift PRIVATE
    $<$<CXX_COMPILER_ID:GNU>:-Wno-error=deprecated-declarations>)
target_link_libraries(bcache2_thrift thrift_static)
target_include_directories(bcache2_thrift PUBLIC
    ${CMAKE_BINARY_DIR}/thirdparty/thrift
    ${PROJECT_SOURCE_DIR}/thirdparty/thrift/lib/cpp/src)
