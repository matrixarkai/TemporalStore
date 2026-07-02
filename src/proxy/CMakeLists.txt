file(GLOB HDRS "*.h")
file(GLOB SRCS service.cc proxy.cc flags.cc heartbeat.cc)
set(BRPC_SOURCE_DIR "" CACHE PATH "Optional BRPC source tree used to compile thrift protocol compatibility objects")
set(BCACHE2_PROXY_NEEDS_BRPC_THRIFT_COMPAT OFF)
if(BRPC_STATIC_LIBRARY AND UNIX)
    execute_process(
        COMMAND nm -gC "${BRPC_STATIC_LIBRARY}"
        COMMAND grep "ThriftService::ThriftService"
        RESULT_VARIABLE BCACHE2_PROXY_BRPC_THRIFT_SYMBOL_RESULT
        OUTPUT_QUIET
        ERROR_QUIET)
    if(NOT BCACHE2_PROXY_BRPC_THRIFT_SYMBOL_RESULT EQUAL 0)
        set(BCACHE2_PROXY_NEEDS_BRPC_THRIFT_COMPAT ON)
    endif()
endif()
if(BCACHE2_PROXY_NEEDS_BRPC_THRIFT_COMPAT)
    list(APPEND SRCS brpc_thrift_compat.cc)
    set(BCACHE2_PROXY_BRPC_THRIFT_PROTOCOL_SOURCE "")
    foreach(_BRPC_SOURCE_CANDIDATE
            "${BRPC_SOURCE_DIR}"
            "${CMAKE_SOURCE_DIR}/.local/deps-src/brpc"
            "${CMAKE_SOURCE_DIR}/.local/deps-src/brpc"
            "/tmp/brpc-src")
        if(NOT BCACHE2_PROXY_BRPC_THRIFT_PROTOCOL_SOURCE AND
           EXISTS "${_BRPC_SOURCE_CANDIDATE}/src/brpc/policy/thrift_protocol.cpp")
            set(BCACHE2_PROXY_BRPC_THRIFT_PROTOCOL_SOURCE
                "${_BRPC_SOURCE_CANDIDATE}/src/brpc/policy/thrift_protocol.cpp")
        endif()
    endforeach()
    if(BCACHE2_PROXY_BRPC_THRIFT_PROTOCOL_SOURCE)
        list(APPEND SRCS "${BCACHE2_PROXY_BRPC_THRIFT_PROTOCOL_SOURCE}")
        get_filename_component(_BCACHE2_PROXY_BRPC_SOURCE_ROOT
            "${BCACHE2_PROXY_BRPC_THRIFT_PROTOCOL_SOURCE}/../../.." ABSOLUTE)
        if(EXISTS "${_BCACHE2_PROXY_BRPC_SOURCE_ROOT}/brpc/thrift_message.cpp")
            list(APPEND SRCS "${_BCACHE2_PROXY_BRPC_SOURCE_ROOT}/brpc/thrift_message.cpp")
        endif()
    endif()
endif()
add_library(proxylib ${HDRS} ${SRCS})
if(BCACHE2_PROXY_BRPC_THRIFT_PROTOCOL_SOURCE)
    target_compile_definitions(proxylib PRIVATE BCACHE2_PROXY_HAS_BRPC_THRIFT_PROTOCOL_SOURCE)
endif()
target_link_libraries(proxylib
    common
    partition
    brpc-static
    bcache2_thrift
    gflags
    client)
set_target_properties(proxylib PROPERTIES CXX_STANDARD 14)

if (CYGWIN)
    add_executable(bcache2-proxy EXCLUDE_FROM_ALL main.cc ${CMAKE_SOURCE_DIR}/src/common/absl_sampler_compat.cc)
else()
    add_executable(bcache2-proxy main.cc)
endif()
set(BCACHE2_PROXY_LINK_LIBS proxylib partition)
if (TARGET tcmalloc-static)
    list(APPEND BCACHE2_PROXY_LINK_LIBS tcmalloc-static)
endif()
if (TARGET absl_hashtablez_sampler)
    list(APPEND BCACHE2_PROXY_LINK_LIBS absl_hashtablez_sampler)
endif()
target_link_libraries(bcache2-proxy
    ${BCACHE2_PROXY_LINK_LIBS}
    -Wl,--whole-archive
    $<TARGET_FILE:partition>
    -Wl,--no-whole-archive
    brpc-static
    byte
    byteraft
    /usr/lib/x86_64-linux-gnu/libleveldb.so
    /usr/lib/x86_64-linux-gnu/libfmt.so
    OpenSSL::SSL
    OpenSSL::Crypto
    /usr/lib/x86_64-linux-gnu/libabsl_throw_delegate.so
    /usr/lib/x86_64-linux-gnu/libabsl_time_zone.so
    /usr/lib/x86_64-linux-gnu/libabsl_raw_logging_internal.so
    /usr/lib/x86_64-linux-gnu/libabsl_log_severity.so)

if (BCACHE2_BUILD_TESTS)
    add_subdirectory(test)
endif()
