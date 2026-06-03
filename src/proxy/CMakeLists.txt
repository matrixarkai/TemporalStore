file(GLOB HDRS "*.h")
file(GLOB SRCS service.cc proxy.cc flags.cc heartbeat.cc)
add_library(proxylib ${HDRS} ${SRCS})
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
    -Wl,--no-whole-archive)

if (BCACHE2_BUILD_TESTS)
    add_subdirectory(test)
endif()
