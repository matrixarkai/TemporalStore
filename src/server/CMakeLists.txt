file(GLOB HDRS "*.h")
file(GLOB SRCS partition_manager.cc service.cc server.cc redis_service.cc redis_command_handler.cc heartbeat.cc meta_tinker.cc)
add_library(serverlib ${HDRS} ${SRCS})
target_link_libraries(serverlib
    common
    partition
    bcache2_proto
    gflags
    brpc-static
    blockcache)
set_target_properties(serverlib PROPERTIES CXX_STANDARD 14)

if (CYGWIN)
    add_executable(bcache2-server EXCLUDE_FROM_ALL main.cc ${CMAKE_SOURCE_DIR}/src/common/absl_sampler_compat.cc)
else()
    add_executable(bcache2-server main.cc)
endif()
set(BCACHE2_SERVER_LINK_LIBS serverlib)
if (TARGET tcmalloc-static)
    list(APPEND BCACHE2_SERVER_LINK_LIBS tcmalloc-static)
endif()
if (TARGET absl_hashtablez_sampler)
    list(APPEND BCACHE2_SERVER_LINK_LIBS absl_hashtablez_sampler)
endif()
if (CYGWIN)
    list(APPEND BCACHE2_SERVER_LINK_LIBS snappy)
endif()
target_link_libraries(bcache2-server
    ${BCACHE2_SERVER_LINK_LIBS}
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
    /usr/lib/x86_64-linux-gnu/libabsl_log_severity.so
    /usr/lib/x86_64-linux-gnu/libabsl_time_zone.so)
target_compile_options(bcache2-server PUBLIC "-DBRPC_ENABLE_CPU_PROFILER")
target_link_options(bcache2-server PRIVATE "-Wl,--no-as-needed")
target_link_libraries(bcache2-server /usr/lib/x86_64-linux-gnu/libabsl_time_zone.so)
if (CYGWIN)
    target_link_options(bcache2-server PRIVATE
        "-fuse-ld=lld"
        "-L/usr/lib/w32api"
        "-Wl,--allow-multiple-definition"
        "-Wl,--strip-debug")
endif()

if (BCACHE2_BUILD_TESTS)
    add_subdirectory(test)
endif()
