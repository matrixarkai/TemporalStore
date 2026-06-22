file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_library(client ${HDRS} ${SRCS})
target_link_libraries(client
    common
    bcache2_proto)

if (CYGWIN)
    add_library(bcache2-shared SHARED EXCLUDE_FROM_ALL ${HDRS} ${SRCS})
else()
    add_library(bcache2-shared SHARED ${HDRS} ${SRCS})
endif()
target_link_directories(bcache2-shared PRIVATE /root/src/cmake-glue/lib)
target_link_libraries(bcache2-shared
    common
    bcache2_proto
    -Wl,--whole-archive
    brpc-static
    -Wl,--no-whole-archive
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
set_target_properties(bcache2-shared PROPERTIES
                      LINKER_LANGUAGE CXX
                      CXX_STANDARD 17
                      LINK_FLAGS "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/exported_symbols.lds"
                      # VERSION "1.1"
                      OUTPUT_NAME "bcache2")

if (BCACHE2_BUILD_TESTS)
    add_subdirectory(test)
endif()
add_subdirectory(example)
