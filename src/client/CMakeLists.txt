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
target_link_libraries(bcache2-shared
    common
    bcache2_proto)
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
