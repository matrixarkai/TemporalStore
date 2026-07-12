file(GLOB HDRS CONFIGURE_DEPENDS "*.h" "store/*.h")
file(GLOB SRCS CONFIGURE_DEPENDS "*.cc" "store/*.cc")
find_package(CURL)
if (NOT CURL_FOUND)
    list(REMOVE_ITEM SRCS "${CMAKE_CURRENT_SOURCE_DIR}/store/s3_store.cc")
endif()
add_library(recordstream ${HDRS} ${SRCS})
target_link_libraries(recordstream
    common
    bcache2_proto
    matrixobjectstore
    byte
    gflags
    fiu)
if (CURL_FOUND)
    target_link_libraries(recordstream CURL::libcurl OpenSSL::Crypto)
    target_compile_definitions(recordstream PUBLIC BCACHE2_ENABLE_S3_STORE)
endif()

if (BCACHE2_BUILD_TESTS)
    add_subdirectory(test)
endif()
add_subdirectory(tools)
