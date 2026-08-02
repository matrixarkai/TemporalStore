file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
list(REMOVE_ITEM SRCS "${CMAKE_CURRENT_SOURCE_DIR}/blockcache_smoke.cc")
list(REMOVE_ITEM SRCS "${CMAKE_CURRENT_SOURCE_DIR}/butil_demangle_compat.cc")

add_executable(blockcache_smoke blockcache_smoke.cc butil_demangle_compat.cc)
target_link_libraries(blockcache_smoke
    blockcache
    blockcache-impl
    common
    byte
    gflags)
set_target_properties(blockcache_smoke PROPERTIES CXX_STANDARD 17)

add_executable(blockcache_test ${HDRS} ${SRCS} butil_demangle_compat.cc)
target_link_libraries(blockcache_test
    blockcache
    common
    gmock_main
    gmock
    gtest
    gflags)
target_compile_options(blockcache_test PUBLIC -fno-access-control)
set_target_properties(blockcache_test PROPERTIES CXX_STANDARD 17)
gtest_discover_tests(blockcache_test DISCOVERY_MODE PRE_TEST)
