file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")

add_executable(blockcache_smoke blockcache_smoke.cc)
target_link_libraries(blockcache_smoke
    blockcache
    blockcache-impl
    common
    byte
    gflags)
set_target_properties(blockcache_smoke PROPERTIES CXX_STANDARD 17)

add_executable(blockcache_test ${HDRS} ${SRCS})
target_link_libraries(blockcache_test
    blockcache
    partition
    common
    gtest
    test_common
    gflags)
target_compile_options(blockcache_test PUBLIC -fno-access-control)
set_target_properties(blockcache_test PROPERTIES CXX_STANDARD 17)
gtest_discover_tests(blockcache_test)
