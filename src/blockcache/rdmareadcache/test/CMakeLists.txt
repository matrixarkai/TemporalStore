file(GLOB HDRS "*.h")
file(GLOB TEST_FILES "*.cc")
add_executable(rdmareadcache_test ${TEST_FILES})
target_link_libraries(rdmareadcache_test
    rdma_read_cache_common
    rdma_read_cache_allocator
    rdma_read_cache_replacement_policy
    rdma_read_cache_index
    rdma_read_cache_storage
    rdma_read_cache
    gtest)
gtest_discover_tests(rdmareadcache_test DISCOVERY_MODE PRE_TEST)
