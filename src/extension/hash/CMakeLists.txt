add_proto_library(hash_proto_lib "*.proto")

add_library(hash_module STATIC implement.cc)
target_link_libraries(hash_module hash_proto_lib partition_compute model)

add_module(hash_module hash_proto_lib)

if (BCACHE2_BUILD_TESTS)
    add_executable(hash_module_test "test.cc")
    target_link_libraries(hash_module_test
        client
        mini_cluster_lib
        test_common)
    gtest_discover_tests(hash_module_test DISCOVERY_MODE PRE_TEST)
endif()
