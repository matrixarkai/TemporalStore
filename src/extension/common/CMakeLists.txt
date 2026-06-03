add_proto_library(common_proto_lib "*.proto")

add_library(common_module STATIC implement.cc)
target_link_libraries(common_module common_proto_lib partition_compute model)

add_module(common_module common_proto_lib)

if (BCACHE2_BUILD_TESTS)
    add_executable(common_module_test "test.cc")
    target_link_libraries(common_module_test
        client
        mini_cluster_lib
        test_common)
    gtest_discover_tests(common_module_test DISCOVERY_MODE PRE_TEST)
endif()
