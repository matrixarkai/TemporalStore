add_proto_library(set_proto_lib "*.proto")

add_library(set_module STATIC implement.cc)
target_link_libraries(set_module set_proto_lib partition_compute model)

add_module(set_module set_proto_lib)

if (BCACHE2_BUILD_TESTS)
    add_executable(set_module_test "test.cc")
    target_link_libraries(set_module_test
        client
        mini_cluster_lib
        test_common)
    gtest_discover_tests(set_module_test DISCOVERY_MODE PRE_TEST)
endif()
