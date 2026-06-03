add_proto_library(string_proto_lib "*.proto")

add_library(string_module STATIC implement.cc)
target_link_libraries(string_module string_proto_lib partition_compute model)

add_module(string_module string_proto_lib)

if (BCACHE2_BUILD_TESTS)
    add_executable(string_module_test "test.cc")
    target_link_libraries(string_module_test
        client
        mini_cluster_lib
        test_common)
    gtest_discover_tests(string_module_test DISCOVERY_MODE PRE_TEST)
endif()
