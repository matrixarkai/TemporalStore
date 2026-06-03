add_proto_library(ips_proto_lib "*.proto")

add_library(ips_module STATIC implement.cc)
target_link_libraries(ips_module ips_proto_lib partition_compute model)

add_module(ips_module ips_proto_lib)

if (BCACHE2_BUILD_TESTS)
    add_executable(ips_module_test "test.cc")
    target_link_libraries(ips_module_test
        client
        mini_cluster_lib
        test_common
        ips_module)
    gtest_discover_tests(ips_module_test DISCOVERY_MODE PRE_TEST)
endif()
