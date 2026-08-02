add_proto_library(context_proto_lib "*.proto")

add_library(context_module STATIC implement.cc)
target_link_libraries(context_module context_proto_lib partition_compute model)

add_module(context_module context_proto_lib)

if (BCACHE2_BUILD_TESTS)
    add_executable(context_module_test test.cc)
    target_link_libraries(context_module_test
        client
        mini_cluster_lib
        test_common)
    gtest_discover_tests(context_module_test DISCOVERY_MODE PRE_TEST)
endif()
