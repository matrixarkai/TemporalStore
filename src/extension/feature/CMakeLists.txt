add_proto_library(feature_proto_lib "*.proto")

add_library(feature_module STATIC implement.cc flags.cc)
target_link_libraries(feature_module feature_proto_lib partition_compute model)

add_module(feature_module feature_proto_lib)

if (BCACHE2_BUILD_TESTS)
    add_executable(feature_module_test test.cc)
    target_link_libraries(feature_module_test
        client
        mini_cluster_lib
        test_common)
    gtest_discover_tests(feature_module_test DISCOVERY_MODE PRE_TEST)
endif()
