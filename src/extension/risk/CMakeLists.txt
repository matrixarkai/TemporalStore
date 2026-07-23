file(GLOB HDRS "*.h")
file(GLOB SRCS "window.cc" "implement.cc")

add_proto_library(risk_proto_lib "*.proto")
add_library(risk_module STATIC ${HDRS} ${SRCS})
target_link_libraries(risk_module
    risk_proto_lib
    partition_compute
    model
    absl_hash
    absl_raw_hash_set)

add_module(risk_module risk_proto_lib)

if (BCACHE2_BUILD_TESTS)
    add_executable(risk_module_test "test.cc")
    target_link_libraries(risk_module_test
        risk_module
        client
        mini_cluster_lib
        test_common
    )

    gtest_discover_tests(risk_module_test DISCOVERY_MODE PRE_TEST)

    add_executable(risk_module_window_test "window_test.cc")
    target_link_libraries(risk_module_window_test
        gtest
        test_common
        risk_module)

    gtest_discover_tests(risk_module_window_test DISCOVERY_MODE PRE_TEST)
endif()
