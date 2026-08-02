add_executable(stream_test stream_test.cc)
target_link_libraries(stream_test
    recordstream
    test_common
    gflags
    gtest)
target_compile_options(stream_test PUBLIC -fno-access-control)
gtest_discover_tests(stream_test DISCOVERY_MODE PRE_TEST)

add_executable(stream_bench stream_bench.cc)
target_link_libraries(stream_bench
    recordstream
    test_common
    gflags
    gtest)
target_compile_options(stream_bench PUBLIC -fno-access-control)

add_executable(store_test store_layer_test.cc)
target_link_libraries(store_test
    recordstream
    test_common
    gflags
    gtest)
target_compile_options(store_test PUBLIC -fno-access-control)
gtest_discover_tests(store_test DISCOVERY_MODE PRE_TEST)

add_executable(object_store_guardrail_test object_store_guardrail_test.cc)
target_link_libraries(object_store_guardrail_test
    recordstream
    gtest
    gtest_main
    OpenSSL::SSL
    OpenSSL::Crypto)
target_compile_options(object_store_guardrail_test PUBLIC -fno-access-control)
gtest_discover_tests(object_store_guardrail_test DISCOVERY_MODE PRE_TEST)

add_executable(stream_unittest log_based_stream_test.cc)
target_link_libraries(stream_unittest
    recordstream
    test_common
    gflags
    gtest
    gmock)
target_compile_options(stream_unittest PUBLIC -fno-access-control)
gtest_discover_tests(stream_unittest DISCOVERY_MODE PRE_TEST)
