file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
add_executable(common_test ${HDRS} ${SRCS})
target_link_libraries(common_test
    common
    gtest
    test_common)
gtest_discover_tests(common_test DISCOVERY_MODE PRE_TEST)

add_executable(storage_pool_uri_guardrail_test storage_pool_uri_guardrail_test.cc)
target_link_libraries(storage_pool_uri_guardrail_test
    common
    gtest
    gtest_main
    OpenSSL::SSL
    OpenSSL::Crypto)
gtest_discover_tests(storage_pool_uri_guardrail_test DISCOVERY_MODE PRE_TEST)
