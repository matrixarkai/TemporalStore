add_executable(hash_model_test hash_model_test.cc)
target_link_libraries(hash_model_test
    bcache2_proto
    partition_storage
    model
    gtest
    test_common)
add_executable(feature_model_test feature_model_test.cc)
target_link_libraries(feature_model_test
    bcache2_proto
    partition_storage
    model
    gtest
    test_common)
add_executable(ips_model_test ips_model_test.cc)
target_link_libraries(ips_model_test
    bcache2_proto
    partition_storage
    model
    gtest
    test_common)
add_executable(risk_hash_model_test risk_hash_model_test.cc)
target_link_libraries(risk_hash_model_test
    bcache2_proto
    partition_storage
    model
    gtest
    test_common)
add_executable(cpc_model_test cpc_model_test.cc)
target_link_libraries(cpc_model_test
    bcache2_proto
    partition_storage
    model
    gtest
    test_common)

gtest_discover_tests(hash_model_test DISCOVERY_MODE PRE_TEST)
gtest_discover_tests(feature_model_test DISCOVERY_MODE PRE_TEST)
gtest_discover_tests(ips_model_test DISCOVERY_MODE PRE_TEST)
gtest_discover_tests(risk_hash_model_test DISCOVERY_MODE PRE_TEST)
gtest_discover_tests(cpc_model_test DISCOVERY_MODE PRE_TEST)

target_compile_options(hash_model_test PUBLIC -fno-access-control)
target_compile_options(ips_model_test PUBLIC -fno-access-control)
target_compile_options(feature_model_test PUBLIC -fno-access-control)
target_compile_options(risk_hash_model_test PUBLIC -fno-access-control)
target_compile_options(cpc_model_test PUBLIC -fno-access-control)
