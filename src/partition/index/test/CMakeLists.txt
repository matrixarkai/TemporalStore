file(GLOB TEST_FILES "*_test.cc")
add_executable(index_test ${TEST_FILES})
target_link_libraries(index_test
    partition
    test_common)
target_compile_options(index_test PUBLIC -fno-access-control)
gtest_discover_tests(index_test DISCOVERY_MODE PRE_TEST)
