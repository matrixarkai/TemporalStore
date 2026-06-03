file(GLOB TEST_FILES "*_test.cc")
add_executable(storage_test ${TEST_FILES})
target_link_libraries(storage_test
    partition
    recordstream
    test_common
    gtest)
target_compile_options(storage_test PUBLIC -fno-access-control)
gtest_discover_tests(storage_test DISCOVERY_MODE PRE_TEST)
