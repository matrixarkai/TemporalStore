file(GLOB TEST_FILES "*_test.cc")
add_executable(partition_test mem_stream.h ${TEST_FILES})
target_link_libraries(partition_test
    partition
    serverlib
    benchlib
    recordstream
    test_common
    gtest)
target_compile_options(partition_test PUBLIC -fno-access-control)
gtest_discover_tests(partition_test DISCOVERY_MODE PRE_TEST)

add_executable(partition_bench mem_stream.h partition_bench.cc)
target_link_libraries(partition_bench
    partition
    recordstream
    test_common
    gtest)
target_compile_options(partition_bench PUBLIC -fno-access-control)
