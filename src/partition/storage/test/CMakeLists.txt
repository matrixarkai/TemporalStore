file(GLOB TEST_FILES "*_test.cc")
add_executable(storage_test ${TEST_FILES})
target_link_libraries(storage_test
    partition
    recordstream
    test_common
    gtest)
target_compile_options(storage_test PUBLIC -fno-access-control)
gtest_discover_tests(storage_test DISCOVERY_MODE PRE_TEST)

add_executable(data_raft_replication_codec_smoke data_raft_replication_test.cc)
target_link_libraries(data_raft_replication_codec_smoke
    partition_storage
    partition
    gtest)
target_compile_options(data_raft_replication_codec_smoke PUBLIC -fno-access-control)
