include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++17" COMPILER_SUPPORTS_CXX17)
if(COMPILER_SUPPORTS_CXX17)
    set(CMAKE_CXX_STANDARD 17)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(${CMAKE_SOURCE_DIR}/src
    ${PROJECT_SOURCE_DIR}/thirdparty/byteraft)

list(APPEND SRC
    meta/serializable.cc
    meta/metabase.cc
    meta/namespace.cc
    meta/partition.cc
    meta/server.cc
    meta/table.cc
    meta/proxy.cc

    scheduler/create_table_task.cc
    scheduler/create_partition_task.cc
    scheduler/update_membership_task.cc
    scheduler/balance_table_task.cc
    scheduler/scheduler_manager.cc
    scheduler/task_scheduler.cc

    resource_placement/placement_manager.cc
    resource_placement/placement_rule.cc
    resource_placement/host_dedup_placement_rule.cc
    resource_placement/load_filter_placement_rule.cc
    resource_placement/location_placement_rule.cc

    service/manage_service.cc
    service/query_service.cc
    service/heartbeat_service.cc
    service/meta_query_service.cc
    service/raft_control_service.cc

    client/partition_server_client.cc

    ha/failure_detector.cc
    ha/convict_routine.cc
    ha/meta_check_routine.cc
    ha/proxy_calibrate_routine.cc

    balance/balance_routine.cc

    metrics.cc
    meta_publisher.cc
    event_harbor.cc
    trivial_routine.cc
    flags.cc
    fsm.cc
    raft_connector.cc
    raft_server.cc
    metaserver.cc)

set(SRC ${SRC} PARENT_SCOPE)

list(APPEND THIRD_LIB
    gflags
    brpc-static
    byte
    byteraft)

if (CYGWIN)
    add_library(metaserver EXCLUDE_FROM_ALL ${SRC})
else()
    add_library(metaserver ${SRC})
endif()
target_link_libraries(metaserver
    common
    bcache2_proto
    ${THIRD_LIB})
set_target_properties(metaserver PROPERTIES CXX_STANDARD 17)

if (CYGWIN)
    add_executable(bcache2-metaserver EXCLUDE_FROM_ALL main.cc ${CMAKE_SOURCE_DIR}/src/common/absl_sampler_compat.cc)
else()
    add_executable(bcache2-metaserver main.cc)
endif()
set(BCACHE2_METASERVER_LINK_LIBS metaserver)
if (TARGET absl_hashtablez_sampler)
    list(APPEND BCACHE2_METASERVER_LINK_LIBS absl_hashtablez_sampler)
endif()
target_link_libraries(bcache2-metaserver
    ${BCACHE2_METASERVER_LINK_LIBS}
    brpc-static
    byte
    byteraft
    /usr/lib/x86_64-linux-gnu/libleveldb.so
    /usr/lib/x86_64-linux-gnu/libfmt.so
    OpenSSL::SSL
    OpenSSL::Crypto
    /usr/lib/x86_64-linux-gnu/libabsl_throw_delegate.so
    /usr/lib/x86_64-linux-gnu/libabsl_time_zone.so
    /usr/lib/x86_64-linux-gnu/libabsl_raw_logging_internal.so
    /usr/lib/x86_64-linux-gnu/libabsl_log_severity.so
    /usr/lib/x86_64-linux-gnu/libabsl_hash.so
    /usr/lib/x86_64-linux-gnu/libabsl_raw_hash_set.so
    /usr/lib/x86_64-linux-gnu/libabsl_city.so)

list(APPEND FE_SRC
    fe/api_gateway.cc
)
if (CYGWIN)
    add_library(metaserver_fe EXCLUDE_FROM_ALL ${FE_SRC})
else()
    add_library(metaserver_fe ${FE_SRC})
endif()
target_link_libraries(metaserver_fe
    metaserver
    common
    bcache2_proto
    ${THIRD_LIB})
set_target_properties(metaserver_fe PROPERTIES CXX_STANDARD 14)
if (CYGWIN)
    add_executable(bcache2-metaserver-fe EXCLUDE_FROM_ALL fe/main.cc ${CMAKE_SOURCE_DIR}/src/common/absl_sampler_compat.cc)
else()
    add_executable(bcache2-metaserver-fe fe/main.cc)
endif()
set(BCACHE2_METASERVER_FE_LINK_LIBS metaserver_fe)
if (TARGET absl_hashtablez_sampler)
    list(APPEND BCACHE2_METASERVER_FE_LINK_LIBS absl_hashtablez_sampler)
endif()
target_link_libraries(bcache2-metaserver-fe
    ${BCACHE2_METASERVER_FE_LINK_LIBS}
    brpc-static
    byte
    byteraft
    /usr/lib/x86_64-linux-gnu/libleveldb.so
    /usr/lib/x86_64-linux-gnu/libfmt.so
    OpenSSL::SSL
    OpenSSL::Crypto
    /usr/lib/x86_64-linux-gnu/libabsl_throw_delegate.so
    /usr/lib/x86_64-linux-gnu/libabsl_time_zone.so
    /usr/lib/x86_64-linux-gnu/libabsl_raw_logging_internal.so
    /usr/lib/x86_64-linux-gnu/libabsl_log_severity.so
    /usr/lib/x86_64-linux-gnu/libabsl_hash.so
    /usr/lib/x86_64-linux-gnu/libabsl_raw_hash_set.so
    /usr/lib/x86_64-linux-gnu/libabsl_city.so)

if (BCACHE2_BUILD_TESTS)
    file(GLOB TEST_FILES "test/*_test.cc")
    add_executable(ms_test ${TEST_FILES})
    target_link_libraries(ms_test
        mini_cluster_lib
        test_common
        gtest)
    set_target_properties(ms_test PROPERTIES CXX_STANDARD 17)
    target_compile_options(ms_test PUBLIC -fno-access-control)
    gtest_discover_tests(ms_test DISCOVERY_MODE PRE_TEST)
endif()
