if (CYGWIN)
    add_executable(client_example EXCLUDE_FROM_ALL client_example.cc)
    add_executable(module_ingest_query_example EXCLUDE_FROM_ALL module_ingest_query_example.cc)
    add_executable(module_latency_benchmark EXCLUDE_FROM_ALL module_latency_benchmark.cc)
    add_executable(customer_client_example EXCLUDE_FROM_ALL customer_client_example.cc)
    add_executable(customer_c_client_example EXCLUDE_FROM_ALL customer_c_client_example.c)
    add_executable(replication_smoke_example EXCLUDE_FROM_ALL replication_smoke_example.cc)
    add_executable(secondary_visibility_lag_benchmark EXCLUDE_FROM_ALL secondary_visibility_lag_benchmark.cc)
    add_executable(string_scale_benchmark EXCLUDE_FROM_ALL string_scale_benchmark.cc)
    add_executable(feature_sequence_benchmark EXCLUDE_FROM_ALL feature_sequence_benchmark.cc)
    add_executable(temporal_aggregate_scale_benchmark EXCLUDE_FROM_ALL temporal_aggregate_scale_benchmark.cc)
    add_executable(temporal_aggregate_lag_sweep EXCLUDE_FROM_ALL temporal_aggregate_lag_sweep.cc)
    if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/temporal_aggregate_mixed_benchmark.cc")
        add_executable(temporal_aggregate_mixed_benchmark EXCLUDE_FROM_ALL temporal_aggregate_mixed_benchmark.cc)
    endif()
    add_executable(proxy_smoke_example EXCLUDE_FROM_ALL proxy_smoke_example.cc)
    add_executable(proxy_ingestion_pressure_example EXCLUDE_FROM_ALL proxy_ingestion_pressure_example.cc)
    add_executable(queue_ingestion_replay_example EXCLUDE_FROM_ALL queue_ingestion_replay_example.cc)
else()
    add_executable(client_example client_example.cc)
    add_executable(module_ingest_query_example module_ingest_query_example.cc)
    add_executable(module_latency_benchmark module_latency_benchmark.cc)
    add_executable(customer_client_example customer_client_example.cc)
    add_executable(customer_c_client_example customer_c_client_example.c)
    add_executable(replication_smoke_example replication_smoke_example.cc)
    add_executable(secondary_visibility_lag_benchmark secondary_visibility_lag_benchmark.cc)
    add_executable(string_scale_benchmark string_scale_benchmark.cc)
    add_executable(feature_sequence_benchmark feature_sequence_benchmark.cc)
    add_executable(temporal_aggregate_scale_benchmark temporal_aggregate_scale_benchmark.cc)
    add_executable(temporal_aggregate_lag_sweep temporal_aggregate_lag_sweep.cc)
    if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/temporal_aggregate_mixed_benchmark.cc")
        add_executable(temporal_aggregate_mixed_benchmark temporal_aggregate_mixed_benchmark.cc)
    endif()
    add_executable(proxy_smoke_example proxy_smoke_example.cc)
    add_executable(proxy_ingestion_pressure_example proxy_ingestion_pressure_example.cc)
    add_executable(queue_ingestion_replay_example queue_ingestion_replay_example.cc)
endif()

set(BCACHE2_CLIENT_EXAMPLE_OPTIONAL_LIBS)
foreach(optional_lib isal-static cryptopp-static)
    if (TARGET ${optional_lib})
        list(APPEND BCACHE2_CLIENT_EXAMPLE_OPTIONAL_LIBS ${optional_lib})
    endif()
endforeach()
if (TARGET thrift_static)
    set(BCACHE2_CLIENT_EXAMPLE_THRIFT_LIB thrift_static)
else()
    set(BCACHE2_CLIENT_EXAMPLE_THRIFT_LIB /usr/lib/x86_64-linux-gnu/libthrift.so)
endif()

set(BCACHE2_CLIENT_EXAMPLE_LIBS
    client
    partition
    partition_storage
    bcache2_proto
    modules_define
    common_proto_lib
    hash_proto_lib
    string_proto_lib
    set_proto_lib
    feature_proto_lib
    ips_proto_lib
    risk_proto_lib
    temporal_aggregate_proto_lib
    byte
    absl_base
    absl_int128
    absl_spinlock_wait
    absl_stacktrace
    absl_str_format_internal
    absl_strings
    absl_symbolize
    absl_synchronization
    absl_time
    gflags
    brpc-static
    ${BCACHE2_CLIENT_EXAMPLE_OPTIONAL_LIBS}
    zookeeper_mt)

if (CYGWIN)
    set(BCACHE2_CLIENT_EXAMPLE_LINK_LIBS ${BCACHE2_CLIENT_EXAMPLE_LIBS})
else()
    set(BCACHE2_CLIENT_EXAMPLE_LINK_LIBS
        -Wl,--start-group
        ${BCACHE2_CLIENT_EXAMPLE_LIBS}
        -Wl,--end-group
        /usr/lib/x86_64-linux-gnu/libleveldb.so
        /usr/lib/x86_64-linux-gnu/libfmt.so
        /usr/lib/x86_64-linux-gnu/libabsl_hash.so
        /usr/lib/x86_64-linux-gnu/libabsl_raw_hash_set.so
        /usr/lib/x86_64-linux-gnu/libabsl_throw_delegate.so
        /usr/lib/x86_64-linux-gnu/libabsl_time_zone.so
        /usr/lib/x86_64-linux-gnu/libabsl_raw_logging_internal.so
        /usr/lib/x86_64-linux-gnu/libabsl_log_severity.so
        /usr/lib/x86_64-linux-gnu/libprotobuf.so
        ${OPENSSL_SSL_LIBRARY}
        ${OPENSSL_CRYPTO_LIBRARY})
endif()

target_link_libraries(client_example ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS})
target_link_libraries(module_ingest_query_example ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS})
target_link_libraries(module_latency_benchmark ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS})
target_link_libraries(customer_client_example ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS})
target_link_libraries(customer_c_client_example ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS})
target_link_libraries(replication_smoke_example ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS})
target_link_libraries(secondary_visibility_lag_benchmark ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS})
target_link_libraries(string_scale_benchmark ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS})
target_link_libraries(feature_sequence_benchmark ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS})
target_link_libraries(temporal_aggregate_scale_benchmark ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS})
target_link_libraries(temporal_aggregate_lag_sweep ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS})
if (TARGET temporal_aggregate_mixed_benchmark)
    target_link_libraries(temporal_aggregate_mixed_benchmark ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS})
endif()
set(BCACHE2_CLIENT_EXAMPLE_BRPC_THRIFT_COMPAT_SOURCES
    ${CMAKE_SOURCE_DIR}/src/proxy/brpc_thrift_compat.cc)
foreach(_BRPC_SOURCE_CANDIDATE
        "${BRPC_SOURCE_DIR}"
        "${CMAKE_SOURCE_DIR}/.local/deps-src/brpc"
        "${CMAKE_SOURCE_DIR}/.local/deps-src/brpc"
        "/tmp/brpc-src")
    if(NOT BCACHE2_CLIENT_EXAMPLE_BRPC_THRIFT_PROTOCOL_SOURCE AND
       EXISTS "${_BRPC_SOURCE_CANDIDATE}/src/brpc/policy/thrift_protocol.cpp")
        set(BCACHE2_CLIENT_EXAMPLE_BRPC_THRIFT_PROTOCOL_SOURCE
            "${_BRPC_SOURCE_CANDIDATE}/src/brpc/policy/thrift_protocol.cpp")
    endif()
endforeach()
if(BCACHE2_CLIENT_EXAMPLE_BRPC_THRIFT_PROTOCOL_SOURCE)
    list(APPEND BCACHE2_CLIENT_EXAMPLE_BRPC_THRIFT_COMPAT_SOURCES
        "${BCACHE2_CLIENT_EXAMPLE_BRPC_THRIFT_PROTOCOL_SOURCE}")
    get_filename_component(_BCACHE2_CLIENT_EXAMPLE_BRPC_SOURCE_ROOT
        "${BCACHE2_CLIENT_EXAMPLE_BRPC_THRIFT_PROTOCOL_SOURCE}/../../.." ABSOLUTE)
    if(EXISTS "${_BCACHE2_CLIENT_EXAMPLE_BRPC_SOURCE_ROOT}/brpc/thrift_message.cpp")
        list(APPEND BCACHE2_CLIENT_EXAMPLE_BRPC_THRIFT_COMPAT_SOURCES
            "${_BCACHE2_CLIENT_EXAMPLE_BRPC_SOURCE_ROOT}/brpc/thrift_message.cpp")
    endif()
endif()
target_sources(proxy_smoke_example PRIVATE ${BCACHE2_CLIENT_EXAMPLE_BRPC_THRIFT_COMPAT_SOURCES})
target_sources(proxy_ingestion_pressure_example PRIVATE ${BCACHE2_CLIENT_EXAMPLE_BRPC_THRIFT_COMPAT_SOURCES})
if(BCACHE2_CLIENT_EXAMPLE_BRPC_THRIFT_PROTOCOL_SOURCE)
    target_compile_definitions(proxy_smoke_example PRIVATE BCACHE2_PROXY_HAS_BRPC_THRIFT_PROTOCOL_SOURCE)
    target_compile_definitions(proxy_ingestion_pressure_example PRIVATE BCACHE2_PROXY_HAS_BRPC_THRIFT_PROTOCOL_SOURCE)
endif()
target_link_libraries(proxy_smoke_example ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS} bcache2_thrift ${BCACHE2_CLIENT_EXAMPLE_THRIFT_LIB})
target_link_libraries(proxy_ingestion_pressure_example ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS} bcache2_thrift ${BCACHE2_CLIENT_EXAMPLE_THRIFT_LIB})
target_link_libraries(queue_ingestion_replay_example ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS})
set_target_properties(customer_c_client_example PROPERTIES LINKER_LANGUAGE CXX)
