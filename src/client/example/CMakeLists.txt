if (CYGWIN)
    add_executable(client_example EXCLUDE_FROM_ALL client_example.cc)
    add_executable(module_ingest_query_example EXCLUDE_FROM_ALL module_ingest_query_example.cc)
    add_executable(module_latency_benchmark EXCLUDE_FROM_ALL module_latency_benchmark.cc)
    add_executable(customer_client_example EXCLUDE_FROM_ALL customer_client_example.cc)
    add_executable(customer_c_client_example EXCLUDE_FROM_ALL customer_c_client_example.c)
    add_executable(replication_smoke_example EXCLUDE_FROM_ALL replication_smoke_example.cc)
    add_executable(string_scale_benchmark EXCLUDE_FROM_ALL string_scale_benchmark.cc)
    add_executable(feature_sequence_benchmark EXCLUDE_FROM_ALL feature_sequence_benchmark.cc)
    add_executable(temporal_aggregate_scale_benchmark EXCLUDE_FROM_ALL temporal_aggregate_scale_benchmark.cc)
    add_executable(proxy_smoke_example EXCLUDE_FROM_ALL proxy_smoke_example.cc)
else()
    add_executable(client_example client_example.cc)
    add_executable(module_ingest_query_example module_ingest_query_example.cc)
    add_executable(module_latency_benchmark module_latency_benchmark.cc)
    add_executable(customer_client_example customer_client_example.cc)
    add_executable(customer_c_client_example customer_c_client_example.c)
    add_executable(replication_smoke_example replication_smoke_example.cc)
    add_executable(string_scale_benchmark string_scale_benchmark.cc)
    add_executable(feature_sequence_benchmark feature_sequence_benchmark.cc)
    add_executable(temporal_aggregate_scale_benchmark temporal_aggregate_scale_benchmark.cc)
    add_executable(proxy_smoke_example proxy_smoke_example.cc)
endif()

set(BCACHE2_CLIENT_EXAMPLE_OPTIONAL_LIBS)
foreach(optional_lib isal-static cryptopp-static)
    if (TARGET ${optional_lib})
        list(APPEND BCACHE2_CLIENT_EXAMPLE_OPTIONAL_LIBS ${optional_lib})
    endif()
endforeach()

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
    absl_str_format
    absl_symbolize
    absl_synchronization
    absl_time
    gflags
    libprotobuf
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
        ${OPENSSL_SSL_LIBRARY}
        ${OPENSSL_CRYPTO_LIBRARY})
endif()

target_link_libraries(client_example ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS})
target_link_libraries(module_ingest_query_example ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS})
target_link_libraries(module_latency_benchmark ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS})
target_link_libraries(customer_client_example ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS})
target_link_libraries(customer_c_client_example ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS})
target_link_libraries(replication_smoke_example ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS})
target_link_libraries(string_scale_benchmark ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS})
target_link_libraries(feature_sequence_benchmark ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS})
target_link_libraries(temporal_aggregate_scale_benchmark ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS})
target_link_libraries(proxy_smoke_example ${BCACHE2_CLIENT_EXAMPLE_LINK_LIBS} bcache2_thrift thrift_static)
set_target_properties(customer_c_client_example PROPERTIES LINKER_LANGUAGE CXX)
