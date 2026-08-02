file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
option(BCACHE2_BUILD_BENCHMARKS "Build benchmark-only binaries" OFF)
if (BCACHE2_BUILD_BENCHMARKS)
    add_executable(feature_model_bench benchmark_example.cc)
    target_link_libraries(feature_model_bench
                bcache2_proto
                model
                benchmark)
endif()
