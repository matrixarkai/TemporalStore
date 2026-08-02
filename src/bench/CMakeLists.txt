file(GLOB HDRS "*.h")
file(GLOB SRCS "*.cc")
list(REMOVE_ITEM SRCS "${CMAKE_CURRENT_SOURCE_DIR}/main.cc")

if (BCACHE2_BUILD_BENCHMARKS)
    add_library(benchlib ${HDRS} ${SRCS})
else()
    add_library(benchlib EXCLUDE_FROM_ALL ${HDRS} ${SRCS})
endif()
target_link_libraries(benchlib
    bench_workloads
    bench_client
    bench_model)

if (BCACHE2_BUILD_BENCHMARKS)
    add_executable(bcache2-bench main.cc)
else()
    add_executable(bcache2-bench EXCLUDE_FROM_ALL main.cc)
endif()
set(BCACHE2_BENCH_LINK_LIBS benchlib)
if (TARGET tcmalloc-static)
    list(APPEND BCACHE2_BENCH_LINK_LIBS tcmalloc-static)
endif()
target_link_libraries(bcache2-bench ${BCACHE2_BENCH_LINK_LIBS})

add_subdirectory(workloads)
if (BCACHE2_BUILD_TESTS)
    add_subdirectory(test)
endif()
add_subdirectory(client)
add_subdirectory(model)
