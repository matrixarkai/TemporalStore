if (BUILD_SSD_CACHE)
  mtcache_add_executable(
    NAME cache_bench
    SOURCES
      cache_bench.cpp
      cache_wrapper.cpp
    OBJECTS
    LIBRARIES
      mtcache_shared
      ${LIBMEMCACHED_LIBRARY}
    )

  mtcache_add_executable(
    NAME zonedstore_bench
    SOURCES
      zonedstore_bench.cpp
    OBJECTS
    LIBRARIES
      mtcache_shared
    )
endif()

mtcache_add_executable(
  NAME pmem_async_write_bench
  SOURCES
    pmem_async_write_bench.cpp
  LIBRARIES
    mtcache_shared
    Folly::follybenchmark
)

mtcache_add_executable(
  NAME pmem_recover_bench
  SOURCES
    pmem_recover_bench.cpp
  LIBRARIES
    mtcache_shared
    Folly::follybenchmark
)