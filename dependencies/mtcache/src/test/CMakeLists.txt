macro(mtcache_add_test)
  cmake_parse_arguments(
    mtcache_test                # prefix
    "DISABLED"                  # <options>
    "NAME"                      # <one_value_args>
    "SOURCES;OBJECTS;LIBRARIES" # <multi_value_args>
    ${ARGN}
  )

  mtcache_add_executable(
    NAME ${mtcache_test_NAME}
    SOURCES ${mtcache_test_SOURCES}
    OBJECTS ${mtcache_test_OBJECTS}
    LIBRARIES
      ${mtcache_test_LIBRARIES}
      mtcache_shared
      ${GTEST_BOTH_LIBRARIES}
  )

  if (NOT ${mtcache_test_DISABLED})
    add_test(
      NAME ${mtcache_test_NAME}
      COMMAND ${mtcache_test_NAME} --tryfromenv=ssd_engine_type --undefok=ssd_engine_type
      )
    message(STATUS "ADD ${mtcache_test_NAME}")

    if(ENABLE_ASAN)
      set_tests_properties(${mtcache_test_NAME} PROPERTIES ENVIRONMENT "ASAN_OPTIONS=detect_container_overflow=true")
    endif()
  endif()
endmacro()

add_subdirectory(allocator)
add_subdirectory(policy)
add_subdirectory(storage)
add_subdirectory(util)

set(SINGLE_FILE_UTS
  simple_lru_cache_test
  cache_buffer_test
  cache_error_test
  cache_instance_test
  unified_cache_test
  unified_cache_ssd_only_test
  cache_map_test
  thread_pool_executor_test
)

foreach (SINGLE_FILE_UT ${SINGLE_FILE_UTS})
  mtcache_add_test(
    NAME ${SINGLE_FILE_UT}
    SOURCES ${SINGLE_FILE_UT}.cpp
  )
endforeach (SINGLE_FILE_UT)

