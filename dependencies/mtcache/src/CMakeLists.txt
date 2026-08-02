if("${MTCACHE_THIRDPARTY_ROOT}" STREQUAL "")
  message(FATAL_ERROR "Please set MTCACHE_THIRDPARTY_ROOT")
endif()

if (BUILD_SSD_CACHE)
  add_definitions(-DBUILD_SSD_CACHE)
endif()
if (BUILD_ZONED_STORE)
  add_definitions(-DBUILD_ZONED_STORE)
endif()

set(MTCACHE_SRC_HOME ${CMAKE_CURRENT_SOURCE_DIR})

macro(mtcache_add_executable)
  cmake_parse_arguments(
      mtcache_exec                # prefix
      ""                          # <options>
      "NAME"                      # <one_value_args>
      "SOURCES;OBJECTS;LIBRARIES" # <multi_value_args>
      ${ARGN}
  )

  add_executable(
      ${mtcache_exec_NAME}
      ${mtcache_exec_SOURCES}
      ${mtcache_exec_OBJECTS}
  )

  target_include_directories(
    ${mtcache_exec_NAME}
    PRIVATE ${MTCACHE_SRC_HOME}
    SYSTEM ${MTCACHE_THIRDPARTY_ROOT}/include

  )
  if (BUILD_SSD_CACHE)
    target_include_directories(${mtcache_exec_NAME} PRIVATE ${TERARKDB_INCLUDE_DIR})
  endif()

  target_link_libraries(
      ${mtcache_exec_NAME}
      ${mtcache_exec_LIBRARIES}
      Threads::Threads
  )
endmacro()

macro(mtcache_add_library name type)
    add_library(${name} ${type} ${ARGN})
endmacro()

aux_source_directory(. MTCACHE_SRC)
aux_source_directory(./common MTCACHE_COMMON)
aux_source_directory(./common/thread_pool MTCACHE_THREADPOOL)
aux_source_directory(./buffer MTCACHE_BUFFER)
aux_source_directory(./allocator MTCACHE_ALLOCATOR)
aux_source_directory(./storage MTCACHE_STORAGE)
aux_source_directory(./storage/zoned_store MTCACHE_ZONED_STORAGE)
aux_source_directory(./policy MTCACHE_POLICY)

if (NOT BUILD_SSD_CACHE)
  list(REMOVE_ITEM MTCACHE_STORAGE
    "./storage/multi_ssd.cpp"
    "./storage/ssd_terarkdb.cpp"
    )
endif()

list(APPEND MTCACHE_SRCS
  ${MTCACHE_SRC}
  ${MTCACHE_COMMON}
  ${MTCACHE_THREADPOOL}
  ${MTCACHE_BUFFER}
  ${MTCACHE_LOADING}
  ${MTCACHE_ALLOCATOR}
  ${MTCACHE_STORAGE}
  ${MTCACHE_POLICY}
  )

if (BUILD_ZONED_STORE)
  list(APPEND MTCACHE_SRCS
    ${MTCACHE_ZONED_STORAGE}
    )
endif()

mtcache_add_library(
  mtcache_obj OBJECT
  ${MTCACHE_SRCS}
)

target_include_directories(mtcache_obj
  PRIVATE ${MTCACHE_SRC_HOME}
  SYSTEM ${MTCACHE_THIRDPARTY_ROOT}/include
)
if (BUILD_SSD_CACHE)
  target_include_directories(mtcache_obj PRIVATE ${TERARKDB_INCLUDE_DIR})
endif()

list(APPEND MTCACHE_DEP_LIBS
  absl::time
  absl::synchronization
  numa
  ${Jemalloc_LIBRARY}
  ${NOODLE_LIBRARY}
  ${CURL_LIBRARY}
  ${XXHASH_LIBRARY}
  ${UUID_LIBRARY}
  Folly::folly
  glog::glog
  gflags
  )

if (TARGET Folly::folly_deps)
  get_target_property(FOLLY_DEPS_LINK_LIBS Folly::folly_deps INTERFACE_LINK_LIBRARIES)
  if (FOLLY_DEPS_LINK_LIBS)
    set(FOLLY_DEPS_LINK_LIBS_FILTERED)
    foreach(FOLLY_DEP_LIB IN LISTS FOLLY_DEPS_LINK_LIBS)
      if (FOLLY_DEP_LIB MATCHES ".*/lib(ssl|crypto)\\.a$")
        continue()
      endif()
      list(APPEND FOLLY_DEPS_LINK_LIBS_FILTERED "${FOLLY_DEP_LIB}")
    endforeach()

    set_target_properties(Folly::folly_deps PROPERTIES
      INTERFACE_LINK_LIBRARIES "${FOLLY_DEPS_LINK_LIBS_FILTERED}")
  endif()
endif()

if (BUILD_SSD_CACHE)
  find_library(TERARKDB_SNAPPY_LIBRARY NAMES snappy
    PATHS ${MTCACHE_THIRDPARTY_ROOT}/lib NO_DEFAULT_PATH)
  find_library(TERARKDB_ZLIB_LIBRARY NAMES z
    PATHS ${MTCACHE_THIRDPARTY_ROOT}/lib NO_DEFAULT_PATH)
  find_library(TERARKDB_ZSTD_LIBRARY NAMES zstd
    PATHS ${MTCACHE_THIRDPARTY_ROOT}/lib NO_DEFAULT_PATH)
  find_library(TERARKDB_LZ4_LIBRARY NAMES lz4
    PATHS ${MTCACHE_THIRDPARTY_ROOT}/lib NO_DEFAULT_PATH)
  find_library(TERARKDB_BZ2_LIBRARY NAMES bz2
    PATHS ${MTCACHE_THIRDPARTY_ROOT}/lib NO_DEFAULT_PATH)

  list(INSERT MTCACHE_DEP_LIBS 0
    ${TERARKDB_LIBRARY}
    ${TERARKDB_SNAPPY_LIBRARY}
    ${TERARKDB_ZLIB_LIBRARY}
    ${TERARKDB_ZSTD_LIBRARY}
    ${TERARKDB_LZ4_LIBRARY}
    ${TERARKDB_BZ2_LIBRARY}
    )
endif()
if (BUILD_ZONED_STORE)
  list(INSERT MTCACHE_DEP_LIBS 0
    ${BYTEDISK_LIBRARY}
    )
endif()

target_link_libraries(mtcache_obj
  ${MTCACHE_DEP_LIBS}
)

if (MTCACHE_BUILD_TEST)
  # This shared library is used by UTs and tools.
  mtcache_add_library(mtcache_shared SHARED)

  target_link_libraries(mtcache_shared
    mtcache_obj
  )
endif()

mtcache_add_library(mtcache STATIC)

target_link_libraries(mtcache
  mtcache_obj
)

if (MTCACHE_BUILD_TEST)
  add_subdirectory(tools)
  add_subdirectory(test)
endif()
