find_path(TERARKDB_INCLUDE_DIR NAMES rocksdb/db.h
  HINTS ${CMAKE_INCLUDE_PATH}/terarkdb/ ${CMAKE_INCLUDE_PATH}
)

find_library(TERARKDB_LIBRARY NAMES rocksdb terarkdb)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(terarkdb DEFAULT_MSG TERARKDB_LIBRARY TERARKDB_INCLUDE_DIR)

if (TERARKDB_FOUND)
    message(STATUS "Found RocksDB-compatible SSD engine: ${TERARKDB_LIBRARY} ${TERARKDB_INCLUDE_DIR}")
endif()

mark_as_advanced(TERARKDB_LIBRARY TERARKDB_INCLUDE_DIR)
