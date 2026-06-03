
# This will define
# NUMA_FOUND
# NUMA_INCLUDE_DIR
# NUMA_LIBRARY
#

find_path(NUMA_INCLUDE_DIR NAMES numa.h)

find_library(NUMA_LIBRARY NAMES libnuma.a)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(
    NUMA DEFAULT_MSG
    NUMA_LIBRARY NUMA_INCLUDE_DIR
)

if (NUMA_FOUND)
    message(STATUS "Found NUMA: ${NUMA_LIBRARY}")
endif()

mark_as_advanced(NUMA_INCLUDE_DIR NUMA_LIBRARY)
