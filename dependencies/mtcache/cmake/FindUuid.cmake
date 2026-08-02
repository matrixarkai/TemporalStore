# This will define
# UUID_FOUND
# UUID_INCLUDE_DIR
# UUID_LIBRARY

find_path(UUID_INCLUDE_DIR NAMES uuid/uuid.h)

find_library(UUID_LIBRARY NAMES libuuid.so)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(
    UUID DEFAULT_MSG
    UUID_LIBRARY UUID_INCLUDE_DIR
)

if (UUID_FOUND)
    message(STATUS "Found UUID: ${UUID_LIBRARY}")
endif()

mark_as_advanced(UUID_INCLUDE_DIR UUID_LIBRARY)
