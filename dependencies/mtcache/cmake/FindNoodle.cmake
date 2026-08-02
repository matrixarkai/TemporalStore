# This will define
# NOODLE_FOUND
# NOODLE_INCLUDE_DIR
# NOODLE_LIBRARY
#

find_path(NOODLE_INCLUDE_DIR NAMES noodle/base/application.h)

find_library(NOODLE_LIBRARY NAMES libnoodle.a)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(
    NOODLE DEFAULT_MSG
    NOODLE_LIBRARY NOODLE_INCLUDE_DIR
)

if (NOODLE_FOUND)
    message(STATUS "Found NOODLE: ${NOODLE_LIBRARY}")
endif()

mark_as_advanced(NOODLE_INCLUDE_DIR NOODLE_LIBRARY)
