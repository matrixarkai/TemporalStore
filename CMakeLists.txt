cmake_minimum_required (VERSION 3.18 FATAL_ERROR)
project(bcache2 LANGUAGES C CXX)

include(TestBigEndian)
TEST_BIG_ENDIAN(IS_BIG_ENDIAN)
if(IS_BIG_ENDIAN)
    message(FATAL_ERROR "We haven't adapted to big endian. e.g. bcache2::partition::ConditionInfo only works on little endian")
endif()

# Options
option(ENABLE_FIU "enable fiu" OFF)
option(ENABLE_ASAN "enable asan" OFF)
option(ENABLE_COVERAGE "enable asan" OFF)
option(ENABLE_MTCACHE "enable mtcache" OFF)
option(ENABLE_MTCACHE_SSD_CACHE "enable MtCache SSD/TerarkDB backend" OFF)
option(BCACHE2_BUILD_TESTS "build bcache2 unit and smoke tests" OFF)
option(TEMPORALSTORE_RELEASE_DEBUG_INFO "build Release binaries with debug symbols" OFF)

# Vars
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(BCACHE2_OUTPUT_DIR "${PROJECT_SOURCE_DIR}/output" CACHE PATH "Directory for bcache2 runtime and SDK artifacts")

# Import CMake modules
list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
include(AddCXXCompilerFlag)
include(AddCXXLinkerFlag)

# compiler flags that are common across debug/release builds
add_required_cxx_compiler_flag(-std=c++17 COMMON)
# -Wall: Enable all warnings.
add_required_cxx_compiler_flag(-Wall COMMON)
# -Werror: treat all compiler warnings as errors
add_required_cxx_compiler_flag(-Werror COMMON)
if(TEMPORALSTORE_USE_ROOT_CMAKE_GLUE)
    add_cxx_compiler_flag(-Wno-error COMMON)
endif()
if (CYGWIN)
    add_cxx_compiler_flag(-Wno-error COMMON)
endif()
# -lpthread: enable multithreaded malloc
add_required_cxx_compiler_flag(-lpthread COMMON)
# -lm: C math library
add_required_cxx_compiler_flag(-lm COMMON)
# -lunwind: Enable libunwind for unwinding frames
# libunwind is required by several prebuilt dependencies
add_required_cxx_compiler_flag(-lunwind COMMON)
# -fno-strict-aliasing: disable optimizations that assume strict aliasing.
add_cxx_compiler_flag(-fno-strict-aliasing COMMON)
# -fno-omit-frame-pointers: Keep frame pointer for functions in register
add_cxx_compiler_flag(-fno-omit-frame-pointer COMMON)
# -fPIC: Position Independent Code
add_cxx_compiler_flag(-fPIC COMMON)
# -faligned-new: enable C++17 over-aligned new support
# since gcc 7.1
add_cxx_compiler_flag(-faligned-new COMMON)
# -Wno-unused-function: suppress warnings for unused functions
add_cxx_compiler_flag(-Wno-unused-function COMMON)
add_cxx_compiler_flag(-Wno-error=deprecated-declarations COMMON)
# -Wno-format-truncaton: suppress warnings about calls to formatted input/output
#   functions such as snprintf and vsnprintf that might result in output truncation.
#   We do use snprintf in partition_bench.
# since gcc 7.1
add_cxx_compiler_flag(-Wno-format-truncation)
add_cxx_compiler_flag(-Wno-class-memaccess COMMON)

# define __STDC_FORMAT_MACROS to make visible the declarations of format macros
# such as PRIdMAX in inttypes.h
add_cxx_compiler_flag(-D__STDC_FORMAT_MACROS COMMON)

# For CMAKE_BUILD_TYPE=Debug
#  -ggdb3: Enable gdb debugging
#  -O0: Disable compiler optimizations
add_required_cxx_compiler_flag(-ggdb3 DEBUG)
add_required_cxx_compiler_flag(-O0 DEBUG)

# For CMAKE_BUILD_TYPE=Release
#  -O3: Enable all compiler optimizations
add_required_cxx_compiler_flag(-O3 RELEASE)
if (TEMPORALSTORE_RELEASE_DEBUG_INFO)
    add_required_cxx_compiler_flag(-g RELEASE)
endif()

# Set build version
EXECUTE_PROCESS(COMMAND git log -n 1 --format=%h
    TIMEOUT 5
    OUTPUT_VARIABLE COMMIT_ID
    OUTPUT_STRIP_TRAILING_WHITESPACE
    )
IF(CUSTOM_SUFFIX)
    MESSAGE(STATUS "Use environment CUSTOM_SUFFIX as version")
    add_compile_definitions(BCACHE2_VERSION="${CUSTOM_SUFFIX}")
ELSE()
    MESSAGE(STATUS "Use git hash as version: ${COMMIT_ID}")
    add_compile_definitions(BCACHE2_VERSION="${COMMIT_ID}")
ENDIF()

# common linker flags
if (NOT CYGWIN)
    add_required_cxx_linker_flag(-static-libstdc++)
    add_required_cxx_linker_flag(-static-libgcc)
endif()
add_required_cxx_linker_flag(-pthread)
# link with the dynamic linking library, e.g., dlopen, etc.
add_cxx_linker_flag(-ldl)
# link with the rt library, which includes clock_gettime, fork, nanosleep, etc.
add_cxx_linker_flag(-lrt)
# not to make a position independent executable (PIE)
add_cxx_linker_flag(-no-pie)

# Set compile flags based on the build type.
if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_COMMON} ${CMAKE_CXX_FLAGS_DEBUG}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_COMMON} ${CMAKE_CXX_FLAGS_RELEASE}")
else()
    message(STATUS "Unknown build type: ${CMAKE_BUILD_TYPE}, set to Release")
    set(CMAKE_BUILD_TYPE "Release")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_COMMON} ${CMAKE_CXX_FLAGS_RELEASE}")
endif()

set(GCC_VERSION "${CMAKE_CXX_COMPILER_VERSION}" CACHE STRING "gcc version")

if (BCACHE2_BUILD_TESTS)
    enable_testing()
    include(GoogleTest)
endif()

if (ENABLE_FIU)
    message(STATUS "Enable fiu")
    add_definitions(-DFIU_ENABLE)
endif()

# Add flags for code coverage
if (ENABLE_COVERAGE)
    message(STATUS "Use gcov")
    add_required_cxx_compiler_flag(--coverage)
    add_required_cxx_linker_flag(--coverage)
endif()

if (ENABLE_ASAN)
    message(STATUS "Enable ASAN")
    # if you encounter compiler feature check failures such as
    # `HAVE_STD_REGEX - compiled but failed to run' in docker
    # environment, please make sure to run docker with
    # `--cap-add SYS_PTRACE`.
    # See here: https://github.com/google/sanitizers/issues/764
    # and https://github.com/google/oss-fuzz/issues/9
    add_required_cxx_compiler_flag(-fsanitize=address,undefined)
    add_required_cxx_linker_flag(-fsanitize=address,undefined)
    # Cannot use vptr sanitizer when there are hidden symbols.
    # See: https://stackoverflow.com/questions/37358573/why-does-fsanitize-undefined-cause-undefined-reference-to-typeinfo
    add_required_cxx_compiler_flag(-fno-sanitize=vptr)
    add_required_cxx_linker_flag(-fno-sanitize=vptr)
endif()

add_custom_target(cpplint
    COMMAND ${PROJECT_SOURCE_DIR}/tools/cpplint.py
    --linelength=100
    --recursive
    --filter=-build/c++11,-readability/fn_size
    --exclude=${PROJECT_SOURCE_DIR}/src/model/performance/benchmark_example.cc   # https://github.com/google/benchmark/issues/394
    ${PROJECT_SOURCE_DIR}/src)

message(STATUS "Compiler Version: ${CMAKE_CXX_COMPILER_ID}-${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "Compiler Flags: ${CMAKE_CXX_FLAGS}")
message(STATUS "Linker Flags: ${CMAKE_EXE_LINKER_FLAGS}")

if(BCACHE2_PROTOBUF_ROOT_DIR)
    include_directories("${BCACHE2_PROTOBUF_ROOT_DIR}/include")
endif()
if(MATRIXOBJECTSTORE_COMPAT_INCLUDE_DIR AND EXISTS "${MATRIXOBJECTSTORE_COMPAT_INCLUDE_DIR}/matrixobjectstore/matrixobjectstore.h")
    include_directories("${MATRIXOBJECTSTORE_COMPAT_INCLUDE_DIR}")
endif()
if(TEMPORALSTORE_USE_ROOT_CMAKE_GLUE)
    include_directories("${PROJECT_SOURCE_DIR}/thirdparty/byteraft/third")
    include_directories("${PROJECT_SOURCE_DIR}/.local/deps-src")
endif()

add_subdirectory(thirdparty)
if(TEMPORALSTORE_USE_ROOT_CMAKE_GLUE AND EXISTS "/root/src/cmake-glue/compat-targets.cmake")
    include(/root/src/cmake-glue/compat-targets.cmake)
endif()
if(TEMPORALSTORE_USE_ROOT_CMAKE_GLUE AND EXISTS "${PROJECT_SOURCE_DIR}/.local/link-shims")
    link_directories("${PROJECT_SOURCE_DIR}/.local/link-shims")
endif()
if(BRPC_STATIC_LIBRARY AND NOT TARGET brpc-static)
    add_library(brpc-static UNKNOWN IMPORTED GLOBAL)
    set(_TEMPORALSTORE_BRPC_INCLUDE_DIRS "")
    foreach(_TEMPORALSTORE_BRPC_INCLUDE_CANDIDATE
            "/root/src/open-source/brpc/build-config-only/output/include"
            "/root/src/open-source/brpc/src")
        if(EXISTS "${_TEMPORALSTORE_BRPC_INCLUDE_CANDIDATE}/brpc/channel.h")
            list(APPEND _TEMPORALSTORE_BRPC_INCLUDE_DIRS "${_TEMPORALSTORE_BRPC_INCLUDE_CANDIDATE}")
        endif()
    endforeach()
    set_target_properties(brpc-static PROPERTIES IMPORTED_LOCATION "${BRPC_STATIC_LIBRARY}")
    if(_TEMPORALSTORE_BRPC_INCLUDE_DIRS)
        set_target_properties(brpc-static PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${_TEMPORALSTORE_BRPC_INCLUDE_DIRS}")
    endif()
endif()
foreach(_TEMPORALSTORE_BRPC_INCLUDE_CANDIDATE
        "/root/src/open-source/brpc/build-config-only/output/include"
        "/root/src/open-source/brpc/src")
    if(EXISTS "${_TEMPORALSTORE_BRPC_INCLUDE_CANDIDATE}/brpc/channel.h")
        include_directories("${_TEMPORALSTORE_BRPC_INCLUDE_CANDIDATE}")
    endif()
endforeach()
add_subdirectory(src)
if (BCACHE2_BUILD_TESTS)
    add_subdirectory(test)
endif()

# Set bcache2 output directory
file(MAKE_DIRECTORY "${BCACHE2_OUTPUT_DIR}")
file(COPY "${PROJECT_SOURCE_DIR}/test/onebox" DESTINATION "${BCACHE2_OUTPUT_DIR}/")
file(COPY "${PROJECT_SOURCE_DIR}/test/run_onebox.sh" DESTINATION "${BCACHE2_OUTPUT_DIR}/")
file(COPY "${PROJECT_SOURCE_DIR}/test/start_chaos.sh" DESTINATION "${BCACHE2_OUTPUT_DIR}/")
foreach(output_target bcache2-server bcache2-bench bcache2-proxy bcache2-metaserver bcache2-metaserver-fe stream_tool)
    set_target_properties(${output_target} PROPERTIES
                          RUNTIME_OUTPUT_DIRECTORY "${BCACHE2_OUTPUT_DIR}"
                          LINK_FLAGS "-Wl,-rpath,./lib")
endforeach()
file(WRITE "${BCACHE2_OUTPUT_DIR}/COMMIT_ID" "${COMMIT_ID}")

# Set third output
foreach(third_target thrift-compiler protoc)
    set_target_properties(${third_target} PROPERTIES
                          RUNTIME_OUTPUT_DIRECTORY "${BCACHE2_OUTPUT_DIR}/third")
endforeach()

# Set sdk output
file(COPY "${PROJECT_SOURCE_DIR}/src/client/bcache2.h" DESTINATION "${BCACHE2_OUTPUT_DIR}/sdk/include/bcache2")
set_target_properties(bcache2-shared PROPERTIES LIBRARY_OUTPUT_DIRECTORY "${BCACHE2_OUTPUT_DIR}/sdk/lib")

# Note that the packing operation needs to be placed at the end
include(library_bundle.cmake)
bundle_static_library(client bcache2)
