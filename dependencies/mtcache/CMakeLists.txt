cmake_minimum_required(VERSION 3.12.0)

project("MTCache project" C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON) # Write compile_commands.json

# includes
set(CMAKE_MODULE_PATH
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake"
  ${CMAKE_MODULE_PATH})

# package information
set(PACKAGE_NAME      "mtcache")
set(PACKAGE_VERSION   "0.0.1")
set(PACKAGE_STRING    "${PACKAGE_NAME} ${PACKAGE_VERSION}")
set(PACKAGE_TARNAME   "${PACKAGE_NAME}_${PACKAGE_VERSION}")
set(MTCACHE_HOME ${CMAKE_CURRENT_SOURCE_DIR})

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "_build")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "_build")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "_build")

option(ENABLE_ASAN "Whether to turn AddressSanitizer ON or OFF" OFF)
option(ENABLE_CCACHE "Whether to use ccache to speed up compiling" ON)
option(ENABLE_UNWIND "Whether to use libunwind" OFF)
option(MTCACHE_BUILD_TEST "Whether to build UT/benchmark/tools" ON)
option(BUILD_SSD_CACHE "Whether to build the code of ssd cache component" ON)
option(BUILD_ZONED_STORE "Whether to build the Bytedisk-backed ZonedStore SSD engine" OFF)

if (BUILD_ZONED_STORE AND NOT BUILD_SSD_CACHE)
  message(FATAL_ERROR "BUILD_ZONED_STORE requires BUILD_SSD_CACHE=ON")
endif()

if("${MTCACHE_THIRDPARTY_ROOT}" STREQUAL "")
  if (ENABLE_ASAN)
    if(EXISTS ${MTCACHE_HOME}/third_party/install_asan)
      SET(MTCACHE_THIRDPARTY_ROOT ${MTCACHE_HOME}/third_party/install_asan)
      message(STATUS "MTCACHE_THIRDPARTY_ROOT not set, using default path=${MTCACHE_HOME}/third_party/install_asan")
    else()
      message(FATAL_ERROR "MTCACHE_THIRDPARTY_ROOT not set and default path=${MTCACHE_HOME}/third_party/install_asan not found")
    endif()
  else()
    if(EXISTS ${MTCACHE_HOME}/third_party/install)
      SET(MTCACHE_THIRDPARTY_ROOT ${MTCACHE_HOME}/third_party/install)
      message(STATUS "MTCACHE_THIRDPARTY_ROOT not set, using default path=${MTCACHE_HOME}/third_party/install")
    else()
      message(FATAL_ERROR "MTCACHE_THIRDPARTY_ROOT not set and default path=${MTCACHE_HOME}/third_party/install not found")
    endif()
  endif()
endif()

message(STATUS "Specified MTCACHE_THIRDPARTY_ROOT: " ${MTCACHE_THIRDPARTY_ROOT})
list(INSERT CMAKE_INCLUDE_PATH 0 ${MTCACHE_THIRDPARTY_ROOT}/include)
list(INSERT CMAKE_LIBRARY_PATH 0 ${MTCACHE_THIRDPARTY_ROOT}/lib)
list(INSERT CMAKE_PROGRAM_PATH 0 ${MTCACHE_THIRDPARTY_ROOT}/bin)

set(CMAKE_PREFIX_PATH ${MTCACHE_THIRDPARTY_ROOT})
set(Boost_NO_BOOST_CMAKE ON)

message(STATUS "CMAKE_CURRENT_SOURCE_DIR: " ${CMAKE_CURRENT_SOURCE_DIR})
message(STATUS "CMAKE_INCLUDE_PATH: " ${CMAKE_INCLUDE_PATH})
message(STATUS "CMAKE_LIBRARY_PATH: " ${CMAKE_LIBRARY_PATH})
message(STATUS "CMAKE_PROGRAM_PATH: " ${CMAKE_PROGRAM_PATH})
message(STATUS "CMAKE_MODULE_PATH: " ${CMAKE_MODULE_PATH})

if(NOT DEFINED CMAKE_SUPPRESS_DEVELOPER_WARNINGS)
  set(CMAKE_SUPPRESS_DEVELOPER_WARNINGS 1 CACHE INTERNAL "No dev warnings")
endif()

find_package(Gflags REQUIRED)
find_package(Glog REQUIRED)
find_package(GTest REQUIRED)
find_package(GMock REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)
find_package(Noodle REQUIRED)
find_package(Boost REQUIRED)
find_package(Uuid REQUIRED)
find_package(XxHash REQUIRED)
find_package(LibMemcached REQUIRED)
find_package(Jemalloc REQUIRED)
find_package(folly CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)

function(mtcache_filter_bundled_openssl target_name)
  if (TARGET ${target_name})
    get_target_property(target_link_libs ${target_name} INTERFACE_LINK_LIBRARIES)
    if (target_link_libs)
      set(filtered_link_libs)
      foreach(link_lib IN LISTS target_link_libs)
        if (link_lib MATCHES ".*/lib(ssl|crypto)\\.a$")
          continue()
        endif()
        list(APPEND filtered_link_libs "${link_lib}")
      endforeach()
      set_target_properties(${target_name} PROPERTIES
        INTERFACE_LINK_LIBRARIES "${filtered_link_libs}")
    endif()
  endif()
endfunction()

mtcache_filter_bundled_openssl(Folly::folly)
mtcache_filter_bundled_openssl(Folly::folly_deps)

if (BUILD_SSD_CACHE)
  find_package(TerarkDB REQUIRED)
endif()
if (BUILD_ZONED_STORE)
  find_package(Bytedisk REQUIRED)
endif()

set(Boost_USE_RELEASE_LIBS ON)
set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_STATIC_RUNTIME ON)
set(Boost_USE_DEBUG_RUNTIME OFF)
set(Protobuf_USE_STATIC_LIBS ON)
find_package(boost_iostreams CONFIG REQUIRED)
if (NOT TARGET absl::time OR NOT TARGET absl::synchronization)
  find_package(absl CONFIG REQUIRED)
endif()

include(CheckCXXCompilerFlag)

# To detect if ccache is available
find_program(ccache_program_found "ccache")
if (ENABLE_CCACHE AND ccache_program_found)
    message(STATUS "ENABLE_CCACHE: ON")
    if (NOT $ENV{CCACHE_DIR} STREQUAL "")
        message(STATUS "CCACHE_DIR: $ENV{CCACHE_DIR}")
    else()
        message(STATUS "CCACHE_DIR: $ENV{HOME}/.ccache")
    endif()
    set(CMAKE_CXX_COMPILER_LAUNCHER "ccache")
elseif (ENABLE_CCACHE)
    message(STATUS "CCACHE: enabled but not found")
    set(CMAKE_CXX_COMPILER_LAUNCHER)
else ()
    message(STATUS "CCACHE: OFF")
    set(CMAKE_CXX_COMPILER_LAUNCHER)
endif()

execute_process(COMMAND bash "-c" "uname -m"
  OUTPUT_VARIABLE
  CMAKE_BUILD_TARGET_ARCH
  OUTPUT_STRIP_TRAILING_WHITESPACE)
message(STATUS "Build target arch is ${CMAKE_BUILD_TARGET_ARCH}")

include(GitUtils)
set(GIT_HASH "")
get_git_hash(GIT_HASH)
message(STATUS "Git hash is ${GIT_HASH}")

set(GIT_BRANCH "")
get_git_branch(GIT_BRANCH)
message(STATUS "Git branch is ${GIT_BRANCH}")

set(BUILD_VERSION $ENV{BUILD_VERSION})

set(CXX_COMMON_FLAGS "-Wall -fPIC -fno-omit-frame-pointer")
set(COMMON_LINKER_FLAGS "-no-pie -ldl -pthread")

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  if("${CLANG_HOME}" STREQUAL "")
    if(DEFINED ENV{CLANG_HOME})
      message(STATUS "Use env clang_home $ENV{CLANG_HOME}")
      set(CLANG_HOME "$ENV{CLANG_HOME}")
    endif()
    if("${CLANG_HOME}" STREQUAL "")
      message(FATAL_ERROR "CLANG_HOME should be specified!")
    endif()
  endif()
  message(STATUS "CLANG_HOME ${CLANG_HOME}")
  set(COMMON_LINKER_FLAGS "${COMMON_LINKER_FLAGS} -fuse-ld=lld ${CLANG_HOME}/lib/libc++.a ${CLANG_HOME}/lib/libc++abi.a -L${CLANG_HOME}/lib -Wl,-rpath ${CLANG_HOME}/lib -Wno-unused-command-line-argument")
  set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -nostdinc++ -nostdlib++ -isystem ${CLANG_HOME}/include/c++/v1")
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 10.0)
    message(FATAL_ERROR "GCC>=10.0 required!")
  endif()
  set(COMMON_LINKER_FLAGS "${COMMON_LINKER_FLAGS} -static-libstdc++ -static-libgcc")
endif()

if(ENABLE_ASAN)
  set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -fsanitize=address,undefined")
  set(COMMON_LINKER_FLAGS "${COMMON_LINKER_FLAGS} -fsanitize=address,undefined")
endif()

# For CMAKE_BUILD_TYPE=Debug
#   -ggdb: Enable gdb debugging
#   -gdwarf-2: Debug information is stored as dwarf2 to be as compatible as possible
set(CXX_FLAGS_DEBUG "-ggdb -O0 -gdwarf-2")

# For CMAKE_BUILD_TYPE=Release
#   -O3: Enable all compiler optimizations
#   -DNDEBUG: Turn off dchecks/asserts/debug only code.
#   -gdwarf-2: Debug information is stored as dwarf2 to be as compatible as possible
set(CXX_FLAGS_RELEASE "-O3 -gdwarf-2 -DNDEBUG")

# Set compile flags based on the build type.
if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} ${CXX_FLAGS_DEBUG}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "Release" OR "${CMAKE_BUILD_TYPE}" STREQUAL "BCC")
  SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} ${CXX_FLAGS_RELEASE}")
else()
    message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif()

# Auto detect CPU instruction set which we can use.
include(cmake/cpu_features.cmake)
message(STATUS "Cpu Options: ${COMPILER_FLAGS}")

SET(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS} ${COMPILER_FLAGS}")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${COMMON_LINKER_FLAGS}")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${COMMON_LINKER_FLAGS}")

message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
message(STATUS "CMAKE_EXE_LINKER_FLAGS: ${CMAKE_EXE_LINKER_FLAGS}")
message(STATUS "CMAKE_SHARED_LINKER_FLAGS: ${CMAKE_SHARED_LINKER_FLAGS}")

# Thrift requires these two definitions for some types that we use
add_definitions(-DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H)

# Enable counters of runtime profile
add_compile_definitions(ENABLE_COUNTERS=1)

set(OS_LINUX 1)
add_definitions(-D OS_LINUX)

# enable ctest
enable_testing()

add_subdirectory(src)

add_custom_target(
    clean-all
    DEPENDS ${CLEAN_TARGETS}
)
