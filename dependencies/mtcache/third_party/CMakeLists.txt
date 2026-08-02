cmake_minimum_required(VERSION 3.12.0)

project("MTCache Third Party" C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Required CMake modules
include(ExternalProject)

option(ENABLE_ASAN "Whether to turn folly AddressSanitizer ON or OFF" OFF)

# Get number of physical CPU cores and megabytes of available memory
if ("${CMAKE_HOST_SYSTEM_PROCESSOR}" STREQUAL "x86_64")
    cmake_host_system_information(RESULT num_cores QUERY NUMBER_OF_PHYSICAL_CORES)
elseif ("${CMAKE_HOST_SYSTEM_PROCESSOR}" STREQUAL "aarch64")
    cmake_host_system_information(RESULT num_cores QUERY NUMBER_OF_LOGICAL_CORES)
endif()
cmake_host_system_information(RESULT available_memory_mb QUERY AVAILABLE_PHYSICAL_MEMORY)

execute_process(
    COMMAND ldd --version
    COMMAND head -1
    COMMAND cut -d ")" -f 2
    COMMAND cut -d " " -f 2
    OUTPUT_VARIABLE GLIBC_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Guess the number of building jobs based on the available memories
set(jobs_by_cpu ${num_cores})
math(EXPR jobs_by_mem "${available_memory_mb} / 1024 / 2")
if (jobs_by_mem EQUAL 0)
    set(jobs_by_mem 1)
endif()

# Set the number of building jobs to min(jobs_by_mem, jobs_by_cpu),
# iff BUILDING_JOBS_NUM has not been set or set to 0.
if (NOT BUILDING_JOBS_NUM OR BUILDING_JOBS_NUM EQUAL 0)
    set(BUILDING_JOBS_NUM ${jobs_by_cpu})
    if (BUILDING_JOBS_NUM GREATER jobs_by_mem)
        set(BUILDING_JOBS_NUM ${jobs_by_mem})
    endif()
endif()

message(STATUS "Number of online physcial CPU cores: ${num_cores}")
message(STATUS "Available physical memory: ${available_memory_mb} MB")
message(STATUS "Building third party with ${BUILDING_JOBS_NUM} jobs")
message(STATUS "Glibc version: ${GLIBC_VERSION}")
message(STATUS "ENABLE_ASAN: ${ENABLE_ASAN}")

if (NOT DOWNLOAD_DIR)
    set(DOWNLOAD_DIR ${CMAKE_SOURCE_DIR}/downloads)
endif()

if("${CMAKE_INSTALL_PREFIX}" STREQUAL "/usr/local")
    if (ENABLE_ASAN)
        SET(CMAKE_INSTALL_PREFIX ${CMAKE_SOURCE_DIR}/install_asan)
    else()
        SET(CMAKE_INSTALL_PREFIX ${CMAKE_SOURCE_DIR}/install)
    endif()
endif()

set(BUILD_INFO_DIR ${CMAKE_CURRENT_BINARY_DIR}/build-info)
set(BUILDING_PATH "${CMAKE_INSTALL_PREFIX}/bin:${CMAKE_INSTALL_PREFIX}/sbin:$ENV{PATH}")
set(ACLOCAL_PATH "${CMAKE_INSTALL_PREFIX}/share/aclocal:${CMAKE_INSTALL_PREFIX}/share/aclocal-1.15")

if (${DISABLE_CXX11_ABI})
    set(extra_cpp_flags "-D_GLIBCXX_USE_CXX11_ABI=0")
else()
    set(extra_cpp_flags "-D_GLIBCXX_USE_CXX11_ABI=1")
endif()

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  if("${CLANG_HOME}" STREQUAL "")
    if(DEFINED ENV{CLANG_HOME})
      message(STATUS "Use env clang_home $ENV{CLANG_HOME}")
      set(CLANG_HOME "$ENV{CLANG_HOME}")
    endif()
    message(STATUS "Use env clang_home $ENV{CLANG_HOME}")
    if("${CLANG_HOME}" STREQUAL "")
      message(FATAL_ERROR "CLANG_HOME should be specified!")
    endif()
  endif()
  message(STATUS "CLANG_HOME ${CLANG_HOME}")
  set(extra_cpp_flags "${extra_cpp_flags} -nostdinc++ -nostdlib++ -isystem ${CLANG_HOME}/include/c++/v1 -L${CLANG_HOME}/lib -Wl,-rpath,${CLANG_HOME}/lib -Wno-unused-command-line-argument")
  set(extra_link_libs "-fuse-ld=lld -pthread -ldl ${CLANG_HOME}/lib/libc++.a ${CLANG_HOME}/lib/libc++abi.a ")
  set(FBTHRIFT_LINK_LIBS "c++")
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 10.0)
    message(FATAL_ERROR "GCC>=10.0 required!")
  endif()
  set(extra_link_libs "-static-libstdc++ -static-libgcc -pthread -ldl")
endif()

if(GLIBC_VERSION VERSION_LESS 2.17)
    set(extra_link_libs "${extra_link_libs} -lrt")
endif()

# Auto detect CPU instruction set which we can use.
include(../cmake/cpu_features.cmake)
message(STATUS "Cpu Options: ${COMPILER_FLAGS}")

# Add flags that are common across build types
SET(extra_cpp_flags "${extra_cpp_flags} ${COMPILER_FLAGS}")

message(STATUS "CMAKE_CXX_COMPILER: ${CMAKE_CXX_COMPILER}")
message(STATUS "CMAKE_SYSTEM_PREFIX_PATH : ${CMAKE_SYSTEM_PREFIX_PATH}")
message(STATUS "CMAKE_INSTALL_PREFIX: ${CMAKE_INSTALL_PREFIX}")
message(STATUS "extra_cpp_flags: ${extra_cpp_flags}")
message(STATUS "extra_link_libs: ${extra_link_libs}")

set(common_cmake_args
    -DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}
    "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
    "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
    "-DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -fPIC ${extra_cpp_flags}"
    "-DCMAKE_C_FLAGS=${CMAKE_C_FLAGS} -fno-omit-frame-pointer -fPIC"
    "-DCMAKE_EXE_LINKER_FLAGS=${extra_link_libs}"
    "-DCMAKE_SHARED_LINKER_FLAGS=${extra_link_libs}"
    -DCMAKE_INCLUDE_PATH=${CMAKE_INSTALL_PREFIX}/include
    -DCMAKE_LIBRARY_PATH=${CMAKE_INSTALL_PREFIX}/lib
    -DCMAKE_BUILD_TYPE=Release
)

set(common_configure_args
    --prefix=${CMAKE_INSTALL_PREFIX}
)

set(common_configure_envs
    "env"
    "CC=${CMAKE_C_COMPILER}"
    "CXX=${CMAKE_CXX_COMPILER}"
    "CFLAGS=${CMAKE_C_FLAGS} -fcommon -fno-omit-frame-pointer -fPIC -O2 -D_DEFAULT_SOURCE -D_GNU_SOURCE ${extra_cpp_flags}"
    "CXXFLAGS=${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -fPIC -O2 -D_DEFAULT_SOURCE -D_GNU_SOURCE ${extra_cpp_flags}"
    "CPPFLAGS=-isystem ${CMAKE_INSTALL_PREFIX}/include ${extra_cpp_flags}"
    "LDFLAGS=-L${CMAKE_INSTALL_PREFIX}/lib -L${CMAKE_INSTALL_PREFIX}/lib64 ${extra_link_libs}"
    "PATH=${BUILDING_PATH}"
    "ACLOCAL_PATH=${ACLOCAL_PATH}"
)

set(ALL_TARGETS
    libunwind-1.2.1
    openssl-1.1.1m
    libevent-2.1.12
    jemalloc-5.2.1
    bzip2-1.0.6
    zstd-1.4.9
    zlib-1.2.11
    snappy-1.1.7
    lzma-5.2.4
    lz4-1.9.2
    libaio-0.3.110-1
    double-conversion-1.1.6
    gflags-2.2.2
    googletest-1.11.0
    glog-0.3.5
    boost_1_75_0
    libsodium-1.0.18
    fmt-7.1.3
    folly-2021.03.08.00
    abseil-cpp-20211102.0
    noodle-v20210325
    xxHash-0.6.5
    pkgconf
    libmemcached-1.1.0
)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/cmake)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/../cmake)

find_package(Libtool 2.4.2 QUIET)
if (NOT Libtool_FOUND)
    list(APPEND ALL_TARGETS libtool-2.4.6)
endif()

find_package(Autoconf 2.69 QUIET)
if (NOT Autoconf_FOUND)
    list(APPEND ALL_TARGETS autoconf-2.69)
endif()

find_package(AutoconfArchive QUIET)
if (NOT AutoconfArchive_FOUND)
    list(APPEND ALL_TARGETS autoconf-archive-2019.01.06)
endif()

find_package(Automake 1.13.4 QUIET)
if (NOT Automake_FOUND)
    list(APPEND ALL_TARGETS automake-1.15.1)
endif()

find_package(BISON 3.0.4 QUIET)
if (NOT BISON_FOUND)
    list(APPEND ALL_TARGETS bison-3.0.5)
endif()

find_package(FLEX 2.6.0 QUIET)
if (NOT FLEX_FOUND)
    list(APPEND ALL_TARGETS flex-2.6.4)
endif()

find_package(Gettext QUIET)
if (NOT Gettext_FOUND)
    list(APPEND ALL_TARGETS gettext-0.19.8.1)
endif()

find_program(GPERF gperf)
if (GPERF-NOTFOUND)
    list(APPEND ALL_TARGETS gperf-3.1)
endif()

find_package(Python3 REQUIRED)
if (NOT Python3_FOUND)
    message(FATAL "Miss the python3")
endif()

find_package(Numa REQUIRED)
if (NOT NUMA_FOUND)
   message(FATA_ERROR "Please install libnuma_dev")
endif()

find_package(Uuid REQUIRED)
if (NOT UUID_FOUND)
  message(FATA_ERROR "Please install uuid_dev")
endif()

foreach(target ${ALL_TARGETS})
    include(externals/${target}.cmake)
    list(APPEND CLEAN_TARGETS ${target}-clean)
    if (TARGET ${target})
        message(STATUS "ADD target ${target}")
    else()
        message(FATAL "MISS target ${target}")
    endif()
endforeach()

macro(maybe_add_dependencies depender)
    if (TARGET ${depender})
        foreach (dependee ${ARGN})
            if (TARGET ${dependee})
                add_dependencies(${depender} ${dependee})
            endif()
        endforeach()
    endif()
endmacro()

maybe_add_dependencies(autoconf-archive-2019.01.06 autoconf-2.69)
maybe_add_dependencies(automake-1.15.1 autoconf-2.69 autoconf-archive-2019.01.06)
maybe_add_dependencies(libtool-2.4.6 automake-1.15.1)
maybe_add_dependencies(libsodium-1.0.18 autoconf-2.69)
maybe_add_dependencies(bison-3.0.5 libtool-2.4.6)
maybe_add_dependencies(flex-2.6.4 libtool-2.4.6)
maybe_add_dependencies(zlib-1.2.11 libtool-2.4.6)
maybe_add_dependencies(gettext-0.19.8.1 libtool-2.4.6)
maybe_add_dependencies(lzma-5.2.4 gettext-0.19.8.1)
maybe_add_dependencies(libevent-2.1.12 libtool-2.4.6 openssl-1.1.1m)
maybe_add_dependencies(libmemcached-1.1.0 libevent-2.1.12)
maybe_add_dependencies(gperf-3.1 libtool-2.4.6)
maybe_add_dependencies(boost_1_75_0 zlib-1.2.11 bzip2-1.0.6 lzma-5.2.4)
maybe_add_dependencies(glog-0.3.5 gflags-2.2.2 libunwind-1.2.1 libtool-2.4.6)
maybe_add_dependencies(googletest-1.11.0 glog-0.3.5)
maybe_add_dependencies(fmt-7.1.3 libsodium-1.0.18 autoconf-archive-2019.01.06)
maybe_add_dependencies(folly-2021.03.08.00 glog-0.3.5 boost_1_75_0 double-conversion-1.1.6 openssl-1.1.1m libevent-2.1.12 lzma-5.2.4 zstd-1.4.9 snappy-1.1.7 lz4-1.9.2 libunwind-1.2.1 libaio-0.3.110-1 fmt-7.1.3)
maybe_add_dependencies(noodle-v20210325 glog-0.3.5 zlib-1.2.11 openssl-1.1.1m zstd-1.4.9 snappy-1.1.7 lz4-1.9.2 xxHash-0.6.5)
maybe_add_dependencies(jemalloc-5.2.1 libunwind-1.2.1)

add_custom_command(
    TARGET zstd-1.4.9 POST_BUILD
    COMMAND
        rm -rf ${CMAKE_INSTALL_PREFIX}/lib/libzstd.so*
)

add_custom_target(
    clean-all
    DEPENDS ${CLEAN_TARGETS}
)
