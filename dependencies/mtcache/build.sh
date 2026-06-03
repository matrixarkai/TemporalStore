#!/bin/bash

set -eo pipefail
unset BUILD_TYPE

CUR_DIR=`dirname "$0"`
MTCACHE_HOME=`cd "$CUR_DIR"; pwd`
MTCACHE_THIRD_PARTY_HOME=${MTCACHE_HOME}/third_party
MTCACHE_THIRD_PARTY_ROOT=
CMAKE_BUILD_DIR=${MTCACHE_HOME}/build

usage() {
  echo "
Usage: $0 <options>
  Optional options:
     --build-type             cmake build type, Release or Debug, default Release
     --enable-asan            enable asan in build process, default false
     --clean-build            clean and re-build target, default false
     --skip-test              skip building unit tests, benchmarks and tools, default false
     --compiler               compiler type, gcc or clang
     --thirdparty-root        path of thirdparty root
     -j                       build parallel
     -h                       print help message

  Eg.
    $0                                      build in release mode
    $0 --build-type Debug --enable-asan     build in debug mode with enable asan
  "
}

OPTS=$(getopt \
  -n $0 \
  -o 'h' \
  -l 'help' \
  -l 'enable-asan' \
  -l 'build-type:' \
  -l 'clean-build' \
  -l 'skip-test' \
  -l 'compiler:' \
  -l 'thirdparty-root:' \
  -o 'j:' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
    exit 1
fi

eval set -- "$OPTS"

PARALLEL=$[$(nproc)/4+1]
ENABLE_ASAN=0
BUILD_TYPE=Release
BUILD_TEST=1
CLEAN=0
COMPILER=gcc
THIRDPARTY_ROOT=

while true; do
  case "$1" in
    --enable-asan) ENABLE_ASAN=1 ; shift ;;
    --build-type) BUILD_TYPE=$2 ; shift 2 ;;
    --skip-test) BUILD_TEST=0 ; shift ;;
    --clean-build) CLEAN=1 ; shift ;;
    --compiler) COMPILER=$2 ; shift 2 ;;
    --thirdparty-root) THIRDPARTY_ROOT=$2 ; shift 2 ;;
    -h) HELP=1 ; shift ;;
    --help) HELP=1 ; shift ;;
    -j) PARALLEL=$2 ; shift 2 ;;
    --) shift ;  break ;;
    *) usage ; exit 1 ;;
  esac
done

if [[ ${HELP} -eq 1 ]]; then
  usage
  exit 0
fi

case "$BUILD_TYPE" in
  "Release"|"Debug") ;;
  *)
    echo "--build-type can only be Release or Debug"
    exit 1
esac

case "$COMPILER" in
  "gcc"|"clang") ;;
  *)
    echo "--compiler can only be gcc or clang"
    exit 1
esac

if [ ${ENABLE_ASAN} -eq 1 ] && [ $BUILD_TYPE = "Release" ]; then
  echo "Release build can not work with ASAN at present."
  exit 1
fi

if [[ $COMPILER == "clang" ]] && [ -z "${CLANG_HOME}" ]; then
  echo "Please set environment variable 'CLANG_HOME' if you want to compile with clang"
  exit 1
fi

echo "Get params for script:
    ENABLE_ASAN         -- $ENABLE_ASAN
    BUILD_TYPE          -- $BUILD_TYPE
    COMPILER            -- $COMPILER
    BUILD_TEST          -- $BUILD_TEST
    CLEAN               -- $CLEAN
    PARALLEL            -- $PARALLEL
"

CMAKE_BUILD_ARG="-DCMAKE_BUILD_TYPE=${BUILD_TYPE}"
if [ ${ENABLE_ASAN} -eq 1 ]; then
  CMAKE_BUILD_ARG+=" -DENABLE_ASAN=ON"
  MTCACHE_THIRD_PARTY_ROOT=${MTCACHE_THIRD_PARTY_HOME}/install
else
  MTCACHE_THIRD_PARTY_ROOT=${MTCACHE_THIRD_PARTY_HOME}/install_asan
fi

if [ -n $THIRDPARTY_ROOT ]; then
  MTCACHE_THIRD_PARTY_ROOT=${THIRDPARTY_ROOT}
fi
CMAKE_BUILD_ARG+=" -DMTCACHE_THIRDPARTY_ROOT=${MTCACHE_THIRD_PARTY_ROOT}"

if [ ${BUILD_TEST} -eq 0 ]; then
  CMAKE_BUILD_ARG+=" -DMTCACHE_BUILD_TEST=OFF"
else
  CMAKE_BUILD_ARG+=" -DMTCACHE_BUILD_TEST=ON"
fi

if [[ $COMPILER == "gcc" ]]; then
  CMAKE_BUILD_ARG+=" -DCMAKE_C_COMPILER=gcc"
  CMAKE_BUILD_ARG+=" -DCMAKE_CXX_COMPILER=g++"
  # export LD_LIBRARY_PATH=/usr/local/lib64:${LD_LIBRARY_PATH}
elif [[ $COMPILER == "clang" ]]; then
  CMAKE_BUILD_ARG+=" -DCMAKE_C_COMPILER=${CLANG_HOME}/bin/clang"
  CMAKE_BUILD_ARG+=" -DCMAKE_CXX_COMPILER=${CLANG_HOME}/bin/clang++"
  export LD_LIBRARY_PATH=${CLANG_HOME}/lib:${LD_LIBRARY_PATH}
fi

function build_third_party() {
  if [ -n ${THIRDPARTY_ROOT} ] || [ -d ${MTCACHE_THIRD_PARTY_ROOT}/lib ]; then
    echo "Third party library path has existed. Skip compile third party."
    return
  fi
  mkdir -p ${MTCACHE_THIRD_PARTY_HOME}/build
  cd ${MTCACHE_THIRD_PARTY_HOME}
  rm -rf downloads
  cd build
  cmake ${CMAKE_BUILD_ARG} -DCMAKE_INSTALL_PREFIX=${MTCACHE_THIRD_PARTY_ROOT} ..
  # Parallel job number is set in third_party/CMakeList.txt. So we should
  # not set -j of the following `make` command.
  make
}

function build_mtcache() {
  # build thirdparty libraries if necessary
  build_third_party

  if [ $CLEAN -eq 1 ]; then
    rm -rf $CMAKE_BUILD_DIR
  fi
  echo "Cmake build args: ${CMAKE_BUILD_ARG}"
  mkdir -p ${CMAKE_BUILD_DIR}
  cd ${CMAKE_BUILD_DIR}
  cmake ${CMAKE_BUILD_ARG} ..
  make -j${PARALLEL}
}

build_mtcache

echo "***************************************"
echo "Successfully build MTCache"
echo "***************************************"

exit 0
