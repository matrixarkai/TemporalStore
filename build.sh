#!/bin/bash
# Compilation w/ gcc630: BCACHE2_TOOLCHAIN=gcc630 ./build.sh [Debug|Release] [JOBS]

set -exo pipefail

cd $(dirname $0)

BUILD_MODE=${1:-Debug}
JOBS=${2:-$(nproc 2>/dev/null || echo 8)}

CMAKE_ARGS="-DCUSTOM_SUFFIX=${CUSTOM_SUFFIX} -DENABLE_BRPC_PROFILE=ON -DBYTE_BUILD_TESTS=OFF -DWITH_GLOG=OFF -DBRPC_WITH_GLOG=OFF -DBRPC_WITH_THRIFT=ON -DWITH_THRIFT=ON -DENABLE_MTCACHE=ON -DWITH_BOOST_STATIC=ON -DBoost_USE_STATIC_RUNTIME=ON -DBoost_USE_STATIC_LIBS=ON -DOPENSSL_USE_STATIC_LIBS=ON"

# for SCM compilation
if [ "${BUILD_MODE}" == "SCM" ]; then
    if [ "$BUILD_TYPE" == "online" ]; then
        BUILD_MODE="Release"
    else
        BUILD_MODE="Debug"
    fi
fi

if [ "${BUILD_MODE}" == "Release" ]; then
    CMAKE_ARGS="${CMAKE_ARGS} -DCMAKE_BUILD_TYPE=Release"
    BUILD_FLAVOR="release"
elif [ "${BUILD_MODE}" == "Debug" ]; then
    CMAKE_ARGS="${CMAKE_ARGS} -DCMAKE_BUILD_TYPE=Debug"
    BUILD_FLAVOR="debug"
else
    # Mainly for local compiling
    CMAKE_ARGS="${CMAKE_ARGS} -DCMAKE_BUILD_TYPE=Debug -DENABLE_COVERAGE=ON -DENABLE_ASAN=ON -DENABLE_FIU=ON"
    BUILD_FLAVOR="debug-sanitizer"
fi

BUILD_DIR=${BUILD_DIR:-build/${BUILD_FLAVOR}}
OUTPUT_DIR=${OUTPUT_DIR:-output/${BUILD_FLAVOR}}
CMAKE_ARGS="${CMAKE_ARGS} -DBCACHE2_OUTPUT_DIR=${OUTPUT_DIR}"

if [ "$CUSTOM_ENABLE_FIU" == "true" ]; then
    CMAKE_ARGS="${CMAKE_ARGS} -DENABLE_FIU=ON"
fi

TOOLCHAIN=${BCACHE2_TOOLCHAIN:-gcc830}
if [[ -n "${MODULESHOME}" ]] && [[ -f "$MODULESHOME/init/bash" ]]; then
    source $MODULESHOME/init/bash
    if [[ $TOOLCHAIN == "gcc830" ]]; then
        module switch gcc/8.3.0
    elif [[ $TOOLCHAIN == "gcc630" ]]; then
        module switch gcc/6.3.0
    else
        echo "Toolchain [${TOOLCHAIN}] is not supported!"
        echo "Supported toolchains [gcc630, gcc830]"
        exit 1
    fi
    echo "Using ${TOOLCHAIN}"
    echo GCC $(gcc --version | head -1 | awk 'BEGIN{FS=") "}{print $2}')
    echo GLIBC $(ldd --version | head -1 | awk '{print $NF}')
elif [[ -n "${BCACHE2_TOOLCHAIN}" ]] && [[ "${BCACHE2_TOOLCHAIN}" != "gcc830" ]]; then
    echo "Environment Module was not found, could not switch to ${BCACHE2_TOOLCHAIN}"
    exit 1
fi

export ASAN_OPTIONS=detect_leaks=false,abort_on_error=true
git submodule update --init --recursive \
    && cmake -S . -B "${BUILD_DIR}" ${CMAKE_ARGS} \
    && cmake --build "${BUILD_DIR}" --parallel $JOBS --target cpplint \
    && cmake --build "${BUILD_DIR}" --parallel $JOBS

ret=$?
if [ $ret -ne 0 ]; then
    exit $ret
fi

if [ $CUSTOM_SUFFIX ]; then
    for RAWBIN in $(ls "${OUTPUT_DIR}"/bcache2-* | awk '{print}')
    do
        cp $RAWBIN $RAWBIN"-"${CUSTOM_SUFFIX}
    done
fi
