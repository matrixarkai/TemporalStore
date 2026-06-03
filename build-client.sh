#!/bin/bash
# Compilation w/ gcc630: BCACHE2_TOOLCHAIN=gcc630 ./build-client.sh [Debug|Release] [JOBS]

set -ex

cd "$(dirname "$0")"

BUILD_MODE=${1:-Debug}
JOBS=${2:-$(nproc 2>/dev/null || echo 8)}
CMAKE_ARGS=(
    "-DCUSTOM_SUFFIX=${CUSTOM_SUFFIX}"
    "-DENABLE_BRPC_PROFILE=OFF"
    "-DBYTE_BUILD_TESTS=OFF"
    "-DWITH_GLOG=OFF"
    "-DBRPC_WITH_GLOG=OFF"
    "-DBRPC_WITH_THRIFT=OFF"
    "-DWITH_THRIFT=OFF"
    "-DENABLE_MTCACHE=OFF"
    "-DBCACHE2_PROTOBUF_USE_STATIC_LIBS=OFF"
    "-DWITH_BOOST_STATIC=ON"
    "-DBoost_USE_STATIC_RUNTIME=ON"
    "-DBoost_USE_STATIC_LIBS=ON"
    "-DOPENSSL_USE_STATIC_LIBS=ON"
)
if [ "${BUILD_MODE}" == "Release" ]; then
    CMAKE_ARGS+=("-DCMAKE_BUILD_TYPE=Release")
    BUILD_FLAVOR="release"
elif [ "${BUILD_MODE}" == "Debug" ]; then
    CMAKE_ARGS+=("-DCMAKE_BUILD_TYPE=Debug")
    BUILD_FLAVOR="debug"
else
    # Mainly for local compiling
    CMAKE_ARGS+=("-DCMAKE_BUILD_TYPE=Debug" "-DENABLE_COVERAGE=ON" "-DENABLE_ASAN=ON")
    BUILD_FLAVOR="debug-sanitizer"
fi

BUILD_DIR=${BUILD_DIR:-build-client/${BUILD_FLAVOR}}
OUTPUT_DIR=${OUTPUT_DIR:-output-client/${BUILD_FLAVOR}}
CMAKE_ARGS+=("-DBCACHE2_OUTPUT_DIR=${OUTPUT_DIR}")

export ASAN_OPTIONS=detect_leaks=false,abort_on_error=true
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git -c submodule.metaserver.update=none submodule update --init --recursive --progress
else
    echo "Skip git submodule update: current source tree is not a git checkout"
fi

cmake -S . -B "${BUILD_DIR}" "${CMAKE_ARGS[@]}" \
    && cmake --build "${BUILD_DIR}" --parallel "${JOBS}" --target bundling_target bcache2-shared
