#!/usr/bin/env bash
set -euxo pipefail

SOURCE_DIR="/mnt/c/Users/Vincent Jiang/Downloads/bytekv-master/bytekv-master"
cd "${SOURCE_DIR}"

scripts/gen_commit_id "${SOURCE_DIR}" || true

BUILD_DIR="${SOURCE_DIR}/build-release"
mkdir -p "${BUILD_DIR}"

export ARROW_SNAPPY_URL="$(readlink -f ./third/arrow_third/snappy-1.1.8.tar.gz)"
export ARROW_BOOST_URL="$(readlink -f ./third/arrow_third/boost-1.71.0.tar.gz)"
export ARROW_THRIFT_URL="$(readlink -f ./third/arrow_third/thrift-0.12.0.tar.gz)"

cd "${BUILD_DIR}"
cmake "${SOURCE_DIR}" \
  -DLINK_TCMALLOC=ON \
  -DENABLE_BRPC_PROFILE=OFF \
  -DBYTERAFT_WITH_EXAMPLE=OFF \
  -DBYTERAFT_BUILD_TESTS=OFF \
  -DBYTEKV_BUILD_TESTS=OFF \
  -DBYTERAFT_WITH_JEPSEN=OFF \
  -DBYTE_BUILD_TESTS=OFF \
  -DBUILD_MSGPACK=OFF \
  -DBUILD_GF_COMPLETE=OFF \
  -DBUILD_JERASURE=OFF \
  -DBUILD_CRYPTOPP=OFF \
  -DCPU_PROFILER=OFF \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo

cmake --build . -j2
