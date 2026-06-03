#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_TYPE="${BUILD_TYPE:-Debug}"
case "${BUILD_TYPE}" in
  Debug|debug)
    BUILD_TYPE="Debug"
    BUILD_FLAVOR="debug"
    ;;
  Release|release)
    BUILD_TYPE="Release"
    BUILD_FLAVOR="release"
    ;;
  *)
    echo "unsupported BUILD_TYPE=${BUILD_TYPE}; use Debug or Release" >&2
    exit 1
    ;;
esac
JOBS="${JOBS:-$(nproc)}"
BUILD_DIR="${BUILD_DIR:-${ROOT}/build-ubuntu22/${BUILD_FLAVOR}}"
OUTPUT_DIR="${OUTPUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
DEPS_DIR="${DEPS_DIR:-${ROOT}/.local/ubuntu22}"
PROTOBUF_VERSION="3.2.0"
PROTOBUF_PREFIX="${PROTOBUF_PREFIX:-${DEPS_DIR}/protobuf-${PROTOBUF_VERSION}}"
ENABLE_MTCACHE="${ENABLE_MTCACHE:-OFF}"
ENABLE_MTCACHE_SSD_CACHE="${ENABLE_MTCACHE_SSD_CACHE:-OFF}"

require_tool() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required tool: $1" >&2
    echo "install Ubuntu packages from docker/README.ubuntu22.md, then rerun" >&2
    exit 1
  fi
}

for tool in cmake gcc g++ make git; do
  require_tool "${tool}"
done

build_protobuf() {
  local pb_src="${ROOT}/dependencies/byte/thirdparty/protobuf/cmake"
  local pb_build="${DEPS_DIR}/build/protobuf-${PROTOBUF_VERSION}"

  if [[ -x "${PROTOBUF_PREFIX}/bin/protoc" ]]; then
    return
  fi

  if [[ ! -d "${pb_src}" ]]; then
    echo "Pinned protobuf source missing: ${pb_src}"
    echo "Using system protoc for build."
    return
  fi
  if ! touch "${pb_src}/.protobuf-build-probe" 2>/dev/null; then
    echo "Pinned protobuf source is read-only: ${pb_src}"
    echo "Using system protoc for build."
    return
  fi
  rm -f "${pb_src}/.protobuf-build-probe"

  echo "Building pinned Protobuf ${PROTOBUF_VERSION} into ${PROTOBUF_PREFIX}"
  cmake -S "${pb_src}" -B "${pb_build}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${PROTOBUF_PREFIX}" \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -Dprotobuf_BUILD_TESTS=OFF \
    -Dprotobuf_BUILD_SHARED_LIBS=OFF
  cmake --build "${pb_build}" --parallel "${JOBS}" --target install
}

build_protobuf

declare -a PROTO_ARGS=()
if [[ -x "${PROTOBUF_PREFIX}/bin/protoc" ]]; then
  export PATH="${PROTOBUF_PREFIX}/bin:${PATH}"
  export CMAKE_PREFIX_PATH="${PROTOBUF_PREFIX}:${CMAKE_PREFIX_PATH:-}"
  PROTO_ARGS=(
    -DBCACHE2_PROTOBUF_ROOT_DIR="${PROTOBUF_PREFIX}"
    -DProtobuf_ROOT="${PROTOBUF_PREFIX}"
    -DPROTOBUF_ROOT_DIR="${PROTOBUF_PREFIX}"
    -DProtobuf_PROTOC_EXECUTABLE="${PROTOBUF_PREFIX}/bin/protoc"
    -DPROTOBUF_PROTOC_EXECUTABLE="${PROTOBUF_PREFIX}/bin/protoc"
  )
else
  # protobuf sources are intentionally optional in this environment; use distro packages when pinned source isn't present.
  echo "Using system protobuf (protoc version: $(protoc --version 2>&1 | head -1))"
fi

if [[ "${ENABLE_MTCACHE}" == "ON" ]]; then
  export ENABLE_MTCACHE_SSD_CACHE
  bash "${ROOT}/tools/prepare_mtcache_ubuntu22.sh"
  export CMAKE_PREFIX_PATH="${ROOT}/thirdparty/mtcache/third_party/install:${CMAKE_PREFIX_PATH:-}"
fi

echo "Using $(gcc --version | head -1)"
echo "Using $(g++ --version | head -1)"
echo "Using $(cmake --version | head -1)"
echo "Using $(protoc --version)"
echo "Build type: ${BUILD_TYPE}"
echo "Build dir: ${BUILD_DIR}"
echo "Output dir: ${OUTPUT_DIR}"

cmake -S "${ROOT}" -B "${BUILD_DIR}" \
  -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
  -DBCACHE2_OUTPUT_DIR="${OUTPUT_DIR}" \
  "${PROTO_ARGS[@]}" \
  -DENABLE_MTCACHE="${ENABLE_MTCACHE}" \
  -DENABLE_MTCACHE_SSD_CACHE="${ENABLE_MTCACHE_SSD_CACHE}" \
  -DENABLE_BRPC_PROFILE=ON \
  -DBYTE_BUILD_TESTS=OFF \
  -DBCACHE2_BUILD_TESTS=OFF \
  -DWITH_GLOG=OFF \
  -DBRPC_WITH_GLOG=OFF \
  -DBRPC_WITH_THRIFT=ON \
  -DBRPC_BUILD_SHARED=OFF \
  -DBRPC_BUILD_TOOLS=OFF \
  -DBRPC_BUILD_PROTOC_GEN_MCPACK=OFF \
  -DWITH_THRIFT=ON \
  -DWITH_BOOST_STATIC=ON \
  -DBoost_USE_STATIC_RUNTIME=ON \
  -DBoost_USE_STATIC_LIBS=ON \
  -DOPENSSL_USE_STATIC_LIBS=OFF \
  -DOPENSSL_SSL_LIBRARY=/usr/lib/x86_64-linux-gnu/libssl.so \
  -DOPENSSL_CRYPTO_LIBRARY=/usr/lib/x86_64-linux-gnu/libcrypto.so

cmake --build "${BUILD_DIR}" --parallel "${JOBS}"
