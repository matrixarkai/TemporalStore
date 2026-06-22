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
BRPC_WITH_GLOG="${BRPC_WITH_GLOG:-}"
MTCACHE_THIRDPARTY_ROOT="${MTCACHE_THIRDPARTY_ROOT:-}"
MTCACHE_CMAKE_COMPAT_DIR="${MTCACHE_CMAKE_COMPAT_DIR:-${ROOT}/cmake/compat}"
BCACHE2_BUILD_TESTS="${BCACHE2_BUILD_TESTS:-OFF}"
BUILD_TARGETS="${BUILD_TARGETS:-}"
OBJECT_STORE_COMPAT_INCLUDE_DIR="${OBJECT_STORE_COMPAT_INCLUDE_DIR:-}"
BYTESTORE_COMPAT_INCLUDE_DIR="${BYTESTORE_COMPAT_INCLUDE_DIR:-}"
BRPC_STATIC_LIBRARY="${BRPC_STATIC_LIBRARY:-}"
EXTRA_CMAKE_ARGS="${EXTRA_CMAKE_ARGS:-}"

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

prepare_link_shims() {
  local shim_dir="${ROOT}/.local/link-shims"
  mkdir -p "${shim_dir}"

  if [[ ! -f "${shim_dir}/libco.a" ]]; then
    rm -f "${shim_dir}/empty.o"
    ar rcs "${shim_dir}/libco.a"
  fi
  if [[ ! -f "${shim_dir}/libabsl_flat_hash_map.a" ]]; then
    ar rcs "${shim_dir}/libabsl_flat_hash_map.a"
  fi
  if [[ -f "/usr/lib/x86_64-linux-gnu/liblz4.a" ]]; then
    ln -sfn "/usr/lib/x86_64-linux-gnu/liblz4.a" "${shim_dir}/liblz4_static.a"
  fi
}

prepare_link_shims

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
  BRPC_WITH_GLOG="${BRPC_WITH_GLOG:-ON}"
  export ENABLE_MTCACHE_SSD_CACHE
  bash "${ROOT}/tools/prepare_mtcache_ubuntu22.sh"
  MTCACHE_THIRDPARTY_ROOT="${MTCACHE_THIRDPARTY_ROOT:-${ROOT}/thirdparty/mtcache/third_party/install}"
  export CMAKE_PREFIX_PATH="${MTCACHE_CMAKE_COMPAT_DIR}:${MTCACHE_THIRDPARTY_ROOT}:${CMAKE_PREFIX_PATH:-}"
  if [[ -n "${BRPC_STATIC_LIBRARY}" ]]; then
    BRPC_STATIC_LIBRARY="$(BRPC_STATIC_LIBRARY="${BRPC_STATIC_LIBRARY}" \
      bash "${ROOT}/tools/prepare_brpc_mtcache_ubuntu22.sh")"
  fi
fi
BRPC_WITH_GLOG="${BRPC_WITH_GLOG:-OFF}"

declare -a COMPAT_ARGS=()
if [[ -n "${MTCACHE_THIRDPARTY_ROOT}" ]]; then
  COMPAT_ARGS+=(-DMTCACHE_THIRDPARTY_ROOT="${MTCACHE_THIRDPARTY_ROOT}")
fi
if [[ "${ENABLE_MTCACHE}" == "ON" && -d "${MTCACHE_CMAKE_COMPAT_DIR}/boost_iostreams-1.74.0" ]]; then
  COMPAT_ARGS+=(-Dboost_iostreams_DIR="${MTCACHE_CMAKE_COMPAT_DIR}/boost_iostreams-1.74.0")
fi
if [[ -n "${OBJECT_STORE_COMPAT_INCLUDE_DIR}" ]]; then
  COMPAT_ARGS+=(-DOBJECT_STORE_COMPAT_INCLUDE_DIR="${OBJECT_STORE_COMPAT_INCLUDE_DIR}")
fi
if [[ -n "${BYTESTORE_COMPAT_INCLUDE_DIR}" ]]; then
  COMPAT_ARGS+=(-DBYTESTORE_COMPAT_INCLUDE_DIR="${BYTESTORE_COMPAT_INCLUDE_DIR}")
fi
if [[ -n "${BRPC_STATIC_LIBRARY}" ]]; then
  COMPAT_ARGS+=(-DBRPC_STATIC_LIBRARY="${BRPC_STATIC_LIBRARY}")
fi
if [[ -n "${EXTRA_CMAKE_ARGS}" ]]; then
  # shellcheck disable=SC2206
  COMPAT_ARGS+=(${EXTRA_CMAKE_ARGS})
fi

echo "Using $(gcc --version | head -1)"
echo "Using $(g++ --version | head -1)"
echo "Using $(cmake --version | head -1)"
echo "Using $(protoc --version)"
echo "Build type: ${BUILD_TYPE}"
echo "Build dir: ${BUILD_DIR}"
echo "Output dir: ${OUTPUT_DIR}"
echo "BCACHE2 build tests: ${BCACHE2_BUILD_TESTS}"
if [[ -n "${BUILD_TARGETS}" ]]; then
  echo "Build targets: ${BUILD_TARGETS}"
fi

cmake -S "${ROOT}" -B "${BUILD_DIR}" \
  -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
  -DBCACHE2_OUTPUT_DIR="${OUTPUT_DIR}" \
  "${PROTO_ARGS[@]}" \
  -DENABLE_MTCACHE="${ENABLE_MTCACHE}" \
  -DENABLE_MTCACHE_SSD_CACHE="${ENABLE_MTCACHE_SSD_CACHE}" \
  -DENABLE_BRPC_PROFILE=ON \
  -DBYTE_BUILD_TESTS=OFF \
  -DBCACHE2_BUILD_TESTS="${BCACHE2_BUILD_TESTS}" \
  -DWITH_GLOG=OFF \
  -DBRPC_WITH_GLOG="${BRPC_WITH_GLOG}" \
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
  -DOPENSSL_CRYPTO_LIBRARY=/usr/lib/x86_64-linux-gnu/libcrypto.so \
  "${COMPAT_ARGS[@]}"

if [[ -n "${BUILD_TARGETS}" ]]; then
  read -r -a targets <<< "${BUILD_TARGETS}"
  cmake --build "${BUILD_DIR}" --parallel "${JOBS}" --target "${targets[@]}"
else
  cmake --build "${BUILD_DIR}" --parallel "${JOBS}"
fi
