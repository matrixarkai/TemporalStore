#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MTCACHE_DIR="${ROOT}/thirdparty/mtcache"
PREFIX="${MTCACHE_THIRDPARTY_ROOT:-${MTCACHE_DIR}/third_party/install}"
BUILD_DIR="${ROOT}/.local/ubuntu22/build/mtcache"
JOBS="${JOBS:-$(nproc)}"
FOLLY_VERSION="2021.03.08.00"
FOLLY_ARCHIVE="${BUILD_DIR}/downloads/folly-${FOLLY_VERSION}.zip"
FOLLY_SOURCE="${BUILD_DIR}/folly-${FOLLY_VERSION}/source"
FOLLY_BUILD="${BUILD_DIR}/folly-${FOLLY_VERSION}/build"
ENABLE_MTCACHE_SSD_CACHE="${ENABLE_MTCACHE_SSD_CACHE:-OFF}"
TERARKDB_BRANCH="${TERARKDB_BRANCH:-dev.1.4}"
TERARKDB_SOURCE="${BUILD_DIR}/terarkdb-${TERARKDB_BRANCH}/source"
TERARKDB_BUILD="${BUILD_DIR}/terarkdb-${TERARKDB_BRANCH}/build"
TERARKDB_PATCH="${MTCACHE_DIR}/third_party/patches/terarkdb-20210714.patch"

require_tool() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required tool: $1" >&2
    exit 1
  fi
}

for tool in cmake curl g++ git make patch unzip; do
  require_tool "${tool}"
done

mkdir -p "${PREFIX}/include" "${PREFIX}/lib" "${PREFIX}/lib/cmake" "${BUILD_DIR}/downloads"

copy_noodle() {
  if [[ -f "${PREFIX}/lib/libnoodle.a" && -d "${PREFIX}/include/noodle" ]]; then
    return
  fi

  local candidates=()
  if [[ -n "${MTCACHE_NOODLE_SRC_DIR:-}" ]]; then
    candidates+=("${MTCACHE_NOODLE_SRC_DIR}")
  fi
  candidates+=(
    "/mnt/c/Users/Vincent Jiang/Downloads/ES_C++/minimum/src"
    "/mnt/c/Users/Vincent/Downloads/ES_C++/minimum/src"
  )

  for src in "${candidates[@]}"; do
    if [[ -d "${src}/noodle" && -f "${src}/libs/libnoodle.a" ]]; then
      echo "Installing local Noodle from ${src}"
      cp -a "${src}/noodle" "${PREFIX}/include/"
      cp "${src}/libs/libnoodle.a" "${PREFIX}/lib/"
      return
    fi
  done

  echo "missing Noodle dependency" >&2
  echo "Set MTCACHE_NOODLE_SRC_DIR to a tree containing noodle/ and libs/libnoodle.a" >&2
  exit 1
}

build_folly() {
  if [[ -f "${PREFIX}/lib/cmake/folly/folly-config.cmake" || \
        -f "${PREFIX}/lib/cmake/folly/FollyConfig.cmake" ]]; then
    return
  fi

  if [[ ! -f "${FOLLY_ARCHIVE}" ]]; then
    echo "Downloading Folly ${FOLLY_VERSION}"
    curl -L \
      "https://github.com/facebook/folly/releases/download/v${FOLLY_VERSION}/folly-v${FOLLY_VERSION}.zip" \
      -o "${FOLLY_ARCHIVE}"
  fi

  rm -rf "${FOLLY_SOURCE}" "${FOLLY_BUILD}"
  mkdir -p "${FOLLY_SOURCE}"
  unzip -q "${FOLLY_ARCHIVE}" -d "${FOLLY_SOURCE}"

  (
    cd "${FOLLY_SOURCE}"
    patch -p1 < "${MTCACHE_DIR}/third_party/patches/folly-${FOLLY_VERSION}.patch"
    perl -0pi -e \
      's/noexcept\(awaiter_\.await_ready\(\)\)/noexcept(std::declval<Awaiter&>().await_ready())/g; s/noexcept\(awaiter_\.await_resume\(\)\)/noexcept(std::declval<Awaiter&>().await_resume())/g; s/noexcept\(awaiter_\.await_suspend\(std::declval<WrapperHandle>\(\)\)\)/noexcept(std::declval<Awaiter&>().await_suspend(std::declval<WrapperHandle>()))/g' \
      folly/experimental/coro/ViaIfAsync.h
    perl -0pi -e \
      's/class co_reschedule_on_current_executor_ \{\n/class co_reschedule_on_current_executor_ {\n public:\n/' \
      folly/experimental/coro/CurrentExecutor.h
    perl -0pi -e \
      's/constexpr size_t kAltStackSize = folly::constexpr_max\(SIGSTKSZ, 32 \* 1024\);/constexpr size_t kAltStackSize = 64 * 1024;/g' \
      folly/fibers/FiberManager.cpp
  )

  cmake -S "${FOLLY_SOURCE}" -B "${FOLLY_BUILD}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${PREFIX}" \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DFOLLY_CXX_FLAGS=-Wno-error \
    -DBoost_NO_BOOST_CMAKE=ON \
    -DFOLLY_USE_JEMALLOC=OFF
  cmake --build "${FOLLY_BUILD}" --parallel "${JOBS}"
  cmake --install "${FOLLY_BUILD}"
}

build_terarkdb() {
  local terarkdb_libs=(
    "${PREFIX}/lib/libterarkdb.a"
    "${PREFIX}/lib/libsnappy.a"
    "${PREFIX}/lib/libz.a"
    "${PREFIX}/lib/libzstd.a"
    "${PREFIX}/lib/liblz4.a"
    "${PREFIX}/lib/libbz2.a"
  )
  local need_build=0
  if [[ ! -f "${PREFIX}/include/terarkdb/rocksdb/db.h" ]]; then
    need_build=1
  fi
  for lib in "${terarkdb_libs[@]}"; do
    if [[ ! -f "${lib}" ]]; then
      need_build=1
    fi
  done
  if [[ "${need_build}" == "0" ]]; then
    return
  fi

  if [[ ! -d "${TERARKDB_SOURCE}/.git" ]]; then
    rm -rf "${TERARKDB_SOURCE}"
    mkdir -p "$(dirname "${TERARKDB_SOURCE}")"
    git clone --depth 1 --branch "${TERARKDB_BRANCH}" \
      https://github.com/bytedance/terarkdb.git "${TERARKDB_SOURCE}"
  fi

  (
    cd "${TERARKDB_SOURCE}"
    git submodule update --init --recursive --depth 1 \
      third-party/terark-zip \
      third-party/zstd \
      third-party/zlib \
      third-party/snappy \
      third-party/lz4
    if git apply --check "${TERARKDB_PATCH}" >/dev/null 2>&1; then
      git apply "${TERARKDB_PATCH}"
    elif git apply --reverse --check "${TERARKDB_PATCH}" >/dev/null 2>&1; then
      echo "TerarkDB patch already applied"
    else
      echo "TerarkDB patch cannot be applied cleanly" >&2
      exit 1
    fi
  )

  rm -rf "${TERARKDB_BUILD}"
  cmake -S "${TERARKDB_SOURCE}" -B "${TERARKDB_BUILD}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${PREFIX}" \
    -DWITH_LZ4=ON \
    -DWITH_BOOSTLIB=OFF \
    -DWITH_JEMALLOC=OFF \
    -DWITH_GFLAGS=OFF \
    -DWITH_TESTS=OFF \
    -DWITH_TOOLS=OFF \
    -DWITH_TERARK_ZIP=OFF \
    -DWITH_ASAN=OFF \
    -DWITH_TERARKDB_NAMESPACE=rocksdb \
    -DTERARK_INCLUDE_PREFIX=terarkdb \
    -DWITH_BYTEDANCE_METRICS=OFF
  cmake --build "${TERARKDB_BUILD}" --parallel "${JOBS}" --target install
  cp \
    "${TERARKDB_BUILD}/lib/libsnappy.a" \
    "${TERARKDB_BUILD}/lib/libz.a" \
    "${TERARKDB_BUILD}/lib/libzstd.a" \
    "${TERARKDB_BUILD}/lib/liblz4.a" \
    "${TERARKDB_BUILD}/lib/libbz2.a" \
    "${PREFIX}/lib/"
}

copy_noodle
build_folly
if [[ "${ENABLE_MTCACHE_SSD_CACHE}" == "ON" ]]; then
  build_terarkdb
fi

mkdir -p "${MTCACHE_DIR}/3rd"
ln -sfn "../third_party/install" "${MTCACHE_DIR}/3rd/install"

echo "MtCache dependencies are ready in ${PREFIX}"
