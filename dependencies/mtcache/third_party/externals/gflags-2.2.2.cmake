set(name gflags-2.2.2)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
ExternalProject_Add(
    ${name}
    URL https://github.com/gflags/gflags/archive/refs/tags/v2.2.2.tar.gz
    URL_HASH MD5=1a865b93bacfa963201af3f75b7bd64c
    DOWNLOAD_NAME gflags-2.2.2.tar.gz
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    PATCH_COMMAND patch -p1 < ${CMAKE_SOURCE_DIR}/patches/gflags-2.2.2.patch
    CMAKE_ARGS
        ${common_cmake_args}
        -DWITH_ASAN=${ENABLE_ASAN}
        -DREGISTER_INSTALL_PREFIX=OFF
    BUILD_COMMAND make -s -j${BUILDING_JOBS_NUM}
    BUILD_IN_SOURCE 1
)
