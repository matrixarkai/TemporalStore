set(name googletest-1.11.0)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
ExternalProject_Add(
    ${name}
    URL https://github.com/google/googletest/archive/refs/tags/release-1.11.0.tar.gz
    URL_HASH MD5=e8a8df240b6938bb6384155d4c37d937
    DOWNLOAD_NAME  googletest-1.11.0.tar.gz
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    PATCH_COMMAND patch -p1 < ${CMAKE_SOURCE_DIR}/patches/googletest-1.11.0.patch
    BUILD_IN_SOURCE 1
    CMAKE_ARGS
        ${common_cmake_args}
        -DWITH_ASAN=${ENABLE_ASAN}
    BUILD_COMMAND make -s -j${BUILDING_JOBS_NUM}
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
)
