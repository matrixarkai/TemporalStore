set(name glog-0.3.5)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
ExternalProject_Add(
    ${name}
    URL https://github.com/google/glog/archive/v0.3.5.tar.gz
    URL_HASH MD5=5df6d78b81e51b90ac0ecd7ed932b0d4
    DOWNLOAD_NAME glog-0.3.5.tar.gz
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    PATCH_COMMAND patch -p1 < ${CMAKE_SOURCE_DIR}/patches/glog-0.3.5.patch
    CMAKE_ARGS
        ${common_cmake_args}
        -DWITH_ASAN=${ENABLE_ASAN}
        -DBUILD_TESTING=OFF
    BUILD_COMMAND make -s -j${BUILDING_JOBS_NUM}
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND make -s -j${BUILDING_JOBS_NUM} install
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
)