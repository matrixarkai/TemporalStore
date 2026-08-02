set(name double-conversion-1.1.6)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
ExternalProject_Add(
    ${name}
    URL https://github.com/google/double-conversion/archive/v1.1.6.tar.gz
    URL_HASH MD5=94f9abc9b1367083cf3e4569886b4170
    DOWNLOAD_NAME double-conversion-1.1.6.tar.gz
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    CMAKE_ARGS
        ${common_cmake_args}
        -DCMAKE_EXPORT_NO_PACKAGE_REGISTRY=ON
    BUILD_IN_SOURCE 1
    BUILD_COMMAND make -s -j${BUILDING_JOBS_NUM}
    INSTALL_COMMAND make -s install -j${BUILDING_JOBS_NUM}
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
)
