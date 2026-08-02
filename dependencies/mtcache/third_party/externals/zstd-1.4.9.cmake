set(name zstd-1.4.9)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
set(MakeEnvs "env" "CFLAGS=-fPIC")
ExternalProject_Add(
    ${name}
    URL https://github.com/facebook/zstd/releases/download/v1.4.9/zstd-1.4.9.tar.gz
    URL_HASH MD5=eb718b8aae0302cabe20f968e500534d
    DOWNLOAD_NAME zstd-1.4.9.tar.gz
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    CONFIGURE_COMMAND ""
    BUILD_COMMAND
        "${MakeEnvs}"
        make -e -s -j${BUILDING_JOBS_NUM}
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND make -s install -j${BUILDING_JOBS_NUM} PREFIX=${CMAKE_INSTALL_PREFIX}
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
)
