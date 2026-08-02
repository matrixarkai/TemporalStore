set(name automake-1.15.1)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
ExternalProject_Add(
    ${name}
    URL https://ftp.gnu.org/gnu/automake/automake-1.15.1.tar.xz
    URL_HASH MD5=24cd3501b6ad8cd4d7e2546f07e8b4d4
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    CONFIGURE_COMMAND
        ${common_configure_envs}
        "LIBS=${LIBS}"
        ./configure ${common_configure_args}
    BUILD_IN_SOURCE 1
    BUILD_COMMAND make -s -j${BUILDING_JOBS_NUM}
    INSTALL_COMMAND make -s -j${BUILDING_JOBS_NUM} install
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
)
