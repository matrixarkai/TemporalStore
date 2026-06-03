set(name libaio-0.3.110-1)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
ExternalProject_Add(
    ${name}
    URL https://github.com/crossbuild/libaio/archive/libaio-0.3.110-1.tar.gz
    URL_HASH MD5=266b58badf6d010eab433abc8713d959
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    CONFIGURE_COMMAND ""
    BUILD_COMMAND env CFLAGS=-fPIC make -s -j${BUILDING_JOBS_NUM}
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND make prefix=${CMAKE_INSTALL_PREFIX} -s install -j${BUILDING_JOBS_NUM}
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
)

add_custom_command(
    TARGET ${name} POST_BUILD
    COMMAND
        rm -f ${CMAKE_INSTALL_PREFIX}/lib/libaio.so*
)
