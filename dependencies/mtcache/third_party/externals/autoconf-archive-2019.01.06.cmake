set(name autoconf-archive-2019.01.06)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
ExternalProject_Add(
    ${name}
    URL https://ftp.gnu.org/gnu/autoconf-archive/autoconf-archive-2019.01.06.tar.xz
    URL_HASH MD5=d46413c8b00a125b1529bae385bbec55
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

add_custom_command(
    TARGET ${name} POST_BUILD
    COMMAND install -m 0644 ${CMAKE_SOURCE_DIR}/externals/pkg.m4 ${CMAKE_INSTALL_PREFIX}/share/aclocal
)
