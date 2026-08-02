set(name jemalloc-5.2.1)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
ExternalProject_Add(
    ${name}
    URL https://github.com/jemalloc/jemalloc/archive/refs/tags/5.2.1.tar.gz
    URL_HASH MD5=0d627898d4aa58d09ef5d3fdde17dacb
    DOWNLOAD_NAME jemalloc-5.2.1.tar.gz
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    CONFIGURE_COMMAND
        ${common_configure_envs}
        ./configure ${common_configure_args}
                    --disable-stats --enable-prof --enable-prof-libunwind --with-static-libunwind=${CMAKE_INSTALL_PREFIX}/lib/libunwind.a
    BUILD_COMMAND make -s -j${BUILDING_JOBS_NUM}
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND make -s install_bin install_include install_lib_static -j${BUILDING_JOBS_NUM}
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
)

ExternalProject_Add_Step(${name} autogen
    DEPENDEES download
    DEPENDERS configure 
    ALWAYS FALSE
    COMMAND ./autogen.sh
    WORKING_DIRECTORY ${source_dir}
)
ExternalProject_Add_StepTargets(${name} autogen)
