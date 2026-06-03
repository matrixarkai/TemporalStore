set(name noodle-v20210325)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
ExternalProject_Add(
    ${name}
    URL "/mnt/c/Users/Vincent Jiang/Downloads/noodle.zip"
    DOWNLOAD_NAME noodle-v20210325.zip
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    PATCH_COMMAND
        sed -i "s/pthread_yield()/sched_yield()/g" ${source_dir}/include/noodle/concurrent/thread.h
        COMMAND sed -i "/add_compile_options(-Werror)/d" ${source_dir}/CMakeLists.txt
    CMAKE_ARGS
        ${common_cmake_args}
        -DHSAP_THIRDPARTY_HOME=${CMAKE_SOURCE_DIR}
        -DCMAKE_BUILD_TYPE=Release
    BUILD_IN_SOURCE 1
    BUILD_COMMAND make -s -j${BUILDING_JOBS_NUM}
    INSTALL_COMMAND make -s install -j${BUILDING_JOBS_NUM}
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
)
