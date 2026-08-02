set(name folly-2021.03.08.00)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
ExternalProject_Add(
    ${name}
    URL https://github.com/facebook/folly/releases/download/v2021.03.08.00/folly-v2021.03.08.00.zip
    URL_HASH MD5=ea3375ef5f6c0019a13f65613fd57a65
    DOWNLOAD_NAME folly-2021.03.08.00.zip
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    PATCH_COMMAND patch -p1 < ${CMAKE_SOURCE_DIR}/patches/folly-2021.03.08.00.patch
    CMAKE_ARGS
        ${common_cmake_args}
        -DCMAKE_BUILD_TYPE=Release
        "-DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS} -fPIC -DFOLLY_HAVE_CLOCK_GETTIME -D__USE_POSIX199309 ${extra_cpp_flags}"
        -DFOLLY_CXX_FLAGS=-Wno-error
        -DBoost_NO_BOOST_CMAKE=BOOL:ON
        -DFOLLY_LIBRARY_SANITIZE_ADDRESS=${ENABLE_ASAN}

    BUILD_COMMAND make -s -j${BUILDING_JOBS_NUM}
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND make -s -j${BUILDING_JOBS_NUM} install/strip
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
)
