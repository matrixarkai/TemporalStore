# -Wno-class-memaccess: suppress warnings when objects of non-trivial class types
#   are manipulated in potentially unsafe ways by raw memory functions
# since gcc 8.1
add_cxx_compiler_flag(-Wno-class-memaccess)

if (ENABLE_MTCACHE)
    add_library(blockcache-impl-objects OBJECT blockcache_impl.h blockcache_impl.cc)
    set(blockcache-deps ${MTCACHE_THIRDPARTY_ROOT}/include)
    if (ENABLE_ASAN)
        set(blockcache-deps ${MTCACHE_THIRDPARTY_ROOT}/include)
    endif()
    target_include_directories(blockcache-impl-objects PRIVATE ${blockcache-deps})
    target_link_libraries(blockcache-impl-objects PRIVATE
        bcache2_proto
        mtcache)
    set_target_properties(blockcache-impl-objects PROPERTIES
        POSITION_INDEPENDENT_CODE ON
        CXX_STANDARD 17
        COMPILE_FLAGS "-Wno-error")

    add_library(blockcache-impl STATIC $<TARGET_OBJECTS:blockcache-impl-objects>)
    target_link_libraries(blockcache-impl PUBLIC
        bcache2_proto
        mtcache)
    set_target_properties(blockcache-impl PROPERTIES CXX_STANDARD 17)

    add_library(blockcache-impl-shared SHARED $<TARGET_OBJECTS:blockcache-impl-objects>)
    target_link_libraries(blockcache-impl-shared PRIVATE
        bcache2_proto
        mtcache)
    set_target_properties(blockcache-impl-shared PROPERTIES
        OUTPUT_NAME blockcache-impl
        CXX_STANDARD 17
        LINK_FLAGS "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/exported_symbols.lds")

    add_library(blockcache flags.h flags.cc blockcache.h blockcache.cc)
    target_link_libraries(blockcache blockcache-impl common gflags)
    set_target_properties(blockcache PROPERTIES CXX_STANDARD 14)

    add_custom_command(
        TARGET blockcache-impl-shared POST_BUILD
        COMMAND "${CMAKE_COMMAND}" -E copy
        "$<TARGET_FILE:blockcache-impl-shared>"
        "${PROJECT_SOURCE_DIR}/output/lib/$<TARGET_FILE_NAME:blockcache-impl-shared>")
    if (BCACHE2_BUILD_TESTS)
        add_subdirectory(test)
    endif()
else()
    add_library(blockcache flags.h flags.cc blockcache.h)
    target_link_libraries(blockcache common gflags)
    set_target_properties(blockcache PROPERTIES CXX_STANDARD 14)
endif()

add_subdirectory(rdmareadcache)
