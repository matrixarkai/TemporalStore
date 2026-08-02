cmake_minimum_required(VERSION 3.16)
project(temporalstore_cpp_sdk LANGUAGES CXX)

add_library(temporalstore_cpp INTERFACE)
target_compile_features(temporalstore_cpp INTERFACE cxx_std_11)
target_include_directories(temporalstore_cpp INTERFACE
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
    "${CMAKE_CURRENT_LIST_DIR}/../../src")

if (TARGET client)
    target_link_libraries(temporalstore_cpp INTERFACE client)
endif()
