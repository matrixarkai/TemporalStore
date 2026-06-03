# - Adds a linker flag if it is supported by the linker
#
# This function checks that the supplied linker flag is supported and then
# adds it to the corresponding linker flags
#
#  add_cxx_linker_flag(<FLAG> [<VARIANT>])
#
# - Example
#
# include(AddExecLinkerFlag)
# add_cxx_linker_flag(-pthread)
# add_cxx_linker_flag(-ldl RELEASE)
# Requires CMake 3.18+

if(__add_cxx_linker_flag)
  return()
endif()
set(__add_cxx_linker_flag INCLUDED)

include(CheckLinkerFlag)

function(mangle_linker_flag FLAG OUTPUT)
  string(TOUPPER "HAVE_CXX_LINKER_FLAG_${FLAG}" SANITIZED_FLAG)
  string(REPLACE "+" "X" SANITIZED_FLAG ${SANITIZED_FLAG})
  string(REGEX REPLACE "[^A-Za-z_0-9]" "_" SANITIZED_FLAG ${SANITIZED_FLAG})
  string(REGEX REPLACE "_+" "_" SANITIZED_FLAG ${SANITIZED_FLAG})
  set(${OUTPUT} "${SANITIZED_FLAG}" PARENT_SCOPE)
endfunction(mangle_linker_flag)

function(add_cxx_linker_flag FLAG)
  mangle_linker_flag("${FLAG}" MANGLED_FLAG)
  set(OLD_CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS}")
  set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} ${FLAG}")
  check_linker_flag(CXX "${FLAG}" ${MANGLED_FLAG})
  set(CMAKE_REQUIRED_FLAGS "${OLD_CMAKE_REQUIRED_FLAGS}")
  if(${MANGLED_FLAG})
    set(VARIANT ${ARGV1})
    if(ARGV1)
      string(TOUPPER "_${VARIANT}" VARIANT)
    endif()
    set(CMAKE_EXE_LINKER_FLAGS${VARIANT} "${CMAKE_EXE_LINKER_FLAGS${VARIANT}} ${FLAG}" PARENT_SCOPE)
  endif()
endfunction()

function(add_required_cxx_linker_flag FLAG)
  mangle_linker_flag("${FLAG}" MANGLED_FLAG)
  set(OLD_CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS}")
  set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} ${FLAG}")
  check_linker_flag(CXX "${FLAG}" ${MANGLED_FLAG})
  set(CMAKE_REQUIRED_FLAGS "${OLD_CMAKE_REQUIRED_FLAGS}")
  if(${MANGLED_FLAG})
    set(VARIANT ${ARGV1})
    if(ARGV1)
      string(TOUPPER "_${VARIANT}" VARIANT)
    endif()
    set(CMAKE_EXE_LINKER_FLAGS${VARIANT} "${CMAKE_EXE_LINKER_FLAGS${VARIANT}} ${FLAG}" PARENT_SCOPE)
  else()
    message(FATAL_ERROR "Required flag '${FLAG}' is not supported by the linker")
  endif()
endfunction()
