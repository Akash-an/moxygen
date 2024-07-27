# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Relay
add_library(moqrelayak MoQRelayAk.cpp)
target_include_directories(
  moqrelayak PUBLIC
  $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
  moqrelayak PRIVATE
  ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(
  moqrelayak PUBLIC
  Folly::folly
  wangle::wangle
  proxygen::proxygen
  proxygen::proxygenhqserver
  moxygen
)

install(
    TARGETS moqrelayak
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)

set(CMAKE_BUILD_TYPE Debug)

# MoQRelayServer
add_executable(
  moqrelayserverak
  MoQRelayServerAk.cpp
)
set_target_properties(
  moqrelayserverak
  PROPERTIES
    BUILD_RPATH ${DEPS_LIBRARIES_DIR}
    INSTALL_RPATH ${DEPS_LIBRARIES_DIR}
)
target_include_directories(
  moqrelayserverak PUBLIC
  $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
  moqrelayserverak PRIVATE
  ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(
  moqrelayserverak PUBLIC
  Folly::folly
  moqrelayak
)

install(
    TARGETS moqrelayserverak
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)
