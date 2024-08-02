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

include(FetchContent)
FetchContent_Declare(
  json
  GIT_REPOSITORY https://github.com/nlohmann/json.git
  GIT_TAG v3.11.3  # Use the latest stable version or the one you need
)
FetchContent_MakeAvailable(json)

target_link_libraries(moqrelayserverak PRIVATE nlohmann_json::nlohmann_json)

install(
    TARGETS moqrelayserverak
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)
