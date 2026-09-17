# Replica of PR #1806 packaging rules at 115329b81a (not a Sirius build).
cmake_minimum_required(VERSION 3.30.4 FATAL_ERROR)

project(
  sirius
  VERSION 0.0.0
  LANGUAGES CXX)

include(GNUInstallDirs)

add_library(sirius_shared SHARED src/sirius.cpp)
add_library(sirius::sirius ALIAS sirius_shared)
set_target_properties(
  sirius_shared
  PROPERTIES OUTPUT_NAME sirius
             EXPORT_NAME sirius
             VERSION "${PROJECT_VERSION}"
             SOVERSION 0
             CXX_STANDARD 20
             CXX_STANDARD_REQUIRED ON)
target_include_directories(
  sirius_shared PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                       $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_compile_features(sirius_shared PUBLIC cxx_std_20)

install(
  DIRECTORY include/sirius
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
  COMPONENT sirius_library)

include(CMakePackageConfigHelpers)
configure_package_config_file(
  cmake/sirius-config.cmake.in "${CMAKE_CURRENT_BINARY_DIR}/sirius-config.cmake"
  INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/sirius")
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/sirius-config-version.cmake"
  VERSION "${PROJECT_VERSION}"
  COMPATIBILITY SameMinorVersion)

install(
  TARGETS sirius_shared
  EXPORT sirius-targets
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT sirius_library
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT sirius_library
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT sirius_library)
install(
  EXPORT sirius-targets
  FILE sirius-targets.cmake
  NAMESPACE sirius::
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/sirius"
  COMPONENT sirius_library)
install(
  FILES "${CMAKE_CURRENT_BINARY_DIR}/sirius-config.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/sirius-config-version.cmake"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/sirius"
  COMPONENT sirius_library)
