cmake_minimum_required (VERSION 2.8.11)
project (clerk C)

include_directories(
  "${PROJECT_SOURCE_DIR}"
  "${PROJECT_BINARY_DIR}")

add_subdirectory (cle_core)
add_subdirectory (test_clerk)
