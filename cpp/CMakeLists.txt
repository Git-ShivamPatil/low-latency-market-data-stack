cmake_minimum_required(VERSION 3.25)
project(mdstack_cpp LANGUAGES CXX)

# C++20 is a hard requirement, not a preference: std::endian and std::byte
# arithmetic are load-bearing in the generated codec.
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "" FORCE)
endif()

add_compile_options(
  -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion -Wshadow -Werror
)

enable_testing()

add_subdirectory(wire)
