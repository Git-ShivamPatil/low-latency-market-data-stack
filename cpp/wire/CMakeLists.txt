# Header-only: the codec is generated into include/wire/generated.hpp and every
# accessor is meant to inline into the caller.
add_library(mdstack_wire INTERFACE)
target_include_directories(mdstack_wire INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# The golden vectors live outside the C++ tree because both language suites read
# the same files. MDSTACK_GOLDEN_DIR in the environment overrides this, which is
# what scripts/verify-golden-corruption.sh uses to point the suite at a
# deliberately damaged copy.
set(MDSTACK_GOLDEN_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../schema/golden")
cmake_path(NORMAL_PATH MDSTACK_GOLDEN_DIR)

add_executable(wire_golden_test tests/golden_test.cpp)
target_link_libraries(wire_golden_test PRIVATE mdstack_wire)
target_include_directories(wire_golden_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
target_compile_definitions(wire_golden_test PRIVATE
  MDSTACK_DEFAULT_GOLDEN_DIR="${MDSTACK_GOLDEN_DIR}"
)

add_test(NAME wire_golden COMMAND wire_golden_test)

add_executable(wire_layout_test tests/layout_test.cpp)
target_link_libraries(wire_layout_test PRIVATE mdstack_wire)
target_include_directories(wire_layout_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)

add_test(NAME wire_layout COMMAND wire_layout_test)
