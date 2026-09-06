# The SPSC shared-memory ring. Not header-only: mmap and the header validation
# are real code, and only the hot push/pop paths need to inline, which they do
# because they are templates on the caller's fill/visit.
add_library(mdstack_ring STATIC src/spsc.cpp)
target_include_directories(mdstack_ring PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(mdstack_ring PUBLIC mdstack_wire)

add_executable(ring_spsc_test tests/spsc_test.cpp)
target_link_libraries(ring_spsc_test PRIVATE mdstack_ring)
add_test(NAME ring_spsc COMMAND ring_spsc_test)

# Drives a ring from the C++ side so scripts/ring-interop-test.sh can put a Rust
# process on the other end of it. The layout agreement between the two is
# generated, but generated is not the same as checked.
add_executable(ring-poke src/poke_main.cpp)
target_link_libraries(ring-poke PRIVATE mdstack_ring)
