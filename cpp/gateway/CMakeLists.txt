# The FIX gateway. Split library and tests the same way `wire` is, but compiled
# rather than header-only: the session layer owns I/O and durable state, and
# neither belongs in a header.
add_library(mdstack_fix STATIC
  src/message.cpp
  src/seqstore.cpp
)
target_include_directories(mdstack_fix PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_executable(fix_message_test tests/message_test.cpp)
target_link_libraries(fix_message_test PRIVATE mdstack_fix)
add_test(NAME fix_message COMMAND fix_message_test)

add_executable(fix_seqstore_test tests/seqstore_test.cpp)
target_link_libraries(fix_seqstore_test PRIVATE mdstack_fix)
add_test(NAME fix_seqstore COMMAND fix_seqstore_test)
