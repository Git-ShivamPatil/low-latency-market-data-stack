# The FIX gateway. Split library and tests the same way `wire` is, but compiled
# rather than header-only: the session layer owns I/O and durable state, and
# neither belongs in a header.
add_library(mdstack_fix STATIC
  src/message.cpp
  src/seqstore.cpp
  src/orderstore.cpp
  src/orderpath.cpp
  src/session.cpp
  src/transport.cpp
)
target_include_directories(mdstack_fix PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
# orderstore.hpp names Side, which is generated.
# orderpath.hpp names the ring as well as the generated codec.
target_link_libraries(mdstack_fix PUBLIC mdstack_wire mdstack_ring)

# The binary the kill-restart test SIGKILLs.
add_executable(fix-gateway src/gateway_main.cpp)
target_link_libraries(fix-gateway PRIVATE mdstack_fix)

add_executable(fix_message_test tests/message_test.cpp)
target_link_libraries(fix_message_test PRIVATE mdstack_fix)
add_test(NAME fix_message COMMAND fix_message_test)

add_executable(fix_seqstore_test tests/seqstore_test.cpp)
target_link_libraries(fix_seqstore_test PRIVATE mdstack_fix)
add_test(NAME fix_seqstore COMMAND fix_seqstore_test)

add_executable(fix_orderstore_test tests/orderstore_test.cpp)
target_link_libraries(fix_orderstore_test PRIVATE mdstack_fix)
add_test(NAME fix_orderstore COMMAND fix_orderstore_test)

add_executable(fix_session_test tests/session_test.cpp)
target_link_libraries(fix_session_test PRIVATE mdstack_fix)
add_test(NAME fix_session COMMAND fix_session_test)

# Operational tooling: read or set a session's persisted sequence numbers.
add_executable(fix-seqtool src/seqtool_main.cpp)
target_link_libraries(fix-seqtool PRIVATE mdstack_fix)

# --- the independent implementation ----------------------------------------
#
# Optional on purpose. QuickFIX is the judge for milestone 7's interop check,
# but a stranger cloning this repository should not need it installed to build
# everything else. When it is absent the target simply does not exist, and
# scripts/quickfix-interop-test.sh says so rather than failing obscurely.
find_path(QUICKFIX_INCLUDE_DIR quickfix/Application.h)
find_library(QUICKFIX_LIBRARY NAMES quickfix)

if(QUICKFIX_INCLUDE_DIR AND QUICKFIX_LIBRARY)
  message(STATUS "QuickFIX found; building the interop acceptor")
  add_executable(quickfix-acceptor interop/quickfix_acceptor.cpp)
  target_include_directories(quickfix-acceptor SYSTEM PRIVATE ${QUICKFIX_INCLUDE_DIR})
  target_link_libraries(quickfix-acceptor PRIVATE ${QUICKFIX_LIBRARY} pthread)

  # C++14, alone in this repository. QuickFIX 1.15 uses dynamic exception
  # specifications, which C++17 removed and C++20 rejects. Isolating it here is
  # what lets the rest of the tree stay C++20 -- and keeping the judge in its
  # own target with its own standard is the right shape anyway, because it
  # shares no code with the thing it judges.
  set_target_properties(quickfix-acceptor PROPERTIES CXX_STANDARD 14 CXX_STANDARD_REQUIRED ON)

  # The project's warning set is deliberately strict and QuickFIX's headers do
  # not survive it. They are third-party and included as SYSTEM above; the
  # remaining pragmatism is switching -Werror off for this one target rather
  # than weakening the flags everything else is held to.
  target_compile_options(quickfix-acceptor PRIVATE
    -Wno-error -Wno-deprecated -Wno-conversion -Wno-sign-conversion -Wno-shadow
  )
else()
  message(STATUS "QuickFIX not found; the interop acceptor will not be built. "
                 "Install libquickfix-dev to run scripts/quickfix-interop-test.sh")
endif()
