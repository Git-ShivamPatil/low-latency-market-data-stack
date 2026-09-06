# The pre-trade risk service.
#
# counting_alloc.cpp replaces the global operator new / delete. It is in this
# library rather than behind a build option on purpose: a build that swapped the
# allocator in for a measurement would be measuring a different program from the
# one that ships. Every binary that links mdstack_risk gets the counters, and the
# service reports its own count at shutdown -- which is what pulls the
# translation unit in and what makes the claim visible outside a test.
add_library(mdstack_risk STATIC
  src/limits.cpp
  src/counting_alloc.cpp
)
target_include_directories(mdstack_risk PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(mdstack_risk PUBLIC mdstack_wire)

add_executable(risk_limits_test tests/limits_test.cpp)
target_link_libraries(risk_limits_test PRIVATE mdstack_risk)
add_test(NAME risk_limits COMMAND risk_limits_test)

add_executable(risk_allocation_test tests/allocation_test.cpp)
target_link_libraries(risk_allocation_test PRIVATE mdstack_risk)
add_test(NAME risk_allocation COMMAND risk_allocation_test)

# The service itself. It reports its own steady-state allocation count at exit,
# which is both the claim made visible outside a test and what pulls
# counting_alloc.cpp into the link.
add_executable(risk-service src/service_main.cpp)
target_link_libraries(risk-service PRIVATE mdstack_risk mdstack_ring)
