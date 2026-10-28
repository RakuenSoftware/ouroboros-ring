cmake_minimum_required(VERSION 3.16)
file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/../VERSION" OUROBOROS_RING_VERSION LIMIT_COUNT 1)
project(ouroboros_ring VERSION ${OUROBOROS_RING_VERSION} LANGUAGES C)

# The C reference implementation of the ouroboros ring: the host (the side that
# owns regions, admits clients and routes) and the client, in one place so a
# change to either is made and tested against the other.
#
#   ouroboros-ring         host + client + module runtime
#   ouroboros-ring-client  what an external module process links: attach, wire,
#                          region, ring and the module protocol. No host,
#                          routing, arena allocator or capture code.

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CTest)

if(NOT UNIX OR APPLE)
    message(FATAL_ERROR "The ouroboros ring C implementation is POSIX shared memory with Linux "
                        "memfd/SCM_RIGHTS admission; this platform is not supported")
endif()

set(OURO_WARNINGS -Wall -Wextra -Werror -Wformat=2 -Wno-unused-parameter)
# GCC 15.2's IPA bit-CP pass can prove a live int parameter constant zero
# (observed in a consumer built -Os -flto: a count > 0 arrived as 0). The ring's
# own build turns the pass off for GCC so no consumer has to know to; clang has
# no such pass.
if(CMAKE_C_COMPILER_ID STREQUAL "GNU")
    add_compile_options(-fno-ipa-bit-cp)
endif()
find_package(Threads REQUIRED)

set(OURO_CLIENT_SOURCES
    src/attach.c
    src/client.c
    src/endpoint.c
    src/module_client.c
    src/module_protocol.c
    src/module_runtime.c
    src/region.c
    src/ring.c
    src/wire.c)
set(OURO_HOST_SOURCES
    src/arena.c
    src/capture.c
    src/host.c
    src/local_attach.c
    src/region_host.c
    src/route.c
    src/runtime.c)

foreach(lib ouroboros-ring-client ouroboros-ring)
    if(lib STREQUAL "ouroboros-ring")
        add_library(${lib} STATIC ${OURO_CLIENT_SOURCES} ${OURO_HOST_SOURCES})
    else()
        add_library(${lib} STATIC ${OURO_CLIENT_SOURCES})
    endif()
    target_compile_features(${lib} PUBLIC c_std_11)
    target_compile_options(${lib} PRIVATE ${OURO_WARNINGS})
    target_include_directories(${lib}
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
               $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    target_link_libraries(${lib} PUBLIC Threads::Threads)
endforeach()

install(TARGETS ouroboros-ring ouroboros-ring-client EXPORT ouroboros-ring-targets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT ouroboros-ring-targets NAMESPACE ouroboros::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ouroboros-ring)
configure_package_config_file(cmake/ouroboros-ring-config.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/ouroboros-ring-config.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ouroboros-ring)
write_basic_package_version_file(${CMAKE_CURRENT_BINARY_DIR}/ouroboros-ring-config-version.cmake
    VERSION ${PROJECT_VERSION} COMPATIBILITY SameMajorVersion)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/ouroboros-ring-config.cmake
              ${CMAKE_CURRENT_BINARY_DIR}/ouroboros-ring-config-version.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ouroboros-ring)
foreach(pc ouroboros-ring ouroboros-ring-client)
    configure_file(${pc}.pc.in ${CMAKE_CURRENT_BINARY_DIR}/${pc}.pc @ONLY)
    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${pc}.pc
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)
endforeach()

if(BUILD_TESTING)
    # Each test links only what it exercises, so a test cannot pass on code it
    # did not mean to depend on.
    set(OURO_ALL_SOURCES ${OURO_HOST_SOURCES} ${OURO_CLIENT_SOURCES})
    set(OURO_TEST_ring src/ring.c)
    set(OURO_TEST_wire src/wire.c)
    set(OURO_TEST_endpoint src/endpoint.c)
    set(OURO_TEST_module_protocol src/module_protocol.c)
    set(OURO_TEST_layout src/region.c src/ring.c src/module_protocol.c)
    set(OURO_TEST_region src/region.c src/region_host.c src/ring.c)
    set(OURO_TEST_arena src/arena.c src/region.c src/ring.c)
    foreach(name host route flow client capture local_attach runtime module_embedded_runtime)
        set(OURO_TEST_${name} ${OURO_ALL_SOURCES})
    endforeach()
    foreach(name ring wire endpoint module_protocol layout region arena host route flow client
                 capture local_attach runtime module_embedded_runtime)
        add_executable(test_${name} tests/test_${name}.c ${OURO_TEST_${name}})
        target_compile_features(test_${name} PRIVATE c_std_11)
        target_compile_options(test_${name} PRIVATE ${OURO_WARNINGS})
        target_include_directories(test_${name} PRIVATE include src tests)
        target_compile_definitions(test_${name} PRIVATE
            OURO_VECTOR_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../vectors")
        target_link_libraries(test_${name} PRIVATE Threads::Threads)
        add_test(NAME ${name} COMMAND test_${name})
    endforeach()

    # The C host other implementations are proven against: it serves the
    # conformance scenarios over a real socket and real shared memory, and the
    # Go and Rust suites drive it (OURO_CONFORMANCE_HOST).
    add_executable(ouroboros-ring-conformance-host conformance/host.c ${OURO_ALL_SOURCES})
    target_compile_features(ouroboros-ring-conformance-host PRIVATE c_std_11)
    target_compile_options(ouroboros-ring-conformance-host PRIVATE ${OURO_WARNINGS})
    target_include_directories(ouroboros-ring-conformance-host PRIVATE include src)
    target_link_libraries(ouroboros-ring-conformance-host PRIVATE Threads::Threads)

    # The module process runtime end to end. With no argument it is a C caller
    # against an in-process C module (a test); given a module binary, or an
    # external caller in OURO_TEST_MODULE_CALLER, it drives another
    # implementation's module helper or caller to the same outcomes.
    add_executable(ouroboros-ring-module-runtime-harness conformance/module_runtime.c
                   ${OURO_ALL_SOURCES})
    target_compile_features(ouroboros-ring-module-runtime-harness PRIVATE c_std_11)
    target_compile_options(ouroboros-ring-module-runtime-harness PRIVATE ${OURO_WARNINGS})
    target_include_directories(ouroboros-ring-module-runtime-harness PRIVATE include src tests)
    target_link_libraries(ouroboros-ring-module-runtime-harness PRIVATE Threads::Threads)
    add_test(NAME module_runtime COMMAND ouroboros-ring-module-runtime-harness)

    # The dispatch-overhead benchmark scripts/check_perf_budget.sh holds to the
    # ceiling in bench/baseline.json. Always optimized, whatever the build type,
    # so the number means the same thing in every build that produces it.
    add_executable(ouroboros-ring-dispatch-bench bench/dispatch.c ${OURO_ALL_SOURCES})
    target_compile_features(ouroboros-ring-dispatch-bench PRIVATE c_std_11)
    target_compile_options(ouroboros-ring-dispatch-bench PRIVATE ${OURO_WARNINGS} -O2)
    target_include_directories(ouroboros-ring-dispatch-bench PRIVATE include src)
    target_link_libraries(ouroboros-ring-dispatch-bench PRIVATE Threads::Threads)

    # The arena's lock discipline under ThreadSanitizer: its own build, because
    # TSan cannot share objects with the address/undefined sanitizer builds.
    option(OURO_TSAN "Build and run the arena ThreadSanitizer test" OFF)
    if(OURO_TSAN)
        add_executable(test_arena_tsan tests/test_arena_tsan.c ${OURO_ALL_SOURCES})
        target_compile_options(test_arena_tsan PRIVATE ${OURO_WARNINGS} -fsanitize=thread -O1 -g)
        target_link_options(test_arena_tsan PRIVATE -fsanitize=thread)
        target_include_directories(test_arena_tsan PRIVATE include src tests)
        target_link_libraries(test_arena_tsan PRIVATE Threads::Threads)
        # TSan's shadow layout collides with high-entropy ASLR on some kernels
        # ("unexpected memory mapping"); setarch -R turns ASLR off for the run and
        # changes nothing the detector checks.
        find_program(OURO_SETARCH setarch)
        if(OURO_SETARCH)
            add_test(NAME arena_tsan
                COMMAND ${OURO_SETARCH} ${CMAKE_SYSTEM_PROCESSOR} -R $<TARGET_FILE:test_arena_tsan>)
        else()
            add_test(NAME arena_tsan COMMAND test_arena_tsan)
        endif()
    endif()
endif()
