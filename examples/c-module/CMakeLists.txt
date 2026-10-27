cmake_minimum_required(VERSION 3.16)
project(ouroboros_ring_module_example LANGUAGES C)

find_package(ouroboros-ring 0.1 CONFIG REQUIRED)
add_executable(ouroboros-ring-module-example main.c)
target_compile_features(ouroboros-ring-module-example PRIVATE c_std_11)
target_link_libraries(ouroboros-ring-module-example PRIVATE ouroboros::ouroboros-ring-client)
