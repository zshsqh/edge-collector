cmake_minimum_required(VERSION 3.16)
project(Day10TCP LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
enable_testing()

add_executable(test_device_message
    tests/test_device_message.cpp
)
add_executable(center src/center.cpp)

target_include_directories(center
    PRIVATE include
)
target_include_directories(test_device_message
    PRIVATE include
)

add_test(
    NAME device_message_parser
    COMMAND test_device_message
)
find_package(SQLite3 REQUIRED)

add_executable(gateway
    src/gateway.cpp
    src/local_store.cpp
)

target_include_directories(gateway PRIVATE include)
find_package(SQLite3 REQUIRED)
target_link_libraries(center PRIVATE SQLite::SQLite3)
target_link_libraries(gateway PRIVATE SQLite::SQLite3)
target_include_directories(gateway PRIVATE include)
add_executable(device_sim src/device_sim.cpp)
target_include_directories(device_sim PRIVATE include)
