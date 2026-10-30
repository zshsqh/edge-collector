
cmake_minimum_required(VERSION 3.16)

project(edge_collector LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(SQLite3 REQUIRED)
find_package(Threads REQUIRED)

enable_testing()

# 设备模拟器
add_executable(device_sim
    src/device_sim.cpp
)
target_include_directories(device_sim PRIVATE include)

# 网关
add_executable(gateway
    src/gateway.cpp
    src/local_store.cpp
)
target_include_directories(gateway PRIVATE include)
target_link_libraries(gateway PRIVATE
    SQLite::SQLite3
    Threads::Threads
)

# 中心服务
add_executable(center
    src/center.cpp
)
target_include_directories(center PRIVATE include)
target_link_libraries(center PRIVATE
    SQLite::SQLite3
    Threads::Threads
)

# 消息解析单元测试
add_executable(test_device_message
    tests/test_device_message.cpp
)
target_include_directories(test_device_message PRIVATE include)

add_test(
    NAME device_message_parser
    COMMAND test_device_message
)

