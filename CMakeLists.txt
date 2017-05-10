cmake_minimum_required(VERSION 3.6)

set(CMAKE_CXX_FLAGS_RELEASE "-O2")
set(CMAKE_CXX_FLAGS_DEBUG "-g")

find_package(Boost)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads)

set(lib.include
    include
    tpie/build.release/install/include
    ${Boost_INCLUDE_DIRS})
set(lib.lib
    tpie
    boost_system
    boost_filesystem
    ${CMAKE_THREAD_LIBS_INIT})
set(lib.libdir
    tpie/build.release/install/lib
    ${Boost_LIBRARY_DIRS})

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
link_directories(${lib.libdir})

add_executable(bt_example src/bt_example.cpp)
target_include_directories(bt_example PRIVATE ${lib.include})
target_link_libraries(bt_example ${lib.lib})

add_executable(leapfrog src/leapfrog.cpp)
target_include_directories(leapfrog PRIVATE ${lib.include})
target_link_libraries(leapfrog ${lib.lib})
