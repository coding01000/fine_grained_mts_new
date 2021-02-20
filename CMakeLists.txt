CMAKE_MINIMUM_REQUIRED(VERSION 3.1)

PROJECT(fine_grained_mts)

include_directories(./include)

ADD_SUBDIRECTORY(./table)
ADD_SUBDIRECTORY(./utils)
ADD_SUBDIRECTORY(./thread_pool)

ADD_EXECUTABLE(fine_grained_mts main.cc)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

TARGET_LINK_LIBRARIES(fine_grained_mts
        table_lib
        utils_lib
        thread_pool_lib
        )