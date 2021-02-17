CMAKE_MINIMUM_REQUIRED(VERSION 3.1)

PROJECT(fine_grained_mts)

include_directories(./include)

ADD_SUBDIRECTORY(./table)

ADD_EXECUTABLE(fine_grained_mts main.cc)

TARGET_LINK_LIBRARIES(fine_grained_mts
        table_lib
        )