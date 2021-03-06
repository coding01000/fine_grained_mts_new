CMAKE_MINIMUM_REQUIRED(VERSION 3.1)

PROJECT(fine_grained_mts)

include_directories(
        /usr/local/opt/mysql-client/include
        ./binlog/include
        ./event_unpack/include
        ./rpl/include
        ./thread_pool/include
        ./cfg/include
#        /usr/local/mysql-connector-c++/include/
        )

link_directories(/usr/local/opt/mysql-client/lib)

link_libraries(mysqlclient z)

#ADD_SUBDIRECTORY(./table)
#ADD_SUBDIRECTORY(./utils)
#ADD_SUBDIRECTORY(./thread_pool)
ADD_SUBDIRECTORY(./binlog)
ADD_SUBDIRECTORY(./event_unpack)
ADD_SUBDIRECTORY(./rpl)
ADD_SUBDIRECTORY(./thread_pool)
ADD_SUBDIRECTORY(./cfg)

ADD_EXECUTABLE(fine_grained_mts test.cc)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

TARGET_LINK_LIBRARIES(fine_grained_mts
        binlog_lib
        event_unpack_lib
        rpl_lib
        thread_pool_lib
        cfg_lib
        )