CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
set(CMAKE_CXX_STANDARD 14)

PROJECT(fine_grained_mts)

#set(BOOST_ROOT "/usr/local/mysql-8.0.11/boost/boost_1_66_0")
#set(BOOST_LIBRARYDIR "/usr/lib64")
#find_package(Boost 1.66.0 COMPONENTS thread system regex date_time REQUIRED)


set(BOOST_ROOT "/usr/include/boost")
find_package(Boost 1.66.0 COMPONENTS thread system regex date_time REQUIRED)

include_directories(
        /usr/include
        /usr/local/mysql/include
        /home/wangdonghui/project/include
        /usr/local/mysql-8.0.11/boost/boost_1_66_0
        ./binlog/include
        ./event_unpack/include
        ./rpl/include
        ./thread_pool/include
        ./cfg/include

        #        /usr/local/mysql-connector-c++/include/
)
# 设置boost库的根路径
# 查找boost的功能模块

link_directories(/root/mysql-8.0.23/lib)

link_directories(/usr/lib64/mysql/)
link_directories(/usr/local/mysql-8.0.11/lib)
link_directories(/usr/lib64)
link_libraries(mysqlclient z)


#ADD_SUBDIRECTORY(./table)
#ADD_SUBDIRECTORY(./utils)
#ADD_SUBDIRECTORY(./thread_pool)
ADD_SUBDIRECTORY(./cfg)
ADD_SUBDIRECTORY(./binlog)
ADD_SUBDIRECTORY(./event_unpack)
ADD_SUBDIRECTORY(./rpl)
ADD_SUBDIRECTORY(./thread_pool)

ADD_EXECUTABLE(fine_grained_mts test.cc)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

TARGET_LINK_LIBRARIES(fine_grained_mts
        rpl_lib
        event_unpack_lib
        cfg_lib
        binlog_lib
        thread_pool_lib
        ${Boost_LIBRARIES}
        tcmalloc_minimal
        )
