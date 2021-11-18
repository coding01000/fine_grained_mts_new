# install
The project uses [Boost](https://www.boost.org/) and mysql-devel. Go check them out if you don't have them locally installed.

Here we give the way to install boost and mysql-devel in centos。
```shell
yum install boost
yum install boost-devel
yum install mysql-devel
```
# Introduction

Here is an example on how to use it.

First, fill in the directory of your installation with the addresses of mysql-devel and boost in the CMakeLists.txt file.

```cmake
set(BOOST_ROOT "/usr/include/boost")
find_package(Boost 1.66.0 COMPONENTS thread system regex date_time REQUIRED)
include_directories(/usr/local/mysql/include)
```

Then compile the project：

```shell
mkdir build
cd build
cmake ..
make -j 32
```

Specify the ip address, port number, user name and password of the database in the configuration file master_cfg.xml.

```xml
<?xml version="1.0" encoding="utf-8" ?>
<master_info>
    <user_name>username</user_name>
    <password>pwd</password>
    <host>ip</host>
    <port>port</port>
</master_info>
```

Set which tables and the number of threads for each group in the configuration file rpl_cfg.xml, the address of the binlog file can be specified in the case of offline, and the real-time synchronization depends on the setting of the file master_cfg.xml.

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<rpl_cfg>
    <is_remote>false</is_remote>
    <is_single_group>false</is_single_group>
    <mysql_mode>false</mysql_mode>
    <query_freqs>
        <query_ferq>
            <freq>700</freq>
            <query_tables>tpcc.bmsql_oorder</query_tables>
        </query_ferq>
    </query_freqs>
    <interval>500</interval>
    <files>
        <file>/root/project/mts/mysql-bin.000209</file>
        <file>/root/project/mts/mysql-bin.000210</file>
        <file>/root/project/mts/mysql-bin.000211</file>
    </files>
    <parse_pool>8</parse_pool>
    <group>
        <group_num>1</group_num>
        <group_cfg>
            <pool>5</pool>
            <group_table>tpcc.bmsql_customer</group_table>
            <group_table>tpcc.bmsql_oorder</group_table>
        </group_cfg>

    </group>
    <tables>
        <table>bmsql_oorder</table>
        <table>bmsql_customer</table>
        <table>bmsql_district</table>
        <table>bmsql_stock</table>
        <table>bmsql_order_line</table>
        <table>bmsql_warehouse</table>
        <table>bmsql_item</table>
        <table>bmsql_new_order</table>
        <table>bmsql_history</table>
    </tables>
</rpl_cfg>

```

The arrival rate of simulated OLAP queries can also be specified in the configuration file.

After configuration, you can directly execute group_base_log_replay for repaly.

```shell
./group_base_log_replay
```

# Directory description

binlog, event_unpack -- Parsing logs.

cfg -- Parsing xml files.

mysql_mts -- Replay method of mysql8.

rpl -- Replay method of group_base_log_replay

thread_pool -- Manage thread pool.
