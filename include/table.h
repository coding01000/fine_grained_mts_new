#ifndef FINE_GRAINED_MTS_TABLE_H
#define FINE_GRAINED_MTS_TABLE_H
#include "vector"
#include <string>
#include <unordered_map>
#include <cstdarg>

class Column{
    uint8_t type;
    void * value;
};

class Row{
public:
    std::vector<void *> column_values;
    Row * next;
    uint64_t primary_ley;
    uint64_t event_time; //todo：记住处理event_time相同的情况
    bool is_deleted;
public:
    Row();
    Row(uint64_t primary_ley, std::vector<void *> column_values, uint64_t event_time, bool is_deleted);
    void print_all();
};

class Table{
public:
    std::string table_name;
    std::vector<std::string> column_names;
    std::vector<uint8_t> column_width;
    std::unordered_map<int, Row> rows;
public:
    Table();
    Table(std::string table_name, std::vector<std::string> column_names, std::vector<uint8_t> column_width);
    uint8_t insert_row(Row row);
    void print_all();
};


#endif
