#ifndef FINE_GRAINED_MTS_EVENT_HANDLE_H
#define FINE_GRAINED_MTS_EVENT_HANDLE_H
#include "table_schema.h"
#include "binary_log.h"
#include "mysql/mysql.h"
#include "unordered_map"

namespace binary_log{
    class Event_Handler{
    private:
        std::vector<std::string> unpack(Rows_event *ev, Event_reader &reader, TableSchema *table);
        MYSQL *mysql;
        std::unordered_map<uint64_t, TableSchema *> table_schemas;
    public:
        Event_Handler();
        std::vector<std::string> unpack(Write_rows_event *ev, Event_reader &reader, TableSchema *table);
        std::vector<std::string> unpack(Delete_rows_event *ev, Event_reader &reader, TableSchema *table);
        std::vector<std::string> unpack(Update_rows_event *ev, Event_reader &reader, TableSchema *table);
        TableSchema* unpack(Table_map_event *ev);
    };
}

#endif //FINE_GRAINED_MTS_EVENT_HANDLE_H
