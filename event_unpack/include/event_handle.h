#ifndef FINE_GRAINED_MTS_EVENT_HANDLE_H
#define FINE_GRAINED_MTS_EVENT_HANDLE_H
#include "table_schema.h"
#include "binary_log.h"
#include "mysql.h"
#include "unordered_map"
#include "master_info.h"
#include "mutex"
#include "table.h"

#include <shared_mutex>
#include <atomic>

namespace binary_log{
    class Event_Handler{
    private:
        std::vector<std::string> unpack(Rows_event *ev, Event_reader &reader, TableSchema *table);
        MYSQL *mysql;
        std::unordered_map<uint64_t, TableSchema *> table_schemas;

        std::atomic<void *> atomic_schema[1010];

        const int hash_range_ = 1010;

        std::unordered_map<std::string, TableSchema *> schemas;
//        std::shared_timed_mutex schemas_mu;

//        std::mutex mu;
    public:
        Event_Handler();
        uint8_t init(std::unordered_map<std::string, rpl::Table *> *db, std::vector<std::string> tables);
        std::vector<std::string> unpack(Write_rows_event *ev, Event_reader &reader, TableSchema *table);
        std::vector<std::string> unpack(Delete_rows_event *ev, Event_reader &reader, TableSchema *table);
        std::vector<std::string> unpack(Update_rows_event *ev, Event_reader &reader, TableSchema *table);
        TableSchema* unpack(Table_map_event *ev);
        TableSchema* get_schema(uint64_t pk);
    };
}

#endif //FINE_GRAINED_MTS_EVENT_HANDLE_H
