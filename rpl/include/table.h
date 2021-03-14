#ifndef FINE_GRAINED_MTS_TABLE_H
#define FINE_GRAINED_MTS_TABLE_H
#include "row.h"
#include "table_schema.h"
#include "shared_mutex"
#include "safe_queue.h"
#include "safe_map.h"

namespace rpl{
    class Table{
    public:
        std::string table_name;
        std::string _pk;
        std::shared_mutex mu;
        binary_log::TableSchema *schema;
//        std::unordered_map<std::string, Hash_Header *> rows;
        SafeMap<std::string, Hash_Header *> rows;
    public:
        Table();
        ~Table();
        Table(std::string table_name);
        uint8_t insert_row(Row *row);
        Row* get(std::string primary_key, uint64_t time);
        void print_all();
    };
}


#endif //FINE_GRAINED_MTS_TABLE_H
