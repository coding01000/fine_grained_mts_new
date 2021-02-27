#ifndef FINE_GRAINED_MTS_TABLE_H
#define FINE_GRAINED_MTS_TABLE_H
#include "row.h"
#include "table_schema.h"

namespace rpl{
    class Table{
    public:
        std::string table_name;
        std::string _pk;
        binary_log::TableSchema *schema;
        std::unordered_map<std::string, Hash_Header *> rows;
    public:
        Table();
        ~Table();
        Table(std::string table_name);
        uint8_t insert_row(Row *row);
        void print_all();
    };
}


#endif //FINE_GRAINED_MTS_TABLE_H
