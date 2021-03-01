#ifndef FINE_GRAINED_MTS_ROW_H
#define FINE_GRAINED_MTS_ROW_H
#include "vector"
#include <string>
#include <unordered_map>
#include <cstdarg>
#include <mutex>

namespace rpl{
    class Row{
    public:
        std::vector<std::string> columns;
        std::string db_name;
        std::string primary_key;
        std::string table_name;
        Row * next;
        uint64_t event_time; //todo：记住处理event_time相同的情况
        uint64_t now;
        uint64_t process_time;
        bool is_deleted;
    public:
        Row();
        ~Row();
        Row(std::string primary_key, uint64_t event_time, bool is_deleted, std::string db_name, std::string table_name);
    };

    class Hash_Header{
    public:
        Row * next;
        std::mutex mu;
    public:
        void push_row(Row *row);
        Hash_Header();
        ~Hash_Header();
    };
}

#endif //FINE_GRAINED_MTS_ROW_H
