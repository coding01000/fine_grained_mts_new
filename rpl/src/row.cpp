#include "../include/row.h"

namespace rpl{

    Row::Row() {}

    Row::Row(std::string primary_key, uint64_t event_time, bool is_deleted, std::string db_name,
             std::string table_name) {
        this->event_time = event_time;
        this->is_deleted = is_deleted;
        this->primary_key = primary_key;
        this->db_name = db_name;
        this->table_name = table_name;
        this->next = NULL;
    }

    Row::~Row() {

    }

    Hash_Header::Hash_Header() {
        this->next = NULL;
    }

    void Hash_Header::push_row(Row *row) {
        std::unique_lock<std::mutex> lock(mu);

        Row * header = this->next;
        if (header && (row->event_time==header->event_time)){
            row->next = header->next;
            this->next = row;
            delete header;
        }else{
            row->next = header;
            this->next = row;
        }
    }
}