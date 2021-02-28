#include "../include/table.h"

namespace rpl{

    Table::Table(std::string table_name) {
        this->table_name = table_name;
    }

    Table::Table() {}

    Table::~Table() {}

    uint8_t Table::insert_row(Row *row) {
        auto it = this->rows.find(row->primary_key);
        if(it == this->rows.end()){
            Hash_Header *hash_header = new Hash_Header();
            this->rows[row->primary_key] = hash_header;
            hash_header->push_row(row);
        }else{
            //如果是同一个时间戳的，那么就替换，不用插入
            it->second->push_row(row);
//        if (row->event_time==it->second->event_time){
//            it->second = row;
//        }else{
//            row->next = it->second;
//            it->second = row;
//        }
        }
        return 0;
    }
    void Table::print_all() {
        for (auto i=rows.begin();i!=rows.end();i++){
            std::cout<<i->first;
            if (i->second->next){
                std::cout<<i->second->next->columns[0]<<i->second->next->columns[1]<<i->second->next->columns[2];
            }
            std::cout<<std::endl;
        }
    }
}