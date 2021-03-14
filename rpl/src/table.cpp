#include "../include/table.h"

namespace rpl{

    Table::Table(std::string table_name) {
        this->table_name = table_name;
    }

    Table::Table() {}

    Table::~Table() {}

    uint8_t Table::insert_row(Row *row) {
        auto it = this->rows.find(row->primary_key);
        if(it == this->rows._map.end()){
            Hash_Header *hash_header = new Hash_Header();
//            this->rows[row->primary_key] = hash_header;
            this->rows.insert(row->primary_key, hash_header);
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
        for (auto i=rows._map.begin();i!=rows._map.end();i++){
            std::cout<<i->first;
            if (i->second->next){
                std::cout<<i->second->next->columns[0]<<i->second->next->columns[1]<<i->second->next->columns[2];
            }
            std::cout<<std::endl;
        }
    }

    Row * Table::get(std::string primary_key, uint64_t time) {
        auto it = rows.find(primary_key);
        //如果没有找到或者最新的数据的时间戳小于当前时间，等待一段时间来来获取最新值
        if (it==rows.end()||it->second->next==NULL||it->second->next->event_time<time){
//            sleep(1);
        }
        it = rows.find(primary_key);
        if (it==rows.end()||it->second->next==NULL){
            return NULL;
        }
        Row *tmp = it->second->next;
        while (tmp&&tmp->event_time>time){
            tmp = tmp->next;
        }
        return tmp;
    }
}