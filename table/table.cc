#include "../include/table.h"

Row::Row(uint64_t primary_ley, std::vector<void *> column_values, uint64_t event_time, bool is_deleted) {
    this->primary_ley = primary_ley;
    this->column_values = column_values;
    this->event_time = event_time;
    this->is_deleted = is_deleted;
    this->next = NULL;
}

Row::Row() {
    this->next = NULL;
}

Row::~Row() {
    for (int i=0;i<column_values.size();i++){
        delete column_values[i];
    }
}

//void Row::print_all() {
//    printf("column value: ");
//    {
//        printf("%")
//    }
//}

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

Table::Table(std::string table_name, std::vector<std::string> column_names, std::vector<uint8_t> column_width) {
    this->table_name = table_name;
    this->column_names = column_names;
    this->column_width = column_width;
}

Table::Table() {}

Table::~Table() {}

uint8_t Table::insert_row(Row *row) {
    std::unordered_map<int, Hash_Header *>::iterator it = this->rows.find(row->primary_ley);
    if(it == this->rows.end()){
        Hash_Header *hash_header = new Hash_Header();
        this->rows[row->primary_ley] = hash_header;
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