#include "../include/table.h"

Row::Row(uint64_t primary_ley, std::vector<void *> column_values, uint64_t event_time, bool is_deleted) {
    this->primary_ley = primary_ley;
    this->column_values = column_values;
    this->event_time = event_time;
    this->is_deleted = is_deleted;
    this->next = NULL;
}

Row::Row() {}

//void Row::print_all() {
//    printf("column value: ");
//    {
//        printf("%")
//    }
//}

Table::Table(std::string table_name, std::vector<std::string> column_names, std::vector<uint8_t> column_width) {
    this->table_name = table_name;
    this->column_names = column_names;
    this->column_width = column_width;
}

Table::Table() {}

uint8_t Table::insert_row(Row row) {
    std::unordered_map<int, Row>::iterator it = this->rows.find(row.primary_ley);
    if(it == this->rows.end()){
        printf("xxxxxxxxxxxxxx%d", row.primary_ley);
        this->rows[row.primary_ley] = row;
    }else{
        printf("aaaaaa %s aaaaaa%d",(uint8_t*)it->second.column_values[1], row.primary_ley);
        row.next = &it->second;
        it->second = row;
    }
}