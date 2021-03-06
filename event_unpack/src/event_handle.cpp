#include "../include/event_handle.h"

namespace binary_log{

    Event_Handler::Event_Handler() {
        mysql = mysql_init(nullptr);
        mysql->reconnect = 1;
        Master_info masterInfo;
        mysql = mysql_real_connect(mysql, masterInfo.host.c_str(), masterInfo.user_name.c_str(), masterInfo.pwd.c_str(),
                                   NULL, masterInfo.port, NULL, 0);
    }

    std::vector<std::string> Event_Handler::unpack(Rows_event *ev, Event_reader &reader,
                                                       TableSchema *table) {
        std::vector<std::string> row;
        int row_bit_len = (ev->m_width-1)/8+1;
        reader.ptr(row_bit_len);//reader的位置是bitmap，移动一个位置
        for(uint64_t i =0; i<ev->m_width;i++) {
            Field *column_field = NULL;
            std::string value;

            column_field = table->getFieldByIndex(i);
            if(column_field == NULL) {
                std::string errmsg = "getFieldByIndex is null";
                std::cout<<"unpackRow err:"<<errmsg;
            }
            //LOG(INFO)<<"i:"<<i<<" ,BitMap:"<<bitmap.isSet(i);

            value = column_field->valueString(reader);

            //avoid add field to row->columns twice
            row.push_back(value);
        }
        return row;
    }

    std::vector<std::string> Event_Handler::unpack(Write_rows_event *ev, Event_reader &reader,
                                                       TableSchema *table) {
        return unpack((Rows_event *)ev, reader, table);
    }

    std::vector<std::string> Event_Handler::unpack(Delete_rows_event *ev, Event_reader &reader,
                                                       TableSchema *table) {
        return unpack((Rows_event *)ev, reader, table);
    }

    std::vector<std::string> Event_Handler::unpack(Update_rows_event *ev, Event_reader &reader,
                                                       TableSchema *table) {
        unpack((Rows_event *)ev, reader, table);
        return unpack((Rows_event *)ev, reader, table);
    }

    TableSchema* Event_Handler::unpack(Table_map_event *ev) {
        std::unique_lock<std::mutex> lock(mu);
        uint64_t table_id = ev->get_table_id();
        std::string full_name = ev->get_db_name() + '.' + ev->get_table_name();
        auto it = table_schemas.find(table_id);
        if (it != table_schemas.end()) { //如table id果存在，并且存的表不一样，要进行更新
            std::string tmpSchemaDbTable = it->second->getDBname() + '.' + it->second->getTablename();
            if (full_name == tmpSchemaDbTable) {
                //table-id,db,tablen all same, skip
                return it->second;
            }
            delete it->second;
            table_schemas.erase(it);
        } else {
            for (auto it = table_schemas.begin(); it != table_schemas.end(); it++) {
                if (full_name == (it->second->getDBname() + '.' + it->second->getTablename())) {
                    delete it->second;
                    table_schemas.erase(it);
                    break;
                }
            }

        }
        char sql[1024];
        snprintf(sql, sizeof(sql), "SELECT COLUMN_NAME,COLUMN_TYPE,CHARACTER_OCTET_LENGTH,ORDINAL_POSITION,"
                                              " COLUMN_KEY FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '%s'"
                                              " AND TABLE_NAME = '%s' ORDER BY ORDINAL_POSITION;", ev->get_db_name().c_str(),
                                              ev->get_table_name().c_str());
//        std::cout<<sql<<std::endl;
        mysql_query(mysql, sql);
        MYSQL_RES *res = mysql_store_result(mysql);
        if (res == NULL) {
            std::cout<<"query table schema error, db:"<<ev->get_db_name().c_str()<<" ,table:"<<ev->get_table_name().c_str()<<" ,offset_:";
            return nullptr;
        }
        if(mysql_num_rows(res) == 0) {
            std::cout<<"query table schema res 0 rows";
            return nullptr;
        }
        //add new table schema to tables_
        std::pair<uint64_t, TableSchema*> kv;
        kv.first = ev->get_table_id();
        kv.second = new TableSchema(ev->get_db_name(), ev->get_table_name());
        //attention &
        std::string& prikey = kv.second->getPrikey();
        MYSQL_ROW row;
        int row_pos = 0;
        while((row = mysql_fetch_row(res))) {
            if(!kv.second->createField(row[0], row[1], row[2])) {
                delete kv.second;
                return nullptr;
            }
            //for mysql 5.6 or later
//            if(strcmp(row[1], "datetime") == 0) {
//                FieldDatetime::setColumnMeta((int)event.columnmeta[row_pos]);
//            }
            row_pos++;
            if(row[4] != NULL && strcmp(row[4], "PRI") == 0) {
                prikey=row[0];
            }
        }
        mysql_free_result(res);
        table_schemas.insert(kv);
        return kv.second;
    }

    TableSchema * Event_Handler::get_schema(uint64_t pk) {
        return table_schemas.find(pk)->second;
    }

}