#include "../include/event_handle.h"

namespace binary_log{

    Event_Handler::Event_Handler() {
        mysql = mysql_init(nullptr);
        mysql->reconnect = 1;
        Master_info masterInfo;
        mysql = mysql_real_connect(mysql, masterInfo.host.c_str(), masterInfo.user_name.c_str(), masterInfo.pwd.c_str(),
                                   NULL, masterInfo.port, NULL, 0);
    }

    uint8_t Event_Handler::init(std::unordered_map<std::string, rpl::Table *> *db) {
        std::string tables[] = {"CUSTOMER", "DISTRICT", "HISTORY", "ITEM", "NEW_ORDER", "OORDER", "ORDER_LINE", "STOCK", "WAREHOUSE"};
        char sql[1024];
        for (const auto &table : tables) {
            snprintf(sql, sizeof(sql), "SELECT COLUMN_NAME,COLUMN_TYPE,CHARACTER_OCTET_LENGTH,ORDINAL_POSITION,"
                                       " COLUMN_KEY FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '%s'"
                                       " AND TABLE_NAME = '%s' ORDER BY ORDINAL_POSITION;", "tpcc",
                     table.c_str());
            mysql_query(mysql, sql);
            MYSQL_RES *res = mysql_store_result(mysql);
            if (res == NULL) {
                std::cout<<"query table schema error, db:"<<"tpcc"<<" ,table:"<<table<<" ,offset_:";
                return 2;
            }
            if(mysql_num_rows(res) == 0) {
                std::cout<<"query table schema res 0 rows";
                return 1;
            }
            auto table_schema = new TableSchema("tpcc", table);
            std::string& prikey = table_schema->getPrikey();
            MYSQL_ROW row;
            while((row = mysql_fetch_row(res))) {
                if(!table_schema->createField(row[0], row[1], row[2])) {
                    return 3;
                }
                if(row[4] != NULL && strcmp(row[4], "PRI") == 0) {
                    prikey=row[0];
                }
            }
            mysql_free_result(res);
            schemas["tpcc."+table] = table_schema;
            std::string full_table_name = "tpcc."+table;
            rpl::Table *ta = new rpl::Table(full_table_name);
            ta->schema = new binary_log::TableSchema(*table_schema);
            ta->_pk = table_schema->getPrikey();
            (*db)[full_table_name] = ta;
        }
        return 0;
    }

    std::vector<std::string> Event_Handler::unpack(Rows_event *ev, Event_reader &reader,
                                                       TableSchema *table) {
        std::vector<std::string> row;
//        int row_bit_len = (ev->m_width-1)/8+1;
        int row_bit_len = ev->n_bits_len;
        const char *ptr = reader.ptr(row_bit_len);//reader的位置是bitmap，移动一个位置
        for(uint64_t i =0; i<ev->m_width;i++) {

            if ((uint8_t(1<<i))&(ptr[i/8]))
                continue;
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
    int cnt = 0;
    TableSchema* Event_Handler::unpack(Table_map_event *ev) {
//        uint64_t table_id = ev->get_table_id();
        auto table_id = ev->get_table_id();
        auto it = table_schemas.find(table_id);
        if (it!=table_schemas.end()){
            return it->second;
        }
        {
            std::unique_lock<std::mutex> lock(mu);
            std::string full_name = ev->get_db_name() + '.' + ev->get_table_name();
            auto t_s = schemas[full_name];
            table_schemas[table_id] = t_s;
            return t_s;
        }

//        auto it = table_schemas.find(table_id);
//        if (it != table_schemas.end()) { //如table id果存在，并且存的表不一样，要进行更新
//            std::string tmpSchemaDbTable = it->second->getDBname() + '.' + it->second->getTablename();
//            if (full_name == tmpSchemaDbTable) {
//                //table-id,db,tablen all same, skip
//                return it->second;
//            }
////            delete it->second;
//            table_schemas.erase(it);
//        } else {
////            for (auto it = table_schemas.begin(); it != table_schemas.end(); it++) {
////                if (full_name == (it->second->getDBname() + '.' + it->second->getTablename())) {
////                    delete it->second;
////                    table_schemas.erase(it);
////                    break;
////                }
////            }
//
//        }
//        char sql[1024];
//        snprintf(sql, sizeof(sql), "SELECT COLUMN_NAME,COLUMN_TYPE,CHARACTER_OCTET_LENGTH,ORDINAL_POSITION,"
//                                              " COLUMN_KEY FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '%s'"
//                                              " AND TABLE_NAME = '%s' ORDER BY ORDINAL_POSITION;", ev->get_db_name().c_str(),
//                                              ev->get_table_name().c_str());
////        std::cout<<sql<<std::endl;
//        mysql_query(mysql, sql);
//        MYSQL_RES *res = mysql_store_result(mysql);
////        lock.unlock();
//        if (res == NULL) {
//            std::cout<<"query table schema error, db:"<<ev->get_db_name().c_str()<<" ,table:"<<ev->get_table_name().c_str()<<" ,offset_:";
//            return nullptr;
//        }
//        if(mysql_num_rows(res) == 0) {
//            std::cout<<"query table schema res 0 rows";
//            return nullptr;
//        }
//        //add new table schema to tables_
////        std::cout << ++cnt<<std::endl;
//        std::pair<uint64_t, TableSchema*> kv;
//        kv.first = ev->get_table_id();
//        kv.second = schemas[full_name];
//        table_schemas.insert(kv);
//        return kv.second;
//        kv.second = new TableSchema(ev->get_db_name(), ev->get_table_name());
//        //attention &
//        std::string& prikey = kv.second->getPrikey();
//        MYSQL_ROW row;
//        int row_pos = 0;
//        while((row = mysql_fetch_row(res))) {
//            if(!kv.second->createField(row[0], row[1], row[2])) {
////                delete kv.second;
//                return nullptr;
//            }
//            //for mysql 5.6 or later
////            if(strcmp(row[1], "datetime") == 0) {
////                FieldDatetime::setColumnMeta((int)event.columnmeta[row_pos]);
////            }
//            row_pos++;
//            if(row[4] != NULL && strcmp(row[4], "PRI") == 0) {
//                prikey=row[0];
//            }
//        }
//        mysql_free_result(res);
//        table_schemas.insert(kv);
//        return kv.second;
    }

    TableSchema * Event_Handler::get_schema(uint64_t pk) {
        return table_schemas.find(pk)->second;
    }

}