#include "../include/event_handle.h"

namespace binary_log{

    Event_Handler::Event_Handler() {
        mysql = mysql_init(nullptr);
        mysql->reconnect = 1;
        Master_info masterInfo;
        mysql = mysql_real_connect(mysql, masterInfo.host.c_str(), masterInfo.user_name.c_str(), masterInfo.pwd.c_str(),
                                   NULL, masterInfo.port, NULL, 0);
    }

    uint8_t Event_Handler::init(std::unordered_map<std::string, rpl::Table *> *db, std::vector<std::string> tables) {
//        std::string tables[] = {"bmsql_customer", "bmsql_district", "bmsql_history", "bmsql_item", "bmsql_new_order", "bmsql_oorder", "bmsql_order_line", "bmsql_stock", "bmsql_warehouse"};
        char sql[1024];
        std::string db_name = "tpcc";
        for (const auto &table : tables) {
            snprintf(sql, sizeof(sql), "SELECT COLUMN_NAME,COLUMN_TYPE,CHARACTER_OCTET_LENGTH,ORDINAL_POSITION,"
                                       " COLUMN_KEY FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '%s'"
                                       " AND TABLE_NAME = '%s' ORDER BY ORDINAL_POSITION;", db_name.c_str(),
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
            auto table_schema = new TableSchema(db_name, table);
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
            schemas[db_name+"."+table] = table_schema;
            std::string full_table_name = db_name+"."+table;
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
        uint64_t table_id = table_id = ev->get_table_id();
        size_t hash_value = std::hash<uint64_t>()(table_id) % hash_range_;
        {
//            std::unique_lock<std::shared_timed_mutex> lock(schemas_mu);
//            std::unique_lock<std::mutex> lock(mu);
//            auto it = table_schemas.find(table_id);
//            if (it != table_schemas.end()) {
//                return it->second;
//            }
            auto it = atomic_schema[hash_value].load();
            if(it != nullptr) return (TableSchema*)(it);
        }
        std::string full_name = ev->get_db_name() + '.' + ev->get_table_name();
        auto t_s = schemas[full_name];
        {
//            std::unique_lock<std::shared_timed_mutex> lock(schemas_mu);
//            std::unique_lock<std::mutex> lock(mu);
//            table_schemas[table_id] = t_s;
            atomic_schema[hash_value].exchange(t_s);
        }
        return t_s;
    }

    TableSchema * Event_Handler::get_schema(uint64_t pk) {
//        return table_schemas.find(pk)->second;
//        std::shared_lock<std::shared_timed_mutex> lock(schemas_mu);
//        std::unique_lock<std::mutex> lock(mu);
//        return table_schemas[pk];
        size_t hash_value = std::hash<uint64_t>()(pk) % hash_range_;
        return (TableSchema *)atomic_schema[hash_value].load();
    }

}