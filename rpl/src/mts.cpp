#include "../include/mts.h"
#include "event_handle.h"
#include <functional>
#include "ctime"
#include "chrono"

namespace rpl{
    std::chrono::system_clock::time_point time_point_now = std::chrono::system_clock::now(); // 获取当前时间点
    std::chrono::system_clock::duration duration_since_epoch
            = time_point_now.time_since_epoch(); // 从1970-01-01 00:00:00到当前时间点的时长
    time_t pretime
            = std::chrono::duration_cast<std::chrono::microseconds>(duration_since_epoch).count(); // 将时长转换为微秒数

    int MTS_Handler::init() {
        pool = new ThreadPool(1);
        pool->init();
        mysql = mysql_init(nullptr);
        mysql->reconnect = 1;
        mysql = mysql_real_connect(mysql, "10.24.10.121", "root", "oj123456789",
                                   NULL, 3306, NULL, 0);
        if (!mysql){
            printf("Error connecting to database:%s\n",mysql_error(mysql));
            return -1;
        }
        mysql_query(mysql,"SHOW MASTER STATUS;");
        MYSQL_RES *res = mysql_store_result(mysql);
        MYSQL_ROW row = mysql_fetch_row(res);
        rpl.start_position = 4U;
        rpl.server_id =1;
        rpl.file_name = row[0];
        rpl.file_name_length = strlen(rpl.file_name);
//        std::cout<<rpl.file_name<<std::endl;
        rpl.start_position = atol(row[1]);
        rpl.flags = MYSQL_RPL_SKIP_HEARTBEAT;
        return 0;
    }

    int MTS_Handler::run() {
        {
            if (mysql_binlog_open(mysql, &rpl))
            {
                fprintf(stderr, "mysql_binlog_open() failed\n");
                fprintf(stderr, "Error %u: %s\n",
                        mysql_errno(mysql), mysql_error(mysql));
                exit(1);
            }



            for (;;)  /* read events until error or EOF */
            {
                std::chrono::system_clock::time_point time_point_now = std::chrono::system_clock::now(); // 获取当前时间点
                std::chrono::system_clock::duration duration_since_epoch
                        = time_point_now.time_since_epoch(); // 从1970-01-01 00:00:00到当前时间点的时长
                time_t now
                        = std::chrono::duration_cast<std::chrono::microseconds>(duration_since_epoch).count(); // 将时长转换为微秒数
                pretime = now;
                if (mysql_binlog_fetch(mysql, &rpl)) {
                    fprintf(stderr, "mysql_binlog_fetch() failed\n");
                    fprintf(stderr, "Error %u: %s\n",
                            mysql_errno(mysql), mysql_error(mysql));
                    break;
                }
                if (rpl.size == 0)  /* EOF */
                {
                    fprintf(stderr, "EOF event received\n");
                    break;
                }

                //printf("DEBUG::Fetch_interval: %lld rpl.size: %ld\n", now - pretime, rpl.size);
                //pretime = now;
                process();
            }
        }while (false);
        mysql_binlog_close(mysql, &rpl);
        mysql_close(mysql);
        pool->shutdown();
    }

    void MTS_Handler::process() {
        char *buffer = (char *)malloc(sizeof(char)*rpl.size);
        int length = rpl.size;
        memcpy(buffer, rpl.buffer, length);
//        return process(buffer, length);
        handle(buffer, length);
//        pool->submit(std::bind(&MTS_Handler::handle, this, buffer, length));
    }

    int MTS_Handler::handle(char *buffer, int length) {
        std::chrono::system_clock::time_point time_point_now = std::chrono::system_clock::now(); // 获取当前时间点
        std::chrono::system_clock::duration duration_since_epoch
                = time_point_now.time_since_epoch(); // 从1970-01-01 00:00:00到当前时间点的时长
        time_t now
                = std::chrono::duration_cast<std::chrono::microseconds>(duration_since_epoch).count(); // 将时长转换为微秒数


        binary_log::Log_event_type type = (binary_log::Log_event_type)buffer[1 + EVENT_TYPE_OFFSET];
        switch (type) {
            case binary_log::FORMAT_DESCRIPTION_EVENT: {
//                printf("FORMAT_DESCRIPTION_EVENT!\n");
                binary_log::Format_description_event *fde_tmp = new binary_log::Format_description_event(4, "8.017");
                char *buf = (char *) malloc(sizeof(char *) * (length + 1));
                memcpy(buf, buffer + 1, length - 1);
                fde = new binary_log::Format_description_event(reinterpret_cast<const char *>(buffer + 1), fde_tmp);
//                fde->print_event_info(std::cout);
                break;
            }
            case binary_log::TABLE_MAP_EVENT:{
//                printf("TABLE_MAP_EVENT!\n");
                auto *ev = new binary_log::Table_map_event(reinterpret_cast<const char *>(buffer + 1), fde);
                std::string full_table_name = ev->get_db_name()+'.'+ev->get_table_name();
                auto it = tables.find(full_table_name);
                //如果没有这个表则创建这个表，并构建schema
                if (it == tables.end()){
                    auto *table_schema = eventHandler.unpack(ev);
                    Table *table = new Table(full_table_name);
                    table->schema = new binary_log::TableSchema(*table_schema);
                    table->_pk = table_schema->getPrikey();
                    tables[full_table_name] = table;
                }
                break;
            }
//            下面三个rows event完全可以使用一套代码，主要处理逻辑的差别在Event_Handler中的unpack中
//             但是考虑到后面可能会加不一样的处理逻辑，在这是分开处理的
            case binary_log::WRITE_ROWS_EVENT: {
//                printf("WRITE_ROWS_EVENTS!\n");
                auto *ev = new binary_log::Write_rows_event(reinterpret_cast<const char *>(buffer + 1), fde);
                char *buf = new char[ev->row.size()];
                std::copy(ev->row.begin(), ev->row.end(), buf);
                auto reader = binary_log::Event_reader(buf, ev->row.size());
                auto *table_schema = eventHandler.get_schema(ev->get_table_id());
                std::string table_full_name = table_schema->getDBname() + '.' + table_schema->getTablename();
                auto it = tables.find(table_full_name);
                if (it==tables.end()){
                    printf("No Match Table!\n");
                }
                Table *table = it->second;
                int idx = table_schema->getIdxByName(table_schema->getPrikey());
                auto col = eventHandler.unpack(ev, reader, table_schema);
                Row *row = new Row(col[idx], ev->header()->when.tv_sec*1000000+ev->header()->when.tv_usec, false, table_schema->getDBname(), table_schema->getTablename());
//                Row *row = new Row(col[idx], now, false, table_schema->getDBname(), table_schema->getTablename());
                row->process_time = now;
                row->columns = col;
//                insert_a_record(table, row);
                pool->submit(std::bind(&MTS_Handler::insert_a_record, this, table, row));
                break;
            }
            case binary_log::DELETE_ROWS_EVENT:{
//                printf("DELETE_ROWS_EVENT!\n");
                auto *ev = new binary_log::Delete_rows_event(reinterpret_cast<const char *>(buffer + 1), fde);
                char *buf = new char[ev->row.size()];
                std::copy(ev->row.begin(), ev->row.end(), buf);
                auto reader = binary_log::Event_reader(buf, ev->row.size());
                auto *table_schema = eventHandler.get_schema(ev->get_table_id());
                std::string table_full_name = table_schema->getDBname() + '.' + table_schema->getTablename();
                auto it = tables.find(table_full_name);
                if (it==tables.end()){
                    printf("No Match Table!\n");
                }
                Table *table = it->second;
                int idx = table_schema->getIdxByName(table_schema->getPrikey());
                auto col = eventHandler.unpack(ev, reader, table_schema);
                Row *row = new Row(col[idx], ev->header()->when.tv_sec*1000000+ev->header()->when.tv_usec, false, table_schema->getDBname(), table_schema->getTablename());
//                Row *row = new Row(col[idx], now, false, table_schema->getDBname(), table_schema->getTablename());
                row->process_time = now;
                row->columns = col;
//                insert_a_record(table, row);
                pool->submit(std::bind(&MTS_Handler::insert_a_record, this, table, row));
                break;
            }
            case binary_log::UPDATE_ROWS_EVENT:{
//                printf("UPDATE_ROWS_EVE NT!\n");
                auto *ev = new binary_log::Update_rows_event(reinterpret_cast<const char *>(buffer + 1), fde);
                char *buf = new char[ev->row.size()];
                std::copy(ev->row.begin(), ev->row.end(), buf);
                auto reader = binary_log::Event_reader(buf, ev->row.size());
                auto *table_schema = eventHandler.get_schema(ev->get_table_id());
                std::string table_full_name = table_schema->getDBname() + '.' + table_schema->getTablename();
                auto it = tables.find(table_full_name);
                if (it==tables.end()){
                    printf("No Match Table!\n");
                }
                Table *table = it->second;
                int idx = table_schema->getIdxByName(table_schema->getPrikey());
                auto col = eventHandler.unpack(ev, reader, table_schema);
                Row *row = new Row(col[idx], ev->header()->when.tv_sec*1000000+ev->header()->when.tv_usec, false, table_schema->getDBname(), table_schema->getTablename());
//                Row *row = new Row(col[idx], now, false, table_schema->getDBname(), table_schema->getTablename());
                row->process_time = now;
                row->columns = col;
//                insert_a_record(table, row);
                pool->submit(std::bind(&MTS_Handler::insert_a_record, this, table, row));
                break;
            }
            default:
//                printf("Other Events!\n");
;
//                for (auto k=tables.begin();k!=tables.end();k++){4047690 4459052

//                    std::cout<<"table:"<<k->first<<std::endl;
//                    auto r= k->second->rows;
//                    for (auto it=r.begin();it!=r.end();it++){
//                        std::cout<<it->first<<" ";
//                        if (it->second->next){
//                            std::cout<<it->second->next->columns[0]<<it->second->next->columns[1]<<it->second->next->columns[2];
//                        }
//                        std::cout<<std::endl;
//                    }
//                }
                break;
        }
    }

    int MTS_Handler::insert_a_record(Table *table, Row *row) {

        //可以把入队代码放到解析事务的代码中一起加锁，保证入队顺序的严格一致
        auto it = q_map.find(row->primary_key);
        SafeQueue<Row *> * q;
        if (it==q_map.end()){
            q = new SafeQueue<Row *>();
            q_map[row->primary_key] = q;
        }else{
            q = it->second;
        }
        q->enqueue(row);
        std::unique_lock<std::mutex> lock(q->mu);
        while (q->q.front()->event_time!=row->event_time){
            q_cv.wait(lock);
        }
        table->insert_row(row);

        std::chrono::system_clock::time_point time_point_now = std::chrono::system_clock::now(); // 获取当前时间点
        std::chrono::system_clock::duration duration_since_epoch
                = time_point_now.time_since_epoch(); // 从1970-01-01 00:00:00到当前时间点的时长
        time_t now
                = std::chrono::duration_cast<std::chrono::microseconds>(duration_since_epoch).count(); // 将时长转换为微秒数
//        std::time_t now = std::time(0);
        row->now = now;
//        std::cout<<row->table_name<<" "<<row->event_time<<" "<<row->now<<std::endl;
        printf("%s,%lld,%lld\n",row->table_name.c_str(),now - row->event_time,now - pretime);
//        pretime = row->event_time;
//        table->g 140898802 54
        q->q.pop();
        q_cv.notify_all();
    }
}